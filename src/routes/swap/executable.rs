//! Finds a take that executes among quoted candidates.
//!
//! The selected batch is simulated as the real `takeOrders4` call. When it
//! reverts, each selected leg is simulated on its own; the legs that revert
//! are left out with their raw revert data, selection is re-run over the rest,
//! and the new batch is simulated again. One reverting order therefore costs
//! the route that order, not the whole route. See `docs/take-simulation.md`.

use super::slippage::select_best_raindex_simulation;
use super::SwapDataSource;
use crate::take_simulation::{decode_revert_reason, TakeOutcome};
use crate::types::swap::{
    ExcludedOrder, TakeSimulation, TakeSimulationSkipReason, TakeSimulationStatus,
};
use alloy::primitives::{keccak256, Address, Bytes, B256};
use alloy::sol_types::SolValue;
use rain_math_float::Float;
use rain_orderbook_bindings::IRaindexV6::TakeOrdersConfigV5;
use rain_orderbook_common::take_orders::{
    build_take_orders_config_from_simulation, ParsedTakeOrdersMode, SimulationResult,
    TakeOrderCandidate,
};
use std::time::{Duration, Instant};

/// Batches simulated before giving up. Each round removes at least one order.
const MAX_ROUNDS: usize = 4;
/// A reverting batch with more legs than this is not taken apart.
const MAX_ISOLATED_LEGS: usize = 12;
/// Upper bound on the whole search, so a slow RPC cannot hold a quote.
const SEARCH_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Clone, Copy)]
pub(crate) struct ExecutableTakeRequest {
    pub chain_id: u32,
    /// `msg.sender` of the take.
    pub taker: Address,
    pub input_token: Address,
    pub mode: ParsedTakeOrdersMode,
    pub price_cap: Float,
}

pub(crate) struct ExecutableTake {
    /// The selection the response describes: the one that executed, the last
    /// one tried when none did, or the original one when nothing is known.
    pub simulation: SimulationResult,
    pub take_simulation: TakeSimulation,
}

fn same_leg(left: &TakeOrderCandidate, right: &TakeOrderCandidate) -> bool {
    left.raindex == right.raindex
        && left.order == right.order
        && left.input_io_index == right.input_io_index
        && left.output_io_index == right.output_io_index
}

/// The batch reduced to one leg, taking what that leg takes in the batch.
/// `minimumIO` is an aggregate constraint, so it is dropped (as the SDK's own
/// preflight does) or every leg of an exact-mode batch would fail alone.
fn single_leg_config(
    config: &TakeOrdersConfigV5,
    selection: &SimulationResult,
    index: usize,
) -> Option<TakeOrdersConfigV5> {
    let leg = selection.legs.get(index)?;
    let amount = if config.IOIsInput {
        leg.output
    } else {
        leg.input
    };
    Some(TakeOrdersConfigV5 {
        minimumIO: B256::ZERO,
        maximumIO: amount.get_inner(),
        maximumIORatio: config.maximumIORatio,
        IOIsInput: config.IOIsInput,
        orders: vec![config.orders.get(index)?.clone()],
        data: config.data.clone(),
    })
}

/// Indexes of the legs that revert when taken alone, with their revert data.
async fn reverting_legs(
    ds: &dyn SwapDataSource,
    request: &ExecutableTakeRequest,
    raindex: Address,
    config: &TakeOrdersConfigV5,
    selection: &SimulationResult,
    batch_revert: &Bytes,
) -> Result<Vec<(usize, Bytes)>, TakeSimulationSkipReason> {
    if config.orders.len() == 1 {
        return Ok(vec![(0, batch_revert.clone())]);
    }
    if config.orders.len() > MAX_ISOLATED_LEGS {
        return Ok(vec![]);
    }
    let probes = (0..config.orders.len()).map(|index| async move {
        let Some(leg_config) = single_leg_config(config, selection, index) else {
            return TakeOutcome::NotSimulated(TakeSimulationSkipReason::BuildFailed);
        };
        // The batch call already checked that the taker is funded.
        ds.simulate_take(
            request.chain_id,
            request.taker,
            request.input_token,
            raindex,
            &leg_config,
            false,
        )
        .await
    });
    let mut reverting = Vec::new();
    for (index, outcome) in futures::future::join_all(probes)
        .await
        .into_iter()
        .enumerate()
    {
        match outcome {
            TakeOutcome::Executes => {}
            TakeOutcome::Reverts(data) => reverting.push((index, data)),
            TakeOutcome::NotSimulated(reason) => return Err(reason),
        }
    }
    Ok(reverting)
}

fn result(
    simulation: SimulationResult,
    status: TakeSimulationStatus,
    reason: Option<TakeSimulationSkipReason>,
    revert_data: Option<Bytes>,
    excluded_orders: Vec<ExcludedOrder>,
) -> ExecutableTake {
    ExecutableTake {
        simulation,
        take_simulation: TakeSimulation {
            status,
            reason,
            revert_reason: revert_data
                .as_ref()
                .and_then(|data| decode_revert_reason(data)),
            revert_data,
            excluded_orders,
        },
    }
}

async fn search(
    ds: &dyn SwapDataSource,
    request: &ExecutableTakeRequest,
    mut remaining: Vec<TakeOrderCandidate>,
    first: SimulationResult,
) -> ExecutableTake {
    use TakeSimulationStatus::{Executes, NotSimulated, Reverts};
    let mut attempted = first.clone();
    // The last selection that was simulated and reverted.
    let mut reverted: Option<SimulationResult> = None;
    let mut excluded = Vec::new();

    for _ in 0..MAX_ROUNDS {
        let built = build_take_orders_config_from_simulation(
            attempted.clone(),
            request.mode,
            request.price_cap,
        );
        let (raindex, config) = match (built, attempted.legs.first()) {
            (Ok(Some(built)), Some(leg)) => (leg.candidate.raindex, built.config),
            (built, _) => {
                // What is left cannot form a take (for example an exact mode
                // that the surviving orders no longer fill).
                return match reverted {
                    Some(reverted) => result(reverted, Reverts, None, None, excluded),
                    None => {
                        if let Err(error) = built {
                            tracing::warn!(%error, "could not build the take to simulate");
                        }
                        let reason = TakeSimulationSkipReason::BuildFailed;
                        result(first, NotSimulated, Some(reason), None, excluded)
                    }
                };
            }
        };

        let batch_revert = match ds
            .simulate_take(
                request.chain_id,
                request.taker,
                request.input_token,
                raindex,
                &config,
                true,
            )
            .await
        {
            TakeOutcome::Executes => return result(attempted, Executes, None, None, excluded),
            TakeOutcome::NotSimulated(reason) => {
                return result(first, NotSimulated, Some(reason), None, excluded)
            }
            TakeOutcome::Reverts(data) => data,
        };

        let reverting =
            match reverting_legs(ds, request, raindex, &config, &attempted, &batch_revert).await {
                Ok(reverting) => reverting,
                Err(reason) => return result(first, NotSimulated, Some(reason), None, excluded),
            };
        if reverting.is_empty() {
            // The batch reverts as a whole; no order can be blamed for it.
            return result(attempted, Reverts, None, Some(batch_revert), excluded);
        }
        for (index, revert_data) in reverting {
            let Some(leg) = attempted.legs.get(index) else {
                continue;
            };
            remaining.retain(|candidate| !same_leg(candidate, &leg.candidate));
            excluded.push(ExcludedOrder {
                order_hash: keccak256(leg.candidate.order.abi_encode()),
                revert_reason: decode_revert_reason(&revert_data),
                revert_data,
            });
        }

        match select_best_raindex_simulation(remaining.clone(), request.mode, request.price_cap) {
            Ok(next) => reverted = Some(std::mem::replace(&mut attempted, next)),
            // Nothing is left to take.
            Err(_) => return result(attempted, Reverts, None, None, excluded),
        }
    }

    let reason = TakeSimulationSkipReason::RoundsExhausted;
    result(first, NotSimulated, Some(reason), None, excluded)
}

/// Simulates the take for `first` (the selection made from `candidates`),
/// dropping orders that revert until a batch executes or nothing is left.
/// Never fails: not getting an answer is a "not simulated" result that keeps
/// `first`.
pub(crate) async fn resolve_executable_take(
    ds: &dyn SwapDataSource,
    request: ExecutableTakeRequest,
    candidates: Vec<TakeOrderCandidate>,
    first: SimulationResult,
) -> ExecutableTake {
    let started_at = Instant::now();
    let resolved = tokio::time::timeout(
        SEARCH_TIMEOUT,
        search(ds, &request, candidates, first.clone()),
    )
    .await
    .unwrap_or_else(|_| {
        let reason = TakeSimulationSkipReason::TimedOut;
        result(
            first,
            TakeSimulationStatus::NotSimulated,
            Some(reason),
            None,
            vec![],
        )
    });

    let simulation = &resolved.take_simulation;
    let duration_ms = started_at.elapsed().as_millis() as u64;
    for order in &simulation.excluded_orders {
        tracing::warn!(
            order_hash = %order.order_hash,
            revert_data = %order.revert_data,
            revert_reason = ?order.revert_reason,
            taker = %request.taker,
            "excluded order whose simulated take reverts"
        );
    }
    tracing::info!(
        chain_id = request.chain_id,
        taker = %request.taker,
        input_token = %request.input_token,
        status = ?simulation.status,
        reason = ?simulation.reason,
        revert_data = ?simulation.revert_data,
        excluded_order_count = simulation.excluded_orders.len(),
        selected_leg_count = resolved.simulation.legs.len(),
        duration_ms,
        "simulated take for swap"
    );
    resolved
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ApiError;
    use crate::routes::swap::quote::process_swap_quote_v2;
    use crate::routes::swap::test_fixtures::MockSwapDataSource;
    use crate::routes::swap::SwapCandidateBuild;
    use crate::test_helpers::{mock_candidate, mock_order};
    use crate::types::swap::{
        SwapCalldataMode, SwapCalldataResponse, SwapDenomination, SwapQuoteV2Request,
    };
    use alloy::primitives::U256;
    use alloy::sol_types::{Revert, SolError};
    use async_trait::async_trait;
    use rain_orderbook_common::raindex_client::orders::RaindexOrder;
    use rain_orderbook_common::raindex_client::take_orders::TakeOrdersRequest;
    use rain_orderbook_common::take_orders::TakeOrdersMode;
    use serde_json::json;
    use std::sync::Mutex;

    const TAKER: Address = Address::repeat_byte(0x11);

    /// Reverts any take that includes an order whose nonce is in `reverting`;
    /// with `batch_only`, reverts every multi-order take instead.
    struct SimulatingDataSource {
        base: MockSwapDataSource,
        reverting: Vec<u64>,
        batch_only: bool,
        unavailable: bool,
        calls: Mutex<Vec<(usize, bool)>>,
    }

    impl SimulatingDataSource {
        fn new(reverting: Vec<u64>) -> Self {
            Self {
                base: MockSwapDataSource {
                    supported_tokens: Ok(()),
                    orders: Ok(vec![mock_order()]),
                    candidates: vec![],
                    calldata_result: Err(ApiError::Internal("unused".into())),
                },
                reverting,
                batch_only: false,
                unavailable: false,
                calls: Mutex::new(vec![]),
            }
        }
    }

    #[async_trait]
    impl SwapDataSource for SimulatingDataSource {
        async fn validate_supported_tokens(&self, a: Address, b: Address) -> Result<(), ApiError> {
            self.base.validate_supported_tokens(a, b).await
        }

        async fn get_orders_for_pair(
            &self,
            a: Address,
            b: Address,
        ) -> Result<Vec<RaindexOrder>, ApiError> {
            self.base.get_orders_for_pair(a, b).await
        }

        async fn build_candidates_for_pair(
            &self,
            orders: &[RaindexOrder],
            a: Address,
            b: Address,
            counterparty: Address,
        ) -> Result<SwapCandidateBuild, ApiError> {
            self.base
                .build_candidates_for_pair(orders, a, b, counterparty)
                .await
        }

        async fn get_calldata(
            &self,
            request: TakeOrdersRequest,
        ) -> Result<SwapCalldataResponse, ApiError> {
            self.base.get_calldata(request).await
        }

        async fn simulate_take(
            &self,
            _chain_id: u32,
            _taker: Address,
            _input_token: Address,
            _raindex: Address,
            config: &TakeOrdersConfigV5,
            verify_funding: bool,
        ) -> TakeOutcome {
            self.calls
                .lock()
                .unwrap()
                .push((config.orders.len(), verify_funding));
            if self.unavailable {
                return TakeOutcome::NotSimulated(TakeSimulationSkipReason::RpcUnavailable);
            }
            if self.batch_only {
                return if config.orders.len() > 1 {
                    TakeOutcome::Reverts(Bytes::from(vec![0xde, 0xad]))
                } else {
                    TakeOutcome::Executes
                };
            }
            let reverts = config.orders.iter().any(|order| {
                self.reverting
                    .contains(&U256::from_be_bytes(order.order.nonce.0).to::<u64>())
            });
            if reverts {
                TakeOutcome::Reverts(Bytes::from(Revert::from("Min trade amount").abi_encode()))
            } else {
                TakeOutcome::Executes
            }
        }
    }

    fn candidate(nonce: u64, max_output: &str, ratio: &str) -> TakeOrderCandidate {
        let mut candidate = mock_candidate(max_output, ratio);
        candidate.order.nonce = U256::from(nonce).into();
        candidate
    }

    fn order_hash(candidate: &TakeOrderCandidate) -> B256 {
        keccak256(candidate.order.abi_encode())
    }

    async fn resolve(
        ds: &SimulatingDataSource,
        candidates: Vec<TakeOrderCandidate>,
        amount: &str,
    ) -> ExecutableTake {
        let mode = ParsedTakeOrdersMode::parse(TakeOrdersMode::BuyUpTo, amount).unwrap();
        let price_cap = Float::parse("10".to_string()).unwrap();
        let first = select_best_raindex_simulation(candidates.clone(), mode, price_cap).unwrap();
        let request = ExecutableTakeRequest {
            chain_id: 8453,
            taker: TAKER,
            input_token: Address::repeat_byte(4),
            mode,
            price_cap,
        };
        resolve_executable_take(ds, request, candidates, first).await
    }

    #[rocket::async_test]
    async fn executing_batch_is_kept_whole_with_one_call() {
        let ds = SimulatingDataSource::new(vec![]);
        let resolved = resolve(
            &ds,
            vec![candidate(1, "5", "1"), candidate(2, "5", "2")],
            "8",
        )
        .await;

        assert_eq!(
            resolved.take_simulation.status,
            TakeSimulationStatus::Executes
        );
        assert!(resolved.take_simulation.excluded_orders.is_empty());
        assert_eq!(resolved.simulation.legs.len(), 2);
        assert_eq!(*ds.calls.lock().unwrap(), vec![(2, true)]);
    }

    #[rocket::async_test]
    async fn one_reverting_order_is_excluded_and_the_rest_still_execute() {
        let cheap_but_reverting = candidate(1, "5", "1");
        let ds = SimulatingDataSource::new(vec![1]);
        let resolved = resolve(
            &ds,
            vec![
                cheap_but_reverting.clone(),
                candidate(2, "5", "2"),
                candidate(3, "5", "3"),
            ],
            "8",
        )
        .await;

        let simulation = &resolved.take_simulation;
        assert_eq!(simulation.status, TakeSimulationStatus::Executes);
        assert_eq!(simulation.revert_data, None);
        assert_eq!(simulation.excluded_orders.len(), 1);
        let excluded = &simulation.excluded_orders[0];
        assert_eq!(excluded.order_hash, order_hash(&cheap_but_reverting));
        assert_eq!(excluded.revert_reason.as_deref(), Some("Min trade amount"));
        assert_eq!(
            excluded.revert_data.as_ref(),
            Revert::from("Min trade amount").abi_encode()
        );

        // The route is now the two surviving orders: 5 at 2 and 3 at 3.
        assert_eq!(resolved.simulation.legs.len(), 2);
        assert_eq!(resolved.simulation.total_output.format().unwrap(), "8");
        assert_eq!(resolved.simulation.total_input.format().unwrap(), "19");
        // Batch, its two legs alone, then the re-selected batch.
        assert_eq!(
            *ds.calls.lock().unwrap(),
            vec![(2, true), (1, false), (1, false), (2, true)]
        );
    }

    #[rocket::async_test]
    async fn only_order_reverting_leaves_no_route_and_keeps_the_reason() {
        let only = candidate(1, "5", "1");
        let ds = SimulatingDataSource::new(vec![1]);
        let resolved = resolve(&ds, vec![only.clone()], "3").await;

        let simulation = &resolved.take_simulation;
        assert_eq!(simulation.status, TakeSimulationStatus::Reverts);
        assert_eq!(simulation.reason, None);
        assert_eq!(simulation.excluded_orders.len(), 1);
        assert_eq!(simulation.excluded_orders[0].order_hash, order_hash(&only));
        assert_eq!(
            simulation.excluded_orders[0].revert_reason.as_deref(),
            Some("Min trade amount")
        );
        // The response still describes the selection that was tried.
        assert_eq!(resolved.simulation.legs.len(), 1);
        assert_eq!(*ds.calls.lock().unwrap(), vec![(1, true)]);
    }

    #[rocket::async_test]
    async fn every_order_reverting_leaves_no_route() {
        let ds = SimulatingDataSource::new(vec![1, 2]);
        let resolved = resolve(
            &ds,
            vec![candidate(1, "5", "1"), candidate(2, "5", "2")],
            "8",
        )
        .await;

        assert_eq!(
            resolved.take_simulation.status,
            TakeSimulationStatus::Reverts
        );
        assert_eq!(resolved.take_simulation.excluded_orders.len(), 2);
    }

    #[rocket::async_test]
    async fn batch_that_reverts_only_as_a_whole_blames_no_order() {
        let mut ds = SimulatingDataSource::new(vec![]);
        ds.batch_only = true;
        let resolved = resolve(
            &ds,
            vec![candidate(1, "5", "1"), candidate(2, "5", "2")],
            "8",
        )
        .await;

        let simulation = &resolved.take_simulation;
        assert_eq!(simulation.status, TakeSimulationStatus::Reverts);
        assert!(simulation.excluded_orders.is_empty());
        assert_eq!(simulation.revert_data, Some(Bytes::from(vec![0xde, 0xad])));
        assert_eq!(simulation.revert_reason, None);
        assert_eq!(resolved.simulation.legs.len(), 2);
    }

    #[rocket::async_test]
    async fn unavailable_simulation_is_neither_pass_nor_fail() {
        let mut ds = SimulatingDataSource::new(vec![1]);
        ds.unavailable = true;
        let resolved = resolve(
            &ds,
            vec![candidate(1, "5", "1"), candidate(2, "5", "2")],
            "8",
        )
        .await;

        let simulation = &resolved.take_simulation;
        assert_eq!(simulation.status, TakeSimulationStatus::NotSimulated);
        assert_eq!(
            simulation.reason,
            Some(TakeSimulationSkipReason::RpcUnavailable)
        );
        assert!(simulation.excluded_orders.is_empty());
        assert_eq!(resolved.simulation.legs.len(), 2);
    }

    #[rocket::async_test]
    async fn search_stops_at_the_round_limit_without_a_verdict() {
        // Five orders that each fill the whole request alone and all revert:
        // one is found and excluded per round.
        let candidates: Vec<_> = (1..=5)
            .map(|nonce| candidate(nonce, "10", &nonce.to_string()))
            .collect();
        let ds = SimulatingDataSource::new(vec![1, 2, 3, 4, 5]);
        let resolved = resolve(&ds, candidates, "8").await;

        let simulation = &resolved.take_simulation;
        assert_eq!(simulation.status, TakeSimulationStatus::NotSimulated);
        assert_eq!(
            simulation.reason,
            Some(TakeSimulationSkipReason::RoundsExhausted)
        );
        assert_eq!(simulation.excluded_orders.len(), MAX_ROUNDS);
        assert_eq!(ds.calls.lock().unwrap().len(), MAX_ROUNDS);
    }

    #[rocket::async_test]
    async fn data_source_without_simulation_reports_not_simulated() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let mode = ParsedTakeOrdersMode::parse(TakeOrdersMode::BuyUpTo, "3").unwrap();
        let price_cap = Float::parse("10".to_string()).unwrap();
        let candidates = vec![candidate(1, "5", "1")];
        let first = select_best_raindex_simulation(candidates.clone(), mode, price_cap).unwrap();
        let request = ExecutableTakeRequest {
            chain_id: 8453,
            taker: TAKER,
            input_token: Address::repeat_byte(4),
            mode,
            price_cap,
        };
        let resolved = resolve_executable_take(&ds, request, candidates, first).await;

        assert_eq!(
            resolved.take_simulation.status,
            TakeSimulationStatus::NotSimulated
        );
    }

    fn quote_request(taker: Option<Address>, amount: &str) -> SwapQuoteV2Request {
        SwapQuoteV2Request {
            chain_id: Some(8453),
            taker,
            input_token: Address::repeat_byte(4),
            output_token: Address::repeat_byte(5),
            mode: SwapCalldataMode::BuyUpTo,
            amount: amount.to_string(),
            price_cap: Some("10".to_string()),
            slippage_bps: None,
            reference_io_ratio: None,
            denomination: SwapDenomination::Wrapped,
        }
    }

    #[rocket::async_test]
    async fn quote_with_reverting_take_is_not_filled_and_carries_the_raw_reason() {
        let only = candidate(1, "5", "1");
        let mut ds = SimulatingDataSource::new(vec![1]);
        ds.base.candidates = vec![only.clone()];

        let response = process_swap_quote_v2(&ds, quote_request(Some(TAKER), "3"))
            .await
            .unwrap();
        let body = serde_json::to_value(&response).unwrap();

        // Calculate-io alone fills this quote; the simulated take does not.
        assert_eq!(body["estimatedOutput"], "3");
        assert_eq!(body["fullyFilled"], false);
        assert_eq!(
            body["takeSimulation"],
            json!({
                "status": "reverts",
                "excludedOrders": [{
                    "orderHash": order_hash(&only),
                    "revertData": Bytes::from(Revert::from("Min trade amount").abi_encode()),
                    "revertReason": "Min trade amount",
                }],
            })
        );
    }

    #[rocket::async_test]
    async fn quote_describes_the_route_left_after_excluding_a_reverting_order() {
        let reverting = candidate(1, "5", "1");
        let mut ds = SimulatingDataSource::new(vec![1]);
        ds.base.candidates = vec![reverting.clone(), candidate(2, "5", "2")];

        let response = process_swap_quote_v2(&ds, quote_request(Some(TAKER), "3"))
            .await
            .unwrap();
        let body = serde_json::to_value(&response).unwrap();

        assert_eq!(body["estimatedOutput"], "3");
        assert_eq!(body["estimatedInput"], "6");
        assert_eq!(body["fullyFilled"], true);
        assert_eq!(body["takeSimulation"]["status"], "executes");
        assert_eq!(
            body["takeSimulation"]["excludedOrders"][0]["orderHash"],
            json!(order_hash(&reverting))
        );
    }

    #[rocket::async_test]
    async fn quote_without_a_result_says_not_simulated_and_is_otherwise_unchanged() {
        let mut ds = SimulatingDataSource::new(vec![1]);
        ds.base.candidates = vec![candidate(1, "5", "1")];

        let no_taker = process_swap_quote_v2(&ds, quote_request(None, "3"))
            .await
            .unwrap();
        assert!(ds.calls.lock().unwrap().is_empty());
        let body = serde_json::to_value(&no_taker).unwrap();
        assert_eq!(body["fullyFilled"], true);
        assert_eq!(
            body["takeSimulation"],
            json!({"status": "notSimulated", "reason": "noTaker", "excludedOrders": []})
        );

        ds.unavailable = true;
        let unavailable = process_swap_quote_v2(&ds, quote_request(Some(TAKER), "3"))
            .await
            .unwrap();
        let body = serde_json::to_value(&unavailable).unwrap();
        assert_eq!(body["fullyFilled"], true);
        assert_eq!(body["estimatedInput"], "3");
        assert_eq!(
            body["takeSimulation"],
            json!({"status": "notSimulated", "reason": "rpcUnavailable", "excludedOrders": []})
        );
    }
}
