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
    use crate::routes::swap::calldata::process_swap_calldata_v2;
    use crate::routes::swap::quote::process_swap_quote_v2;
    use crate::routes::swap::test_fixtures::MockSwapDataSource;
    use crate::routes::swap::SwapCandidateBuild;
    use crate::routes::swap::{exclude_orders_owned_by, user_for_selection};
    use crate::test_helpers::{mock_candidate, mock_order};
    use crate::types::swap::{
        SwapCalldataMode, SwapCalldataResponse, SwapCalldataV2Request, SwapDenomination,
        SwapQuoteV2Request,
    };
    use alloy::primitives::U256;
    use alloy::sol_types::{Revert, SolCall, SolError};
    use async_trait::async_trait;
    use rain_orderbook_bindings::IRaindexV6::{quote2Call, takeOrders4Call, QuoteV2};
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
            user: None,
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

    const USER: Address = Address::repeat_byte(0x22);

    fn owned_by(owner: Address, mut candidate: TakeOrderCandidate) -> TakeOrderCandidate {
        candidate.order.owner = owner;
        candidate
    }

    fn calldata_request(taker: Address, user: Option<Address>) -> SwapCalldataV2Request {
        SwapCalldataV2Request {
            chain_id: Some(8453),
            taker,
            user,
            input_token: Address::repeat_byte(4),
            output_token: Address::repeat_byte(5),
            mode: SwapCalldataMode::BuyUpTo,
            amount: "3".to_string(),
            price_cap: Some("10".to_string()),
            slippage_bps: None,
            reference_io_ratio: None,
            denomination: SwapDenomination::Wrapped,
        }
    }

    fn order_hashes(calldata: &SwapCalldataResponse) -> Vec<B256> {
        takeOrders4Call::abi_decode(&calldata.data)
            .unwrap()
            .config
            .orders
            .iter()
            .map(|order| keccak256(order.order.abi_encode()))
            .collect()
    }

    #[test]
    fn user_field_is_optional_and_parsed_like_taker() {
        let body = |user: serde_json::Value| {
            let mut body = json!({
                "chainId": 8453,
                "taker": TAKER,
                "inputToken": Address::repeat_byte(4),
                "outputToken": Address::repeat_byte(5),
                "mode": "buyUpTo",
                "amount": "3",
                "priceCap": "10",
            });
            if !user.is_null() {
                body["user"] = user;
            }
            body
        };

        let absent: SwapQuoteV2Request = serde_json::from_value(body(json!(null))).unwrap();
        assert_eq!(absent.user, None);
        assert_eq!(absent.taker, Some(TAKER));
        let present: SwapQuoteV2Request = serde_json::from_value(body(json!(USER))).unwrap();
        assert_eq!(present.user, Some(USER));
        assert_eq!(present.taker, Some(TAKER));
        assert!(serde_json::from_value::<SwapQuoteV2Request>(body(json!("0x1234"))).is_err());

        let absent: SwapCalldataV2Request = serde_json::from_value(body(json!(null))).unwrap();
        assert_eq!(absent.user, None);
        let present: SwapCalldataV2Request = serde_json::from_value(body(json!(USER))).unwrap();
        assert_eq!(present.user, Some(USER));
        assert_eq!(present.taker, TAKER);
        assert!(serde_json::from_value::<SwapCalldataV2Request>(body(json!("0x1234"))).is_err());
    }

    #[test]
    fn orders_owned_by_the_user_are_removed_and_counted() {
        let own = owned_by(USER, candidate(1, "5", "1"));
        let other = candidate(2, "5", "2");
        let also_own = owned_by(USER, candidate(3, "5", "3"));

        let mut candidates = vec![own.clone(), other.clone(), also_own];
        assert_eq!(exclude_orders_owned_by(&mut candidates, Some(USER)), 2);
        assert_eq!(candidates.len(), 1);
        assert_eq!(order_hash(&candidates[0]), order_hash(&other));

        // No user named: nothing is left out by ownership, as before.
        let mut candidates = vec![own, other];
        assert_eq!(exclude_orders_owned_by(&mut candidates, None), 0);
        assert_eq!(candidates.len(), 2);
        // The taker is not the user: owning nothing, it removes nothing.
        assert_eq!(exclude_orders_owned_by(&mut candidates, Some(TAKER)), 0);
        assert_eq!(candidates.len(), 2);
    }

    #[rocket::async_test]
    async fn quote_never_selects_the_users_own_order_even_at_the_better_price() {
        let own = owned_by(USER, candidate(1, "5", "1"));
        let mut ds = SimulatingDataSource::new(vec![]);
        ds.base.candidates = vec![own.clone(), candidate(2, "5", "2")];

        // Taker is the executor, not the user; the user is named.
        let mut request = quote_request(Some(crate::river_takes::RIVER_TAKER_BASE), "3");
        request.user = Some(USER);
        let response = process_swap_quote_v2(&ds, request).await.unwrap();
        assert_eq!(response.estimated_input, "6");
        assert_eq!(response.estimated_output, "3");
        assert!(response.fully_filled);
        assert_eq!(
            response.take_simulation.status,
            TakeSimulationStatus::Executes
        );
        // Left out before selection: never simulated, so not "excluded" either.
        assert!(response.take_simulation.excluded_orders.is_empty());
        assert_eq!(*ds.calls.lock().unwrap(), vec![(1, true)]);

        // Without the field the cheaper order is selected, as before.
        let response = process_swap_quote_v2(
            &ds,
            quote_request(Some(crate::river_takes::RIVER_TAKER_BASE), "3"),
        )
        .await
        .unwrap();
        assert_eq!(response.estimated_input, "3");
    }

    #[rocket::async_test]
    async fn own_order_does_not_come_back_when_the_other_orders_revert() {
        let own = owned_by(USER, candidate(1, "5", "1"));
        let mut ds = SimulatingDataSource::new(vec![2]);
        ds.base.candidates = vec![own.clone(), candidate(2, "5", "2")];

        let mut request = quote_request(Some(crate::river_takes::RIVER_TAKER_BASE), "3");
        request.user = Some(USER);
        let response = process_swap_quote_v2(&ds, request).await.unwrap();

        // The only other order reverts. The search must end there rather than
        // fall back to the user's own order.
        assert_eq!(
            response.take_simulation.status,
            TakeSimulationStatus::Reverts
        );
        assert!(!response.fully_filled);
        assert_eq!(response.take_simulation.excluded_orders.len(), 1);
        assert_ne!(
            response.take_simulation.excluded_orders[0].order_hash,
            order_hash(&own)
        );
        assert_eq!(*ds.calls.lock().unwrap(), vec![(1, true)]);
    }

    #[rocket::async_test]
    async fn quote_with_only_the_users_own_orders_has_no_liquidity() {
        let mut ds = SimulatingDataSource::new(vec![]);
        ds.base.candidates = vec![owned_by(USER, candidate(1, "5", "1"))];

        let mut request = quote_request(Some(crate::river_takes::RIVER_TAKER_BASE), "3");
        request.user = Some(USER);
        assert!(matches!(
            process_swap_quote_v2(&ds, request).await,
            Err(ApiError::Coded {
                code: crate::error::ApiErrorCode::SwapNoLiquidity,
                ..
            })
        ));
        assert!(ds.calls.lock().unwrap().is_empty());
    }

    #[rocket::async_test]
    async fn executor_calldata_leaves_out_the_users_own_order() {
        let executor = crate::river_takes::RIVER_TAKER_BASE;
        let own = owned_by(USER, candidate(1, "5", "1"));
        let other = candidate(2, "5", "2");
        let mut ds = SimulatingDataSource::new(vec![]);
        ds.base.candidates = vec![own.clone(), other.clone()];

        let calldata = process_swap_calldata_v2(&ds, calldata_request(executor, Some(USER)))
            .await
            .unwrap()
            .calldata;
        assert_eq!(order_hashes(&calldata), vec![order_hash(&other)]);
        assert_eq!(calldata.to, other.raindex);
        assert_eq!(calldata.estimated_input, "6");
        assert!(calldata.approvals.is_empty());

        // With no simulation answer the SDK must still not be asked: it
        // would select from candidates that include the user's own order.
        ds.unavailable = true;
        let calldata = process_swap_calldata_v2(&ds, calldata_request(executor, Some(USER)))
            .await
            .unwrap()
            .calldata;
        assert_eq!(order_hashes(&calldata), vec![order_hash(&other)]);

        // No user named and nothing reverting: the SDK path is used (the
        // mock's SDK result is an error, mapped at the boundary).
        ds.unavailable = false;
        assert!(matches!(
            process_swap_calldata_v2(&ds, calldata_request(executor, None)).await,
            Err(ApiError::Coded {
                code: crate::error::ApiErrorCode::SwapCalldataFailed,
                ..
            })
        ));
    }

    #[rocket::async_test]
    async fn executor_calldata_with_only_the_users_own_orders_has_no_liquidity() {
        let executor = crate::river_takes::RIVER_TAKER_BASE;
        let mut ds = SimulatingDataSource::new(vec![]);
        ds.base.candidates = vec![owned_by(USER, candidate(1, "5", "1"))];

        assert!(matches!(
            process_swap_calldata_v2(&ds, calldata_request(executor, Some(USER))).await,
            Err(ApiError::Coded {
                code: crate::error::ApiErrorCode::SwapNoLiquidity,
                ..
            })
        ));
    }

    #[rocket::async_test]
    async fn user_changes_nothing_when_the_taker_is_not_the_executor() {
        let own = owned_by(USER, candidate(1, "5", "1"));
        let mut ds = SimulatingDataSource::new(vec![]);
        ds.base.candidates = vec![own.clone(), candidate(2, "5", "2")];
        let sdk_calldata = SwapCalldataResponse {
            chain_id: 8453,
            to: own.raindex,
            data: Bytes::from(vec![0xab, 0xcd]),
            value: U256::ZERO,
            estimated_input: "3".to_string(),
            denomination: SwapDenomination::Wrapped,
            approvals: vec![],
        };
        ds.base.calldata_result = Ok(sdk_calldata.clone());

        // The user takes directly (taker is their own wallet) or through
        // some other address: the SDK builds that calldata and may select
        // the user's own order, so the quote must not leave it out either.
        for taker in [USER, TAKER] {
            let mut named = quote_request(Some(taker), "3");
            named.user = Some(USER);
            let named = process_swap_quote_v2(&ds, named).await.unwrap();
            let unnamed = process_swap_quote_v2(&ds, quote_request(Some(taker), "3"))
                .await
                .unwrap();
            assert_eq!(
                serde_json::to_value(&named).unwrap(),
                serde_json::to_value(&unnamed).unwrap()
            );
            // The cheaper order, the user's own, is still the one quoted.
            assert_eq!(named.estimated_input, "3");

            let named = process_swap_calldata_v2(&ds, calldata_request(taker, Some(USER)))
                .await
                .unwrap()
                .calldata;
            let unnamed = process_swap_calldata_v2(&ds, calldata_request(taker, None))
                .await
                .unwrap()
                .calldata;
            assert_eq!(named.data, sdk_calldata.data);
            assert_eq!(named.data, unnamed.data);
            assert_eq!(named.estimated_input, unnamed.estimated_input);
        }

        // A quote with no taker cannot be for the executor either.
        let mut no_taker = quote_request(None, "3");
        no_taker.user = Some(USER);
        let no_taker = process_swap_quote_v2(&ds, no_taker).await.unwrap();
        assert_eq!(no_taker.estimated_input, "3");
    }

    #[test]
    fn user_only_counts_for_selection_when_the_taker_is_the_executor() {
        let executor = crate::river_takes::RIVER_TAKER_BASE;
        assert_eq!(
            user_for_selection(8453, Some(executor), Some(USER)),
            Some(USER)
        );
        assert_eq!(user_for_selection(8453, Some(executor), None), None);
        assert_eq!(user_for_selection(8453, Some(USER), Some(USER)), None);
        assert_eq!(user_for_selection(8453, None, Some(USER)), None);
        // The executor is pinned on Base only.
        assert_eq!(user_for_selection(1, Some(executor), Some(USER)), None);
    }

    /// Simulates at a fixed block through the production simulation code.
    struct ReplayDataSource {
        rpcs: Vec<url::Url>,
        block: alloy::eips::BlockId,
    }

    #[async_trait]
    impl SwapDataSource for ReplayDataSource {
        async fn validate_supported_tokens(&self, _: Address, _: Address) -> Result<(), ApiError> {
            Err(ApiError::Internal("replay only simulates".into()))
        }

        async fn get_orders_for_pair(
            &self,
            _: Address,
            _: Address,
        ) -> Result<Vec<RaindexOrder>, ApiError> {
            Err(ApiError::Internal("replay only simulates".into()))
        }

        async fn build_candidates_for_pair(
            &self,
            _: &[RaindexOrder],
            _: Address,
            _: Address,
            _: Address,
        ) -> Result<SwapCandidateBuild, ApiError> {
            Err(ApiError::Internal("replay only simulates".into()))
        }

        async fn get_calldata(
            &self,
            _: TakeOrdersRequest,
        ) -> Result<SwapCalldataResponse, ApiError> {
            Err(ApiError::Internal("replay only simulates".into()))
        }

        async fn simulate_take(
            &self,
            chain_id: u32,
            taker: Address,
            input_token: Address,
            raindex: Address,
            config: &TakeOrdersConfigV5,
            verify_funding: bool,
        ) -> TakeOutcome {
            crate::take_simulation::simulate_funded_call(crate::take_simulation::FundedCall {
                rpcs: &self.rpcs,
                chain_id,
                block: self.block,
                from: taker,
                to: raindex,
                token: input_token,
                calldata: takeOrders4Call {
                    config: config.clone(),
                }
                .abi_encode()
                .into(),
                verify_funding,
            })
            .await
        }
    }

    /// Replays logged `/v3/swap/calldata` exchanges against chain state at
    /// chosen blocks, read-only, and prints what happens:
    ///
    /// - the logged inner `takeOrders4` from the logged taker (the executor);
    /// - the production search (`resolve_executable_take`) over the orders in
    ///   that calldata, at the same block;
    /// - the site's outer `RiverTaker.take` from the user's wallet.
    ///
    /// `TAKE_REPLAY_FILE`: JSON array of `{timestamp_utc, request, response}`.
    /// `TAKE_REPLAY_RPCS`: comma-separated RPC URLs, each tried on its own.
    /// `TAKE_REPLAY_BLOCKS`: comma-separated block numbers and/or `latest`.
    /// `TAKE_REPLAY_USER`: the wallet that would have sent the transaction.
    #[rocket::async_test]
    #[ignore = "live: replays logged calldata against a chain RPC"]
    async fn replay_logged_calldata() {
        use crate::take_simulation::{simulate_funded_call, FundedCall};
        use alloy::eips::BlockId;
        use alloy::providers::{Provider, ProviderBuilder};
        use rain_orderbook_common::take_orders::SelectedTakeOrderLeg;

        let var = |name: &str| std::env::var(name).unwrap_or_default();
        let exchanges: Vec<serde_json::Value> =
            serde_json::from_str(&std::fs::read_to_string(var("TAKE_REPLAY_FILE")).unwrap())
                .unwrap();
        let rpcs: Vec<url::Url> = var("TAKE_REPLAY_RPCS")
            .split(',')
            .map(|rpc| rpc.trim().parse().unwrap())
            .collect();
        let blocks: Vec<BlockId> = var("TAKE_REPLAY_BLOCKS")
            .split(',')
            .map(|block| match block.trim() {
                "latest" => BlockId::latest(),
                number => BlockId::number(number.parse().unwrap()),
            })
            .collect();
        let user: Address = var("TAKE_REPLAY_USER").parse().unwrap();
        let describe = |outcome: &TakeOutcome| match outcome {
            TakeOutcome::Reverts(data) => {
                format!(
                    "REVERTS data={data} reason={:?}",
                    decode_revert_reason(data)
                )
            }
            other => format!("{other:?}"),
        };

        let mut seen = std::collections::HashSet::new();
        for exchange in &exchanges {
            let (request, response) = (&exchange["request"], &exchange["response"]);
            let data: Bytes = response["data"].as_str().unwrap().parse().unwrap();
            if !seen.insert(data.clone()) {
                continue;
            }
            let address = |value: &serde_json::Value| value.as_str().unwrap().parse::<Address>();
            let executor = address(&request["taker"]).unwrap();
            let raindex = address(&response["to"]).unwrap();
            let input_token = address(&request["inputToken"]).unwrap();
            let output_token = address(&request["outputToken"]).unwrap();
            let amount = request["amount"].as_str().unwrap();
            let price_cap = response["resolvedPriceCap"].as_str().unwrap();
            let config = takeOrders4Call::abi_decode(&data).unwrap().config;
            let float = |raw: B256| Float::from_raw(raw).format().unwrap();

            println!("\n=== calldata logged {} ===", exchange["timestamp_utc"]);
            println!(
                "minimumIO={} maximumIO={} maximumIORatio={} IOIsInput={} orders={}",
                float(config.minimumIO),
                float(config.maximumIO),
                float(config.maximumIORatio),
                config.IOIsInput,
                config.orders.len()
            );
            for order in &config.orders {
                println!(
                    "order {} owner={} signedContexts={} outputVault={}",
                    keccak256(order.order.abi_encode()),
                    order.order.owner,
                    order.signedContext.len(),
                    order.order.validOutputs[order.outputIOIndex.to::<usize>()].vaultId,
                );
            }

            let mode = ParsedTakeOrdersMode::parse(TakeOrdersMode::SpendUpTo, amount).unwrap();
            let price_cap = Float::parse(price_cap.to_string()).unwrap();
            let candidates: Vec<TakeOrderCandidate> = config
                .orders
                .iter()
                .map(|order| TakeOrderCandidate {
                    raindex,
                    order: order.order.clone(),
                    input_io_index: order.inputIOIndex.to::<u32>(),
                    output_io_index: order.outputIOIndex.to::<u32>(),
                    max_output: Float::parse("1000000".to_string()).unwrap(),
                    ratio: price_cap,
                    signed_context: order.signedContext.clone(),
                })
                .collect();
            // The logged calldata as a selection. With one order the leg
            // amounts do not affect the take that is rebuilt from it.
            let first = SimulationResult {
                legs: candidates
                    .iter()
                    .map(|candidate| SelectedTakeOrderLeg {
                        candidate: candidate.clone(),
                        input: mode.target_amount(),
                        output: mode.target_amount(),
                    })
                    .collect(),
                total_input: mode.target_amount(),
                total_output: mode.target_amount(),
            };
            let rebuilt = build_take_orders_config_from_simulation(first.clone(), mode, price_cap)
                .unwrap()
                .unwrap()
                .config;
            println!(
                "selection rebuilds the logged calldata exactly: {}",
                rebuilt == config
            );

            // What the site sends: RiverTaker.take(config, in, out, maxIn, minOut).
            let config_type = takeOrders4Call::SIGNATURE
                .strip_prefix("takeOrders4(")
                .and_then(|rest| rest.strip_suffix(')'))
                .unwrap();
            let selector = keccak256(format!(
                "take({config_type},address,address,uint256,uint256)"
            ));
            let max_in = mode.target_amount().to_fixed_decimal_lossy(18).unwrap().0;
            let mut outer = selector[..4].to_vec();
            outer.extend(
                (
                    config.clone(),
                    input_token,
                    output_token,
                    max_in,
                    U256::from(1),
                )
                    .abi_encode_params(),
            );

            for rpc in &rpcs {
                let host = rpc.host_str().unwrap_or("?").to_string();
                let rpcs = std::slice::from_ref(rpc);
                for block in &blocks {
                    let header = ProviderBuilder::new()
                        .connect_http(rpc.clone())
                        .get_block(*block)
                        .await
                        .ok()
                        .flatten()
                        .map(|block| (block.header.number, block.header.timestamp));
                    println!("--- {host} block {block:?} (number, timestamp) = {header:?}");

                    // What a quote sees: each order's calculate-io alone.
                    for order in &config.orders {
                        let quote = quote2Call {
                            quoteConfig: QuoteV2 {
                                order: order.order.clone(),
                                inputIOIndex: order.inputIOIndex,
                                outputIOIndex: order.outputIOIndex,
                                signedContext: order.signedContext.clone(),
                            },
                        };
                        let request = alloy::rpc::types::TransactionRequest::default()
                            .from(executor)
                            .to(raindex)
                            .input(quote.abi_encode().into());
                        let quoted = ProviderBuilder::new()
                            .connect_http(rpc.clone())
                            .call(request)
                            .block(*block)
                            .await
                            .ok()
                            .and_then(|output| quote2Call::abi_decode_returns(&output).ok())
                            .map(|quote| {
                                (quote.exists, float(quote.outputMax), float(quote.ioRatio))
                            });
                        println!(
                            "quote2 {} (exists, outputMax, ioRatio) = {quoted:?}",
                            keccak256(order.order.abi_encode())
                        );
                    }

                    let inner = simulate_funded_call(FundedCall {
                        rpcs,
                        chain_id: 8453,
                        block: *block,
                        from: executor,
                        to: raindex,
                        token: input_token,
                        calldata: data.clone(),
                        verify_funding: true,
                    })
                    .await;
                    println!("inner takeOrders4 from executor: {}", describe(&inner));

                    let resolved = resolve_executable_take(
                        &ReplayDataSource {
                            rpcs: rpcs.to_vec(),
                            block: *block,
                        },
                        ExecutableTakeRequest {
                            chain_id: 8453,
                            taker: executor,
                            input_token,
                            mode,
                            price_cap,
                        },
                        candidates.clone(),
                        first.clone(),
                    )
                    .await;
                    println!(
                        "search: {}",
                        serde_json::to_string(&resolved.take_simulation).unwrap()
                    );

                    // The same, with the user's own orders left out first, as
                    // a request naming `user` does.
                    let mut not_own = candidates.clone();
                    let own_order_count = exclude_orders_owned_by(&mut not_own, Some(user));
                    let first_not_own = SimulationResult {
                        legs: first
                            .legs
                            .iter()
                            .filter(|leg| leg.candidate.order.owner != user)
                            .cloned()
                            .collect(),
                        ..first.clone()
                    };
                    let resolved_not_own = resolve_executable_take(
                        &ReplayDataSource {
                            rpcs: rpcs.to_vec(),
                            block: *block,
                        },
                        ExecutableTakeRequest {
                            chain_id: 8453,
                            taker: executor,
                            input_token,
                            mode,
                            price_cap,
                        },
                        not_own.clone(),
                        first_not_own,
                    )
                    .await;
                    println!(
                        "search with user={user}: own orders left out={own_order_count} candidates={:?} result={}",
                        not_own.iter().map(order_hash).collect::<Vec<_>>(),
                        serde_json::to_string(&resolved_not_own.take_simulation).unwrap()
                    );

                    // The site's outer call for the route the search ends with.
                    if resolved.take_simulation.status == TakeSimulationStatus::Executes {
                        let surviving = build_take_orders_config_from_simulation(
                            resolved.simulation.clone(),
                            mode,
                            price_cap,
                        )
                        .unwrap()
                        .unwrap()
                        .config;
                        let mut surviving_outer = selector[..4].to_vec();
                        surviving_outer.extend(
                            (
                                surviving.clone(),
                                input_token,
                                output_token,
                                max_in,
                                U256::from(1),
                            )
                                .abi_encode_params(),
                        );
                        let outcome = simulate_funded_call(FundedCall {
                            rpcs,
                            chain_id: 8453,
                            block: *block,
                            from: user,
                            to: executor,
                            token: input_token,
                            calldata: surviving_outer.into(),
                            verify_funding: true,
                        })
                        .await;
                        println!(
                            "outer RiverTaker.take from user, surviving {} order(s): {}",
                            surviving.orders.len(),
                            describe(&outcome)
                        );
                    }

                    let wrapped = simulate_funded_call(FundedCall {
                        rpcs,
                        chain_id: 8453,
                        block: *block,
                        from: user,
                        to: executor,
                        token: input_token,
                        calldata: outer.clone().into(),
                        verify_funding: true,
                    })
                    .await;
                    println!("outer RiverTaker.take from user: {}", describe(&wrapped));
                }
            }
        }
    }
}
