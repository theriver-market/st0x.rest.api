//! Simulates the `takeOrders4` call a swap quote leads to.
//!
//! A Raindex quote evaluates only each order's calculate-io. Whatever an order
//! does in handle-io runs only when the trade is taken, so a quote that fills
//! does not prove the take executes. The orders are the source of truth for
//! that, so this runs the real call with `eth_call` and reports what happened;
//! nothing here knows what any order does.
//!
//! Approvals are exact, so at quote time the taker normally has no allowance
//! to the orderbook and may not hold the input token. The call therefore runs
//! with state overrides that give the taker an input-token balance and an
//! allowance to the orderbook for that one call. See `docs/take-simulation.md`.

use crate::types::swap::TakeSimulationSkipReason;
use alloy::primitives::{b256, keccak256, Address, Bytes, B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::state::{AccountOverride, StateOverride};
use alloy::rpc::types::TransactionRequest;
use alloy::sol_types::{Revert, SolCall, SolError, SolValue};
use rain_orderbook_bindings::IRaindexV6::{takeOrders4Call, TakeOrdersConfigV5};
use rain_orderbook_bindings::IERC20::{allowanceCall, balanceOfCall};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock, PoisonError};
use url::Url;

/// Balance and allowance given to the taker for the simulated call. Below
/// 2^96 so it survives tokens that store amounts in narrower integers or pack
/// flags into the high bits (USDC keeps a blacklist flag in bit 255).
const SIMULATED_FUNDS: U256 = U256::from_limbs([0, 1 << 31, 0, 0]);

/// Plain declared storage slots probed for the balance and allowance mappings.
const PROBED_DECLARED_SLOTS: u64 = 32;

/// ERC-7201 namespace of OpenZeppelin's upgradeable ERC-20 (v5): `_balances`
/// is the first member, `_allowances` the second.
const OZ_ERC20_NAMESPACE: B256 =
    b256!("52c63247e1f47db19d5ce0460030c497f067ca4cebf71ba98eeadabe20bace00");

/// How a token derives the storage slot of a `mapping(address => ...)` entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MappingLayout {
    /// Solidity: `keccak256(abi.encode(key, slot))`.
    Solidity(U256),
    /// Vyper: `keccak256(abi.encode(slot, key))`.
    Vyper(U256),
}

impl MappingLayout {
    fn candidates() -> Vec<Self> {
        let namespace = U256::from_be_bytes(OZ_ERC20_NAMESPACE.0);
        let declared = (0..PROBED_DECLARED_SLOTS).map(U256::from);
        declared
            .clone()
            .map(Self::Solidity)
            .chain([
                Self::Solidity(namespace),
                Self::Solidity(namespace + U256::from(1)),
            ])
            .chain(declared.map(Self::Vyper))
            .collect()
    }

    fn entry(self, key: Address) -> B256 {
        match self {
            Self::Solidity(slot) => keccak256((key, slot).abi_encode()),
            Self::Vyper(slot) => keccak256((slot, key).abi_encode()),
        }
    }

    fn balance_slot(self, owner: Address) -> B256 {
        self.entry(owner)
    }

    fn allowance_slot(self, owner: Address, spender: Address) -> B256 {
        let inner = self.entry(owner);
        match self {
            Self::Solidity(_) => keccak256((spender, inner).abi_encode()),
            Self::Vyper(_) => keccak256((inner, spender).abi_encode()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TokenLayout {
    balance: MappingLayout,
    allowance: MappingLayout,
}

/// Layouts found so far, per chain and token. A token's layout does not change
/// short of a proxy upgrade; a cached layout that stops working is dropped
/// (see `funding_reads_back`).
fn token_layouts() -> &'static Mutex<HashMap<(u32, Address), TokenLayout>> {
    static LAYOUTS: OnceLock<Mutex<HashMap<(u32, Address), TokenLayout>>> = OnceLock::new();
    LAYOUTS.get_or_init(Default::default)
}

enum CallFailure {
    /// The node executed the call and it reverted with this data.
    Reverted(Bytes),
    /// No node gave a usable answer.
    Unavailable,
}

/// `eth_call` against each RPC in turn. A revert is an answer, so it is
/// returned at once; anything else moves on to the next RPC.
async fn eth_call(
    rpcs: &[Url],
    request: &TransactionRequest,
    overrides: &StateOverride,
) -> Result<Bytes, CallFailure> {
    for (rpc_index, rpc) in rpcs.iter().enumerate() {
        let provider = ProviderBuilder::new().connect_http(rpc.clone());
        match provider
            .call(request.clone())
            .overrides(overrides.clone())
            .await
        {
            Ok(output) => return Ok(output),
            Err(error) => {
                let Some(response) = error.as_error_resp() else {
                    // Transport errors can carry the RPC URL; keep it out of logs.
                    tracing::warn!(rpc_index, "take simulation RPC transport failed");
                    continue;
                };
                if let Some(data) = response.as_revert_data() {
                    return Err(CallFailure::Reverted(data));
                }
                tracing::warn!(
                    rpc_index,
                    code = response.code,
                    message = %response.message.chars().take(256).collect::<String>(),
                    "take simulation RPC returned an error that is not a revert"
                );
            }
        }
    }
    Err(CallFailure::Unavailable)
}

fn token_override(token: Address, slots: impl IntoIterator<Item = (B256, U256)>) -> StateOverride {
    let state_diff = slots
        .into_iter()
        .map(|(slot, value)| (slot, B256::from(value)))
        .collect();
    let mut overrides = StateOverride::default();
    overrides.insert(
        token,
        AccountOverride {
            state_diff: Some(state_diff),
            ..Default::default()
        },
    );
    overrides
}

fn funding_overrides(
    layout: TokenLayout,
    token: Address,
    taker: Address,
    spender: Address,
) -> StateOverride {
    token_override(
        token,
        [
            (layout.balance.balance_slot(taker), SIMULATED_FUNDS),
            (
                layout.allowance.allowance_slot(taker, spender),
                SIMULATED_FUNDS,
            ),
        ],
    )
}

/// The value written to candidate `index` while probing. Each candidate gets
/// its own value, so one call's answer names the slot the token really reads.
fn probe_value(index: usize) -> U256 {
    SIMULATED_FUNDS - U256::from(1) - U256::from(index)
}

fn probed_candidate(candidates: &[MappingLayout], returned: U256) -> Option<MappingLayout> {
    let index = (SIMULATED_FUNDS - U256::from(1)).checked_sub(returned)?;
    let index = usize::try_from(index).ok()?;
    candidates.get(index).copied()
}

fn decode_word(output: &[u8]) -> Option<U256> {
    (output.len() == 32).then(|| U256::from_be_slice(output))
}

/// Finds which candidate layout a token view reads by overriding every
/// candidate slot with a distinct value and looking at what the view returns.
/// `Ok(None)`: the token answered, but with none of the probe values.
async fn probe_layout(
    rpcs: &[Url],
    token: Address,
    view_calldata: Vec<u8>,
    slot_of: impl Fn(MappingLayout) -> B256,
) -> Result<Option<MappingLayout>, ()> {
    let candidates = MappingLayout::candidates();
    let overrides = token_override(
        token,
        candidates
            .iter()
            .enumerate()
            .map(|(index, layout)| (slot_of(*layout), probe_value(index))),
    );
    let request = TransactionRequest::default()
        .to(token)
        .input(view_calldata.into());
    match eth_call(rpcs, &request, &overrides).await {
        Ok(output) => {
            Ok(decode_word(&output).and_then(|value| probed_candidate(&candidates, value)))
        }
        // A token whose view reverts cannot be funded this way.
        Err(CallFailure::Reverted(_)) => Ok(None),
        Err(CallFailure::Unavailable) => Err(()),
    }
}

async fn token_layout(
    rpcs: &[Url],
    chain_id: u32,
    token: Address,
    taker: Address,
    spender: Address,
) -> Result<Option<TokenLayout>, ()> {
    let cached = token_layouts()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&(chain_id, token))
        .copied();
    if let Some(layout) = cached {
        return Ok(Some(layout));
    }

    let (balance, allowance) = futures::join!(
        probe_layout(
            rpcs,
            token,
            balanceOfCall { account: taker }.abi_encode(),
            |layout| layout.balance_slot(taker),
        ),
        probe_layout(
            rpcs,
            token,
            allowanceCall {
                owner: taker,
                spender
            }
            .abi_encode(),
            |layout| layout.allowance_slot(taker, spender),
        ),
    );
    let (Some(balance), Some(allowance)) = (balance?, allowance?) else {
        return Ok(None);
    };
    let layout = TokenLayout { balance, allowance };
    tracing::info!(
        chain_id,
        %token,
        ?layout,
        "located token balance and allowance storage for take simulation"
    );
    token_layouts()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert((chain_id, token), layout);
    Ok(Some(layout))
}

/// Whether the token reports the simulated balance and allowance under
/// `overrides`. Checked before a revert is reported: if the funding did not
/// take, the revert may be the token transfer rather than the orders.
async fn funding_reads_back(
    rpcs: &[Url],
    token: Address,
    taker: Address,
    spender: Address,
    overrides: &StateOverride,
) -> Result<bool, ()> {
    let balance_request = TransactionRequest::default()
        .to(token)
        .input(balanceOfCall { account: taker }.abi_encode().into());
    let allowance_request = TransactionRequest::default().to(token).input(
        allowanceCall {
            owner: taker,
            spender,
        }
        .abi_encode()
        .into(),
    );
    let (balance, allowance) = futures::join!(
        eth_call(rpcs, &balance_request, overrides),
        eth_call(rpcs, &allowance_request, overrides),
    );
    let funded = |result: Result<Bytes, CallFailure>| match result {
        Ok(output) => Ok(decode_word(&output) == Some(SIMULATED_FUNDS)),
        Err(CallFailure::Reverted(_)) => Ok(false),
        Err(CallFailure::Unavailable) => Err(()),
    };
    Ok(funded(balance)? && funded(allowance)?)
}

/// The revert string when `data` is a standard `Error(string)`, verbatim.
pub(crate) fn decode_revert_reason(data: &[u8]) -> Option<String> {
    Revert::abi_decode(data).ok().map(|revert| revert.reason)
}

/// What one simulated call did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TakeOutcome {
    Executes,
    /// The call reverted with this data while the taker was funded.
    Reverts(Bytes),
    /// No answer. Never to be read as either of the above.
    NotSimulated(TakeSimulationSkipReason),
}

pub(crate) struct TakeSimulationRequest<'a> {
    pub rpcs: &'a [Url],
    pub chain_id: u32,
    /// `msg.sender` of the take: the address the calldata would be built for.
    pub taker: Address,
    /// The orderbook: call target and spender of the input token.
    pub raindex: Address,
    pub input_token: Address,
    pub config: &'a TakeOrdersConfigV5,
    /// On a revert, check that the token really reported the simulated funds
    /// before blaming the take. Costs two calls; once per token and round is
    /// enough, since every call in a round uses the same overrides.
    pub verify_funding: bool,
}

/// Simulates `takeOrders4(config)` on `raindex` from `taker`, with the taker
/// funded for the call. Never fails: every way of not getting an answer is
/// `NotSimulated`.
pub(crate) async fn simulate_take(request: TakeSimulationRequest<'_>) -> TakeOutcome {
    let TakeSimulationRequest {
        rpcs,
        chain_id,
        taker,
        raindex,
        input_token,
        config,
        verify_funding,
    } = request;
    if rpcs.is_empty() {
        return TakeOutcome::NotSimulated(TakeSimulationSkipReason::RpcUnavailable);
    }

    let layout = match token_layout(rpcs, chain_id, input_token, taker, raindex).await {
        Ok(Some(layout)) => layout,
        Ok(None) => {
            return TakeOutcome::NotSimulated(TakeSimulationSkipReason::OverrideUnavailable)
        }
        Err(()) => return TakeOutcome::NotSimulated(TakeSimulationSkipReason::RpcUnavailable),
    };
    let overrides = funding_overrides(layout, input_token, taker, raindex);
    let take = TransactionRequest::default().from(taker).to(raindex).input(
        takeOrders4Call {
            config: config.clone(),
        }
        .abi_encode()
        .into(),
    );

    let data = match eth_call(rpcs, &take, &overrides).await {
        Ok(_) => return TakeOutcome::Executes,
        Err(CallFailure::Unavailable) => {
            return TakeOutcome::NotSimulated(TakeSimulationSkipReason::RpcUnavailable)
        }
        Err(CallFailure::Reverted(data)) => data,
    };
    if !verify_funding {
        return TakeOutcome::Reverts(data);
    }
    match funding_reads_back(rpcs, input_token, taker, raindex, &overrides).await {
        Ok(true) => TakeOutcome::Reverts(data),
        Ok(false) => {
            token_layouts()
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&(chain_id, input_token));
            tracing::warn!(
                chain_id,
                %input_token,
                "take simulation funding did not read back; dropped cached token layout"
            );
            TakeOutcome::NotSimulated(TakeSimulationSkipReason::OverrideUnavailable)
        }
        Err(()) => TakeOutcome::NotSimulated(TakeSimulationSkipReason::RpcUnavailable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;

    const OWNER: Address = address!("d2843d9e7738d46d90cb6dff8d6c83db58b9c165");
    const SPENDER: Address = address!("e522cB4a5fCb2eb31a52Ff41a4653d85A4fd7C9D");

    #[test]
    fn simulated_funds_fit_narrow_and_flagged_balances() {
        assert_eq!(SIMULATED_FUNDS, U256::from(1) << 95);
    }

    #[test]
    fn solidity_slots_match_the_compiler_layout() {
        // Slots 9 and 10 are `balanceAndBlacklistStates` and `allowed` in Base
        // USDC; the expected words are keccak256 of the ABI encoding by hand.
        let balance = MappingLayout::Solidity(U256::from(9));
        let mut preimage = [0u8; 64];
        preimage[12..32].copy_from_slice(OWNER.as_slice());
        preimage[63] = 9;
        assert_eq!(balance.balance_slot(OWNER), keccak256(preimage));

        let allowance = MappingLayout::Solidity(U256::from(10));
        preimage[63] = 10;
        let inner = keccak256(preimage);
        let mut outer = [0u8; 64];
        outer[12..32].copy_from_slice(SPENDER.as_slice());
        outer[32..].copy_from_slice(inner.as_slice());
        assert_eq!(allowance.allowance_slot(OWNER, SPENDER), keccak256(outer));
    }

    #[test]
    fn vyper_slots_put_the_slot_before_the_key() {
        let layout = MappingLayout::Vyper(U256::from(3));
        let mut preimage = [0u8; 64];
        preimage[31] = 3;
        preimage[44..].copy_from_slice(OWNER.as_slice());
        let inner = keccak256(preimage);
        assert_eq!(layout.balance_slot(OWNER), inner);

        let mut outer = [0u8; 64];
        outer[..32].copy_from_slice(inner.as_slice());
        outer[44..].copy_from_slice(SPENDER.as_slice());
        assert_eq!(layout.allowance_slot(OWNER, SPENDER), keccak256(outer));
    }

    #[test]
    fn every_candidate_has_its_own_slot_and_probe_value() {
        let candidates = MappingLayout::candidates();
        let balance_slots: std::collections::HashSet<_> = candidates
            .iter()
            .map(|layout| layout.balance_slot(OWNER))
            .collect();
        let allowance_slots: std::collections::HashSet<_> = candidates
            .iter()
            .map(|layout| layout.allowance_slot(OWNER, SPENDER))
            .collect();
        assert_eq!(balance_slots.len(), candidates.len());
        assert_eq!(allowance_slots.len(), candidates.len());

        for (index, layout) in candidates.iter().enumerate() {
            assert_eq!(
                probed_candidate(&candidates, probe_value(index)),
                Some(*layout)
            );
        }
    }

    #[test]
    fn probe_ignores_values_that_are_not_probe_values() {
        let candidates = MappingLayout::candidates();
        assert_eq!(probed_candidate(&candidates, U256::ZERO), None);
        assert_eq!(probed_candidate(&candidates, SIMULATED_FUNDS), None);
        assert_eq!(
            probed_candidate(&candidates, probe_value(candidates.len())),
            None
        );
        assert_eq!(probed_candidate(&candidates, U256::MAX), None);
    }

    #[test]
    fn funding_overrides_touch_only_the_two_token_slots() {
        let layout = TokenLayout {
            balance: MappingLayout::Solidity(U256::from(9)),
            allowance: MappingLayout::Solidity(U256::from(10)),
        };
        let token = address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
        let overrides = funding_overrides(layout, token, OWNER, SPENDER);

        assert_eq!(overrides.len(), 1);
        let account = overrides.get(&token).expect("token override");
        assert!(account.state.is_none() && account.code.is_none() && account.balance.is_none());
        let diff = account.state_diff.as_ref().expect("state diff");
        assert_eq!(diff.len(), 2);
        assert_eq!(
            diff.get(&layout.balance.balance_slot(OWNER)),
            Some(&B256::from(SIMULATED_FUNDS))
        );
        assert_eq!(
            diff.get(&layout.allowance.allowance_slot(OWNER, SPENDER)),
            Some(&B256::from(SIMULATED_FUNDS))
        );
    }

    #[test]
    fn revert_reason_is_the_error_string_verbatim() {
        let data = Revert::from("Min trade amount").abi_encode();
        assert_eq!(
            decode_revert_reason(&data),
            Some("Min trade amount".to_string())
        );
    }

    #[test]
    fn revert_reason_is_absent_for_other_revert_data() {
        assert_eq!(decode_revert_reason(&[]), None);
        assert_eq!(decode_revert_reason(&[0xde, 0xad, 0xbe, 0xef]), None);
    }

    #[rocket::async_test]
    async fn no_rpc_means_not_simulated() {
        let config = TakeOrdersConfigV5 {
            minimumIO: B256::ZERO,
            maximumIO: B256::ZERO,
            maximumIORatio: B256::ZERO,
            IOIsInput: false,
            orders: vec![],
            data: Bytes::new(),
        };
        let simulation = simulate_take(TakeSimulationRequest {
            rpcs: &[],
            chain_id: 8453,
            taker: OWNER,
            raindex: SPENDER,
            input_token: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
            config: &config,
            verify_funding: true,
        })
        .await;
        assert_eq!(
            simulation,
            TakeOutcome::NotSimulated(TakeSimulationSkipReason::RpcUnavailable)
        );
    }

    #[rocket::async_test]
    async fn unreachable_rpc_means_not_simulated() {
        let config = TakeOrdersConfigV5 {
            minimumIO: B256::ZERO,
            maximumIO: B256::ZERO,
            maximumIORatio: B256::ZERO,
            IOIsInput: false,
            orders: vec![],
            data: Bytes::new(),
        };
        // Port 9 (discard) on loopback: nothing answers JSON-RPC there.
        let rpcs = [Url::parse("http://127.0.0.1:9").expect("static url")];
        let simulation = simulate_take(TakeSimulationRequest {
            rpcs: &rpcs,
            chain_id: 8453,
            taker: OWNER,
            raindex: SPENDER,
            input_token: address!("0000000000000000000000000000000000000abc"),
            config: &config,
            verify_funding: true,
        })
        .await;
        assert_eq!(
            simulation,
            TakeOutcome::NotSimulated(TakeSimulationSkipReason::RpcUnavailable)
        );
    }
}
