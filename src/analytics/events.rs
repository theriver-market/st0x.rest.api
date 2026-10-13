//! Event builders and shared attribution helpers.
//!
//! Every event carries `api_client_{key_id,label,owner}` so volume and users can be
//! sliced by client (site vs. integrator). "Site vs. integrator" is defined in
//! PostHog by filtering on `api_client_owner` rather than hardcoded here.

use super::AnalyticsEvent;
use crate::auth::{AuthClientInfo, AuthenticatedKey};
use crate::error::ApiError;
use crate::types::swap::{SwapCalldataResponse, SwapQuoteResponse, SwapQuoteV2Response};
use alloy::primitives::Address;
use rain_math_float::Float;
use serde_json::{Map, Value};

const MAX_ANALYTICS_AMOUNT_LENGTH: usize = 128;

/// Which swap API version produced an event.
#[derive(Clone, Copy)]
pub(crate) enum ApiVersion {
    V1,
    V2,
    V3,
}

impl ApiVersion {
    fn as_str(&self) -> &'static str {
        match self {
            ApiVersion::V1 => "v1",
            ApiVersion::V2 => "v2",
            ApiVersion::V3 => "v3",
        }
    }
}

/// Client-attribution properties present on every API analytics event.
fn base_props(info: &AuthClientInfo) -> Map<String, Value> {
    let mut props = Map::new();
    props.insert("api_client_key_id".to_string(), info.key_id.clone().into());
    props.insert("api_client_label".to_string(), info.label.clone().into());
    props.insert("api_client_owner".to_string(), info.owner.clone().into());
    props
}

/// `distinct_id` for events attributed to the calling client rather than an end user.
fn client_distinct_id(info: &AuthClientInfo) -> String {
    format!("client:{}", info.key_id)
}

/// Collapse high-cardinality path segments (`0x…` addresses/hashes, numeric ids) into
/// placeholders so `endpoint` stays low-cardinality in analytics.
fn normalize_path(path: &str) -> String {
    path.split('/')
        .map(|segment| {
            if segment.len() >= 10
                && segment
                    .get(..2)
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case("0x"))
                && segment[2..].chars().all(|c| c.is_ascii_hexdigit())
            {
                "{address}"
            } else if !segment.is_empty() && segment.chars().all(|c| c.is_ascii_digit()) {
                "{id}"
            } else {
                segment
            }
            .to_string()
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Generic per-request usage event, emitted for every authenticated request.
pub(crate) fn api_request_event(
    info: &AuthClientInfo,
    method: &str,
    path: &str,
    status_code: i32,
    latency_ms: f64,
) -> AnalyticsEvent {
    let mut props = base_props(info);
    props.insert("method".to_string(), method.into());
    props.insert("endpoint".to_string(), normalize_path(path).into());
    props.insert("status_code".to_string(), status_code.into());
    props.insert("latency_ms".to_string(), latency_ms.into());
    AnalyticsEvent {
        event: "api_request",
        distinct_id: client_distinct_id(info),
        properties: props,
    }
}

/// Swap quote (trade intent) event. Attributed to the client (quotes carry no wallet).
pub(crate) fn swap_quoted_event(
    key: &AuthenticatedKey,
    input_token: Address,
    output_token: Address,
    denomination: Value,
    resp: &SwapQuoteResponse,
) -> AnalyticsEvent {
    let info = key.client_info();
    let mut props = base_props(&info);
    props.insert("chain_id".to_string(), resp.chain_id.into());
    props.insert("input_token".to_string(), token_str(input_token).into());
    props.insert("output_token".to_string(), token_str(output_token).into());
    props.insert("denomination".to_string(), denomination);
    props.insert(
        "estimated_input".to_string(),
        resp.estimated_input.clone().into(),
    );
    props.insert(
        "estimated_output".to_string(),
        resp.estimated_output.clone().into(),
    );
    props.insert(
        "estimated_io_ratio".to_string(),
        resp.estimated_io_ratio.clone().into(),
    );
    props.insert("api_version".to_string(), ApiVersion::V1.as_str().into());
    AnalyticsEvent {
        event: "swap_quoted",
        distinct_id: client_distinct_id(&info),
        properties: props,
    }
}

/// Mode-based swap quote event. Attributed to the client, with the optional
/// taker deliberately omitted from analytics because this is still quote intent.
pub(crate) fn swap_quoted_v2_event(
    key: &AuthenticatedKey,
    resp: &SwapQuoteV2Response,
) -> AnalyticsEvent {
    let info = key.client_info();
    let mut props = base_props(&info);
    props.insert("chain_id".to_string(), resp.chain_id.into());
    props.insert(
        "input_token".to_string(),
        token_str(resp.input_token).into(),
    );
    props.insert(
        "output_token".to_string(),
        token_str(resp.output_token).into(),
    );
    props.insert(
        "denomination".to_string(),
        serde_json::to_value(resp.denomination).unwrap_or(Value::Null),
    );
    props.insert(
        "mode".to_string(),
        serde_json::to_value(resp.mode).unwrap_or(Value::Null),
    );
    props.insert(
        "estimated_input".to_string(),
        resp.estimated_input.clone().into(),
    );
    props.insert(
        "estimated_output".to_string(),
        resp.estimated_output.clone().into(),
    );
    props.insert(
        "estimated_io_ratio".to_string(),
        resp.estimated_io_ratio.clone().into(),
    );
    props.insert("fully_filled".to_string(), resp.fully_filled.into());
    props.insert(
        "take_simulation_status".to_string(),
        serde_json::to_value(resp.take_simulation.status).unwrap_or(Value::Null),
    );
    props.insert(
        "excluded_order_count".to_string(),
        resp.take_simulation.excluded_orders.len().into(),
    );
    props.insert(
        "resolved_price_cap".to_string(),
        resp.resolved_price_cap.clone().into(),
    );
    props.insert("api_version".to_string(), ApiVersion::V2.as_str().into());
    AnalyticsEvent {
        event: "swap_quoted",
        distinct_id: client_distinct_id(&info),
        properties: props,
    }
}

/// Swap calldata (execution-intent) event — the closest proxy the API has to a real
/// trade, and the one event carrying the end-user wallet (`taker`). `distinct_id` is
/// the lowercased wallet so a trader is one PostHog person across the site and every
/// integrator (cross-venue uniqueness).
#[allow(clippy::too_many_arguments)]
pub(crate) fn swap_calldata_generated_event(
    key: &AuthenticatedKey,
    taker: Address,
    input_token: Address,
    output_token: Address,
    denomination: Value,
    api_version: ApiVersion,
    mode: Option<Value>,
    resp: &SwapCalldataResponse,
) -> AnalyticsEvent {
    let info = key.client_info();
    let taker_id = token_str(taker);
    let mut props = base_props(&info);
    props.insert("chain_id".to_string(), resp.chain_id.into());
    props.insert("taker".to_string(), taker_id.clone().into());
    props.insert("input_token".to_string(), token_str(input_token).into());
    props.insert("output_token".to_string(), token_str(output_token).into());
    props.insert("denomination".to_string(), denomination);
    props.insert(
        "estimated_input".to_string(),
        resp.estimated_input.clone().into(),
    );
    props.insert("value".to_string(), resp.value.to_string().into());
    props.insert("api_version".to_string(), api_version.as_str().into());
    if let Some(mode) = mode {
        props.insert("mode".to_string(), mode);
    }
    AnalyticsEvent {
        event: "swap_calldata_generated",
        distinct_id: taker_id,
        properties: props,
    }
}

/// What a caller asked for on a swap request that then failed.
///
/// The success events describe a *response*, so they can only ever describe requests
/// that worked — a client failing 100% of the time is indistinguishable from one
/// sending no traffic at all. This carries the *request* instead, captured before it
/// is consumed, so a failure reports the pair and size that produced it.
pub(crate) struct SwapFailure<'a> {
    pub chain_id: Option<u32>,
    pub input_token: Address,
    pub output_token: Address,
    /// The requested amount, verbatim. Which side it denominates depends on the
    /// endpoint (v1 quote takes an output amount, v2 an amount plus a mode), so it
    /// is reported as-is alongside `api_version` rather than being reinterpreted.
    pub requested_amount: &'a str,
    pub denomination: Value,
    pub api_version: ApiVersion,
    pub mode: Option<Value>,
    /// Present only on calldata requests, which carry an end-user wallet.
    pub taker: Option<Address>,
}

/// Failed swap quote. Attributed to the client: a failing quote is an integration
/// problem, so the useful grouping is "which client", not "which wallet".
pub(crate) fn swap_quote_failed_event(
    key: &AuthenticatedKey,
    failure: SwapFailure<'_>,
    error: &ApiError,
) -> AnalyticsEvent {
    swap_failed_event("swap_quote_failed", key, failure, error)
}

/// Failed swap calldata. Attributed to the client for the same reason as
/// [`swap_quote_failed_event`]; the wallet is still reported as a `taker` property.
pub(crate) fn swap_calldata_failed_event(
    key: &AuthenticatedKey,
    failure: SwapFailure<'_>,
    error: &ApiError,
) -> AnalyticsEvent {
    swap_failed_event("swap_calldata_failed", key, failure, error)
}

fn swap_failed_event(
    event: &'static str,
    key: &AuthenticatedKey,
    failure: SwapFailure<'_>,
    error: &ApiError,
) -> AnalyticsEvent {
    let info = key.client_info();
    let code = error.code();
    let mut props = base_props(&info);
    if let Some(chain_id) = failure.chain_id {
        props.insert("chain_id".to_string(), chain_id.into());
    }
    props.insert(
        "input_token".to_string(),
        token_str(failure.input_token).into(),
    );
    props.insert(
        "output_token".to_string(),
        token_str(failure.output_token).into(),
    );
    if analytics_requested_amount(failure.requested_amount) {
        props.insert(
            "requested_amount".to_string(),
            failure.requested_amount.into(),
        );
    }
    props.insert("denomination".to_string(), failure.denomination);
    props.insert(
        "api_version".to_string(),
        failure.api_version.as_str().into(),
    );
    props.insert("error_code".to_string(), code.as_str().into());
    props.insert("status_code".to_string(), code.status().code.into());
    // Precomputed because it is the failure mode that is invisible in aggregate:
    // a same-token pair is always a caller bug, and grouping on two address columns
    // to notice it is exactly the step nobody takes.
    props.insert(
        "same_token".to_string(),
        (failure.input_token == failure.output_token).into(),
    );
    if let Some(mode) = failure.mode {
        props.insert("mode".to_string(), mode);
    }
    if let Some(taker) = failure.taker {
        props.insert("taker".to_string(), token_str(taker).into());
    }
    AnalyticsEvent {
        event,
        distinct_id: client_distinct_id(&info),
        properties: props,
    }
}

/// Keep arbitrary request text out of PostHog while preserving valid trade sizes.
///
/// Swap amount fields deserialize as strings and failure events include parse errors,
/// so a failed request is not proof that the value is numeric. The length bound also
/// keeps an authenticated caller from creating oversized analytics properties.
fn analytics_requested_amount(amount: &str) -> bool {
    amount.len() <= MAX_ANALYTICS_AMOUNT_LENGTH && Float::parse(amount.to_string()).is_ok()
}

/// Lowercased `0x…` address string. Matches the site's `posthog.identify()` form so
/// wallet identities line up across venues.
fn token_str(address: Address) -> String {
    address.to_string().to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::swap::SwapDenomination;

    fn test_key() -> AuthenticatedKey {
        AuthenticatedKey {
            id: 1,
            key_id: "site-key".to_string(),
            label: "St0x Website".to_string(),
            owner: "st0x".to_string(),
            is_admin: false,
            swap_max_concurrent: None,
        }
    }

    fn addr(byte: u8) -> Address {
        Address::from([byte; 20])
    }

    #[test]
    fn normalize_path_collapses_addresses_and_ids() {
        assert_eq!(
            normalize_path("/v1/tokens/0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913/details"),
            "/v1/tokens/{address}/details"
        );
        assert_eq!(normalize_path("/v1/tokens"), "/v1/tokens");
        assert_eq!(normalize_path("/v1/order/12345"), "/v1/order/{id}");
        assert_eq!(normalize_path("/health"), "/health");
    }

    #[test]
    fn api_request_event_carries_client_attribution() {
        let info = test_key().client_info();
        let event = api_request_event(&info, "GET", "/v1/tokens", 200, 4.2);
        assert_eq!(event.event, "api_request");
        assert_eq!(event.distinct_id, "client:site-key");
        assert_eq!(event.properties["method"], Value::from("GET"));
        assert_eq!(event.properties["endpoint"], Value::from("/v1/tokens"));
        assert_eq!(event.properties["status_code"], Value::from(200));
        assert_eq!(event.properties["api_client_owner"], Value::from("st0x"));
    }

    #[test]
    fn swap_quoted_event_is_client_scoped() {
        let resp = SwapQuoteResponse {
            chain_id: 8453,
            input_token: addr(1),
            output_token: addr(2),
            output_amount: "0.5".to_string(),
            denomination: SwapDenomination::Wrapped,
            estimated_output: "0.5".to_string(),
            estimated_input: "1250.75".to_string(),
            estimated_io_ratio: "2501.5".to_string(),
        };
        let event = swap_quoted_event(&test_key(), addr(1), addr(2), Value::from("wrapped"), &resp);
        assert_eq!(event.event, "swap_quoted");
        assert_eq!(event.distinct_id, "client:site-key");
        assert_eq!(event.properties["estimated_input"], Value::from("1250.75"));
        assert_eq!(event.properties["api_version"], Value::from("v1"));
        assert_eq!(
            event.properties["api_client_label"],
            Value::from("St0x Website")
        );
    }

    #[test]
    fn swap_quote_failed_event_reports_the_request_and_the_wire_error() {
        let event = swap_quote_failed_event(
            &test_key(),
            SwapFailure {
                chain_id: Some(8453),
                input_token: addr(1),
                output_token: addr(2),
                requested_amount: "0.01",
                denomination: Value::from("wrapped"),
                api_version: ApiVersion::V1,
                mode: None,
                taker: None,
            },
            &crate::error::ApiError::coded(
                crate::error::ApiErrorCode::SwapNoLiquidity,
                "no executable liquidity is available for this pair",
            ),
        );

        assert_eq!(event.event, "swap_quote_failed");
        // Client-scoped: a failing quote is an integration problem, not a wallet one.
        assert_eq!(event.distinct_id, "client:site-key");
        assert_eq!(event.properties["requested_amount"], Value::from("0.01"));
        assert_eq!(
            event.properties["error_code"],
            Value::from("SWAP_NO_LIQUIDITY")
        );
        // Mirrors the HTTP status the caller actually received.
        assert_eq!(event.properties["status_code"], Value::from(404));
        assert_eq!(event.properties["same_token"], Value::from(false));
        assert!(event.properties.get("taker").is_none());
        assert!(event.properties.get("mode").is_none());
    }

    #[test]
    fn swap_failure_omits_non_numeric_requested_amount() {
        let event = swap_quote_failed_event(
            &test_key(),
            SwapFailure {
                chain_id: Some(8453),
                input_token: addr(1),
                output_token: addr(2),
                requested_amount: "customer@example.com",
                denomination: Value::from("wrapped"),
                api_version: ApiVersion::V1,
                mode: None,
                taker: None,
            },
            &crate::error::ApiError::BadRequest("invalid output_amount".into()),
        );

        assert!(event.properties.get("requested_amount").is_none());
    }

    #[test]
    fn analytics_requested_amount_enforces_length_boundary() {
        assert!(analytics_requested_amount(&format!("{}1", "0".repeat(127))));
        assert!(!analytics_requested_amount(&format!(
            "{}1",
            "0".repeat(128)
        )));
    }

    #[test]
    fn swap_failure_flags_a_same_token_pair() {
        let event = swap_calldata_failed_event(
            &test_key(),
            SwapFailure {
                chain_id: Some(8453),
                input_token: addr(7),
                output_token: addr(7),
                requested_amount: "100",
                denomination: Value::from("wrapped"),
                api_version: ApiVersion::V2,
                mode: Some(Value::from("spendExact")),
                taker: Some(addr(0xAB)),
            },
            &crate::error::ApiError::coded(
                crate::error::ApiErrorCode::SwapSameToken,
                "inputToken and outputToken must be different tokens",
            ),
        );

        assert_eq!(event.event, "swap_calldata_failed");
        assert_eq!(event.properties["same_token"], Value::from(true));
        assert_eq!(event.properties["status_code"], Value::from(400));
        assert_eq!(event.properties["api_version"], Value::from("v2"));
        assert_eq!(event.properties["mode"], Value::from("spendExact"));
        assert_eq!(
            event.properties["taker"],
            Value::from(addr(0xAB).to_string().to_lowercase())
        );
    }

    #[test]
    fn swap_calldata_event_is_keyed_on_lowercased_taker() {
        let resp = SwapCalldataResponse {
            chain_id: 8453,
            to: addr(9),
            data: Default::default(),
            value: alloy::primitives::U256::from(1000u64),
            estimated_input: "1250.75".to_string(),
            denomination: SwapDenomination::Wrapped,
            approvals: vec![],
        };
        let taker = addr(0xAB);
        let event = swap_calldata_generated_event(
            &test_key(),
            taker,
            addr(1),
            addr(2),
            Value::from("wrapped"),
            ApiVersion::V1,
            None,
            &resp,
        );
        assert_eq!(event.event, "swap_calldata_generated");
        // distinct_id is the lowercased wallet, matching the site's identify() form.
        assert_eq!(event.distinct_id, taker.to_string().to_lowercase());
        assert_eq!(
            event.distinct_id,
            event.properties["taker"].as_str().unwrap()
        );
        assert_eq!(event.properties["value"], Value::from("1000"));
        assert_eq!(event.properties["api_version"], Value::from("v1"));
        assert!(event.properties.get("mode").is_none());
    }
}
