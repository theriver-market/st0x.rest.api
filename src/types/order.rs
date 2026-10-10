use crate::types::common::{Approval, Denomination, TokenRef};
use alloy::primitives::{Address, Bytes, FixedBytes, U256};
use rocket::form::FromForm;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PeriodUnit {
    Days,
    Hours,
    Minutes,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeployDcaOrderRequest {
    #[schema(example = 8453)]
    pub chain_id: Option<u32>,
    #[schema(value_type = String, example = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")]
    pub input_token: Address,
    #[schema(value_type = String, example = "0x4200000000000000000000000000000000000006")]
    pub output_token: Address,
    #[schema(example = "1000000")]
    pub budget_amount: String,
    #[schema(example = 4)]
    pub period: u32,
    #[schema(example = "hours")]
    pub period_unit: PeriodUnit,
    #[schema(example = "0.0005")]
    pub start_io: String,
    #[schema(example = "0.0003")]
    pub floor_io: String,
    #[schema(value_type = Option<String>)]
    pub input_vault_id: Option<U256>,
    #[schema(value_type = Option<String>)]
    pub output_vault_id: Option<U256>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeploySolverOrderRequest {
    #[schema(example = 8453)]
    pub chain_id: Option<u32>,
    #[schema(value_type = String, example = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")]
    pub input_token: Address,
    #[schema(value_type = String, example = "0x4200000000000000000000000000000000000006")]
    pub output_token: Address,
    #[schema(example = "1000000")]
    pub amount: String,
    #[schema(example = "0.0005")]
    pub io_ratio: String,
    #[schema(value_type = Option<String>)]
    pub input_vault_id: Option<U256>,
    #[schema(value_type = Option<String>)]
    pub output_vault_id: Option<U256>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeployOrderResponse {
    #[schema(example = 8453)]
    pub chain_id: u32,
    #[schema(value_type = String, example = "0xDEF171Fe48CF0115B1d80b88dc8eAB59176FEe57")]
    pub to: Address,
    #[schema(value_type = String, example = "0xabcdef...")]
    pub data: Bytes,
    #[schema(value_type = String, example = "0x0")]
    pub value: U256,
    pub approvals: Vec<Approval>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CancelOrderRequest {
    #[schema(example = 8453)]
    pub chain_id: Option<u32>,
    #[schema(value_type = String, example = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab")]
    pub order_hash: FixedBytes<32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CancelTransaction {
    #[schema(value_type = String, example = "0xDEF171Fe48CF0115B1d80b88dc8eAB59176FEe57")]
    pub to: Address,
    #[schema(value_type = String, example = "0xabcdef...")]
    pub data: Bytes,
    #[schema(value_type = String, example = "0x0")]
    pub value: U256,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TokenReturn {
    #[schema(value_type = String, example = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")]
    pub token: Address,
    #[schema(example = "USDC")]
    pub symbol: String,
    #[schema(example = "1000000")]
    pub amount: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CancelSummary {
    #[schema(example = 2)]
    pub vaults_to_withdraw: u32,
    pub tokens_returned: Vec<TokenReturn>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CancelOrderResponse {
    #[schema(example = 8453)]
    pub chain_id: u32,
    pub transactions: Vec<CancelTransaction>,
    pub summary: CancelSummary,
}

#[derive(Debug, Clone, FromForm, Serialize, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(rename_all = "camelCase")]
pub struct OrderDetailParams {
    #[field(name = "chainId")]
    #[param(example = 8453)]
    pub chain_id: Option<u32>,
    #[field(name = "denomination")]
    #[param(example = "wrapped")]
    pub denomination: Option<Denomination>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum OrderType {
    Dca,
    Solver,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OrderDetailsInfo {
    #[serde(rename = "type")]
    #[schema(example = "dca")]
    pub type_: OrderType,
    #[schema(example = "0.0005")]
    pub io_ratio: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OrderTradeEntry {
    #[schema(example = 8453)]
    pub chain_id: u32,
    #[schema(example = "trade-1")]
    pub id: String,
    #[schema(value_type = String, example = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab")]
    pub tx_hash: FixedBytes<32>,
    #[schema(example = "1000000")]
    pub input_amount: String,
    #[schema(example = "500000")]
    pub output_amount: String,
    #[schema(example = 1718452800)]
    pub timestamp: u64,
    #[schema(value_type = String, example = "0x1234567890abcdef1234567890abcdef12345678")]
    pub sender: Address,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OrderDetail {
    #[schema(example = 8453)]
    pub chain_id: u32,
    #[schema(value_type = String, example = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab")]
    pub order_hash: FixedBytes<32>,
    #[schema(value_type = String, example = "0x1234567890abcdef1234567890abcdef12345678")]
    pub owner: Address,
    pub order_details: OrderDetailsInfo,
    pub input_token: TokenRef,
    pub output_token: TokenRef,
    #[schema(value_type = String, example = "0x1")]
    pub input_vault_id: U256,
    #[schema(value_type = String, example = "0x2")]
    pub output_vault_id: U256,
    #[schema(example = "1000000")]
    pub input_vault_balance: String,
    #[schema(example = "500000")]
    pub output_vault_balance: String,
    #[schema(example = "0.0005")]
    pub io_ratio: String,
    #[schema(example = 1718452800)]
    pub created_at: u64,
    #[schema(value_type = String, example = "0x1234567890abcdef1234567890abcdef12345678")]
    pub orderbook_id: Address,
    pub trades: Vec<OrderTradeEntry>,
    /// The order's Rainlang from its onchain meta (falls back to the dotrain source), for
    /// classifying strategy type. Omitted when the order has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(example = "#calculate-io\nmax-output: 100,\nio-ratio: 0.0005;\n#handle-io\n:;")]
    pub rainlang: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_period_unit_variants() {
        let variants = [
            ("\"minutes\"", PeriodUnit::Minutes),
            ("\"hours\"", PeriodUnit::Hours),
            ("\"days\"", PeriodUnit::Days),
        ];
        for (json, expected) in variants {
            let parsed: PeriodUnit = serde_json::from_str(json).unwrap();
            assert_eq!(parsed, expected);
        }
    }

    #[test]
    fn test_period_unit_rejects_invalid() {
        let result = serde_json::from_str::<PeriodUnit>("\"seconds\"");
        assert!(result.is_err());
        let result = serde_json::from_str::<PeriodUnit>("\"weeks\"");
        assert!(result.is_err());
        let result = serde_json::from_str::<PeriodUnit>("\"months\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_order_details_info_type_rename() {
        let info = OrderDetailsInfo {
            type_: OrderType::Dca,
            io_ratio: "0.0005".into(),
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("\"type\":\"dca\""));
        assert!(!json.contains("\"type_\""));
    }
}
