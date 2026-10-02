use crate::types::common::{Denomination, TokenRef};
use alloy::primitives::{Address, Bytes, FixedBytes};
use rocket::form::{FromForm, FromFormField};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Clone, FromForm, Serialize, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(rename_all = "camelCase")]
pub struct OrdersPaginationParams {
    #[field(name = "chainId")]
    #[param(example = 8453)]
    pub chain_id: Option<u32>,
    #[field(name = "state")]
    pub state: Option<OrderState>,
    #[field(name = "page")]
    #[param(example = 1)]
    pub page: Option<u16>,
    #[field(name = "pageSize")]
    #[param(example = 20)]
    pub page_size: Option<u16>,
    #[field(name = "denomination")]
    #[param(example = "wrapped")]
    pub denomination: Option<Denomination>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, FromFormField, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum OrderSide {
    #[field(value = "input")]
    Input,
    #[field(value = "output")]
    Output,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, FromFormField, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OrderState {
    #[field(value = "active")]
    Active,
    #[field(value = "inactive")]
    Inactive,
    #[field(value = "all")]
    All,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum OrderSummaryOrderType {
    Limit,
    Dca,
    DynamicSpread,
    Custom,
    /// The River's buy-the-dip ladder (river-ladder).
    Ladder,
    /// The River's DIA-triggered stop-loss (river-stop).
    Stop,
    /// The River's buy/sell cycle (river-cycle).
    Cycle,
}

#[derive(Debug, Clone, FromForm, Serialize, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(rename_all = "camelCase")]
pub struct OrdersByTokenParams {
    #[field(name = "chainId")]
    #[param(example = 8453)]
    pub chain_id: Option<u32>,
    #[field(name = "state")]
    pub state: Option<OrderState>,
    #[field(name = "side")]
    pub side: Option<OrderSide>,
    #[field(name = "page")]
    #[param(example = 1)]
    pub page: Option<u16>,
    #[field(name = "pageSize")]
    #[param(example = 20)]
    pub page_size: Option<u16>,
    #[field(name = "denomination")]
    #[param(example = "wrapped")]
    pub denomination: Option<Denomination>,
}

#[derive(Debug, Clone, FromForm, Serialize, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(rename_all = "camelCase")]
pub struct OrdersByTxParams {
    #[field(name = "chainId")]
    #[param(example = 8453)]
    pub chain_id: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OrdersQueryRequest {
    /// Network to query. The ID must exist in the active Raindex registry.
    #[schema(example = 8453)]
    pub chain_id: u32,
    /// Match any of these tokens on the selected side. Addresses are
    /// case-normalized and deduplicated before querying.
    #[schema(
        value_type = Vec<String>,
        example = json!([
            "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "0x4200000000000000000000000000000000000006"
        ])
    )]
    #[serde(default)]
    pub token_addresses: Vec<String>,
    /// Optional owner filter. Addresses are case-normalized and deduplicated.
    #[serde(default)]
    #[schema(value_type = Vec<String>)]
    pub owner_addresses: Vec<String>,
    /// Optional Raindex contract filter. Addresses are case-normalized and
    /// deduplicated.
    #[serde(default)]
    #[schema(value_type = Vec<String>)]
    pub raindex_addresses: Vec<String>,
    /// Optional exact order hash. The pinned SDK exposes one exact hash on
    /// `GetOrdersFilters`; it does not expose an order-hash set.
    #[schema(
        value_type = Option<String>,
        example = "0x000000000000000000000000000000000000000000000000000000000000abcd"
    )]
    pub order_hash: Option<String>,
    pub state: Option<OrderState>,
    pub side: Option<OrderSide>,
    #[schema(example = 1, minimum = 1)]
    pub page: Option<u16>,
    #[schema(example = 20, minimum = 1, maximum = 50)]
    pub page_size: Option<u16>,
    #[schema(example = "wrapped")]
    pub denomination: Option<Denomination>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OrderSummary {
    #[schema(value_type = String, example = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab")]
    pub order_hash: FixedBytes<32>,
    #[schema(value_type = String, example = "0x1234567890abcdef1234567890abcdef12345678")]
    pub owner: Address,
    #[schema(example = 8453)]
    pub chain_id: u32,
    #[schema(value_type = String, example = "0x01")]
    pub order_bytes: Bytes,
    #[schema(example = true)]
    pub active: bool,
    #[schema(example = 1718452900)]
    pub removed_at: Option<u64>,
    #[schema(example = "limit")]
    pub order_type: OrderSummaryOrderType,
    pub input_token: TokenRef,
    pub output_token: TokenRef,
    #[schema(example = "500000")]
    pub output_vault_balance: String,
    #[schema(example = "500000")]
    pub max_output: Option<String>,
    #[schema(example = "0.0005")]
    pub io_ratio: String,
    #[schema(example = 1718452800)]
    pub created_at: u64,
    #[schema(value_type = String, example = "0x1234567890abcdef1234567890abcdef12345678")]
    pub orderbook_id: Address,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OrdersPagination {
    #[schema(example = 1)]
    pub page: u32,
    #[schema(example = 20)]
    pub page_size: u32,
    #[schema(example = 100)]
    pub total_orders: u64,
    #[schema(example = 5)]
    pub total_pages: u64,
    #[schema(example = true)]
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OrdersListResponse {
    pub orders: Vec<OrderSummary>,
    pub pagination: OrdersPagination,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OrderByTxEntry {
    #[schema(value_type = String, example = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab")]
    pub order_hash: FixedBytes<32>,
    #[schema(value_type = String, example = "0x1234567890abcdef1234567890abcdef12345678")]
    pub owner: Address,
    #[schema(value_type = String, example = "0x1234567890abcdef1234567890abcdef12345678")]
    pub orderbook_id: Address,
    pub input_token: TokenRef,
    pub output_token: TokenRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OrdersByTxResponse {
    #[schema(example = 8453)]
    pub chain_id: u32,
    #[schema(value_type = String, example = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab")]
    pub tx_hash: FixedBytes<32>,
    #[schema(example = 12345678)]
    pub block_number: u64,
    #[schema(example = 1718452800)]
    pub timestamp: u64,
    pub orders: Vec<OrderByTxEntry>,
}
