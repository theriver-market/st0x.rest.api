use alloy::primitives::{Address, FixedBytes};
use rocket::form::FromForm;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Clone, Default, FromForm, Serialize, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(rename_all = "camelCase")]
pub struct VaultsQueryParams {
    #[field(name = "chainId")]
    #[param(example = 8453)]
    pub chain_id: Option<u32>,
    #[field(name = "owner")]
    #[param(required = true)]
    #[param(example = "0x1234567890abcdef1234567890abcdef12345678")]
    pub owner: Option<String>,
    #[field(name = "token")]
    #[param(example = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")]
    pub token: Option<String>,
    #[field(name = "hideZeroBalance")]
    #[param(example = false)]
    pub hide_zero_balance: Option<bool>,
    #[field(name = "page")]
    #[param(example = 1)]
    pub page: Option<u16>,
    #[field(name = "pageSize")]
    #[param(example = 100)]
    pub page_size: Option<u16>,
}

#[derive(Debug, Clone, Default, FromForm, Serialize, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(rename_all = "camelCase")]
pub struct VaultTotalsQueryParams {
    #[field(name = "chainId")]
    #[param(example = 8453)]
    pub chain_id: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VaultTokenResponse {
    #[schema(value_type = String, example = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")]
    pub address: Address,
    #[schema(example = "USD Coin")]
    pub name: Option<String>,
    #[schema(example = "USDC")]
    pub symbol: Option<String>,
    #[schema(example = 6)]
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VaultTotalTokenResponse {
    #[schema(value_type = String, example = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")]
    pub address: Address,
    #[schema(example = "USDC")]
    pub symbol: Option<String>,
    #[schema(example = 6)]
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VaultOrderRef {
    #[schema(value_type = String, example = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab")]
    pub order_hash: FixedBytes<32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VaultPositionResponse {
    #[schema(example = 8453)]
    pub chain_id: u32,
    #[schema(value_type = String, example = "0xabcdef")]
    pub id: String,
    #[schema(example = "123")]
    pub vault_id: String,
    #[schema(value_type = String, example = "0x1234567890abcdef1234567890abcdef12345678")]
    pub owner: Address,
    pub token: VaultTokenResponse,
    #[schema(example = "1000000")]
    pub balance: String,
    #[schema(value_type = String, example = "0xd2938e7c9fe3597f78832ce780feb61945c377d7")]
    pub orderbook: Address,
    pub orders_as_input: Vec<VaultOrderRef>,
    pub orders_as_output: Vec<VaultOrderRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VaultsPagination {
    #[schema(example = 1)]
    pub page: u32,
    #[schema(example = 100)]
    pub page_size: u32,
    #[schema(example = 123)]
    pub total_items: u64,
    #[schema(example = true)]
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VaultsResponse {
    pub vaults: Vec<VaultPositionResponse>,
    pub pagination: VaultsPagination,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VaultTotalResponse {
    #[schema(example = 8453)]
    pub chain_id: u32,
    pub token: VaultTotalTokenResponse,
    #[schema(example = "42000000000000000000")]
    pub total_balance: String,
    #[schema(example = 42)]
    pub vault_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VaultTotalsResponse {
    pub totals: Vec<VaultTotalResponse>,
}

/// One deposit, withdrawal or fill on a vault (own index; for The River's strategy P&L).
/// Amounts and balances are raw token units (integers as strings); `amount` is signed:
/// negative = out of the vault.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VaultChangeResponse {
    /// Machine key: deposit | withdrawal | takeOrder | clear | clearBounty | unknown.
    #[schema(example = "takeOrder")]
    pub change_type: String,
    /// SDK display name (e.g. "Take order").
    #[schema(example = "Take order")]
    pub kind: String,
    #[schema(example = "-1500000")]
    pub amount: String,
    #[schema(example = "10000000")]
    pub old_balance: String,
    #[schema(example = "8500000")]
    pub new_balance: String,
    #[schema(example = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")]
    pub token: String,
    #[schema(example = 6)]
    pub decimals: u8,
    #[schema(example = 1760000000)]
    pub timestamp: u64,
    #[schema(example = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")]
    pub tx_hash: String,
    #[schema(example = "0x1234567890abcdef1234567890abcdef12345678")]
    pub sender: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VaultChangesResponse {
    #[schema(example = 8453)]
    pub chain_id: u32,
    #[schema(example = "0xabcdef")]
    pub vault: String,
    #[schema(example = 1)]
    pub page: u16,
    /// Newest first.
    pub changes: Vec<VaultChangeResponse>,
}

/// One vault of an owner with all its balance changes.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OwnerVaultChangesEntry {
    /// Vault id as returned by /v2/vaults (`id`, hex); the per-vault changes route key.
    #[schema(
        example = "0xe522cb4a5fcb2eb31a52ff41a4653d85a4fd7c9d1111111111111111111111111111111111111111833589fcd6edb6e08f4c7c32d4f71b54bda02913ab0f000000000000000000000000000000000000000000000000000000000000"
    )]
    pub id: String,
    /// On-chain vault id (decimal), as /v2/vaults `vaultId`.
    #[schema(example = "4011")]
    pub vault_id: String,
    #[schema(example = "0xe522cb4a5fcb2eb31a52ff41a4653d85a4fd7c9d")]
    pub orderbook: String,
    #[schema(example = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")]
    pub token: String,
    #[schema(example = 6)]
    pub decimals: u8,
    /// Newest first; same rows as /v2/vaults/{chain_id}/{raindex}/{id}/changes.
    pub changes: Vec<VaultChangeResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OwnerVaultChangesResponse {
    #[schema(example = 8453)]
    pub chain_id: u32,
    #[schema(example = "0x1111111111111111111111111111111111111111")]
    pub owner: String,
    pub vaults: Vec<OwnerVaultChangesEntry>,
}
