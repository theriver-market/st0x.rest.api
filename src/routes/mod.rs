pub mod admin;
pub mod attribution_admin;
pub(crate) mod batch_query;
pub mod health;
pub mod order;
pub mod orders;
pub mod owners;
pub mod prices;
pub mod registry;
pub mod swap;
pub mod tokens;
pub mod trades;
pub mod vaults;

use crate::error::ApiError;
use rain_orderbook_app_settings::token::TokenCfg;
use rain_orderbook_app_settings::yaml::{raindex::RaindexYaml, FieldErrorKind, YamlError};
use rain_orderbook_common::raindex_client::vaults::{RaindexVault, RaindexVaultType};
use rain_orderbook_common::raindex_client::RaindexClient;

pub(crate) const LEGACY_CHAIN_ID: u32 = 8453;

pub(crate) fn compatibility_chain_id(
    api_version: Option<u8>,
    requested_chain_id: Option<u32>,
) -> Option<u32> {
    if api_version == Some(1) {
        requested_chain_id.or(Some(LEGACY_CHAIN_ID))
    } else {
        requested_chain_id
    }
}

pub(crate) fn compatibility_swap_chain_id(
    api_version: Option<u8>,
    requested_chain_id: Option<u32>,
) -> Result<Option<u32>, ApiError> {
    if matches!(api_version, Some(1 | 2)) {
        match requested_chain_id {
            Some(chain_id) if chain_id != LEGACY_CHAIN_ID => Err(ApiError::BadRequest(
                "multi-network swaps require the v3 API".into(),
            )),
            _ => Ok(Some(LEGACY_CHAIN_ID)),
        }
    } else {
        requested_chain_id
            .map(Some)
            .ok_or_else(|| ApiError::BadRequest("chainId is required".into()))
    }
}

pub(crate) fn configured_raindex_chain_ids(client: &RaindexClient) -> Result<Vec<u32>, ApiError> {
    let mut chain_ids = client
        .get_all_raindexes()
        .map_err(|error| {
            tracing::error!(%error, "failed to read configured raindexes");
            ApiError::Internal("failed to read configured raindexes".into())
        })?
        .into_values()
        .map(|raindex| raindex.network.chain_id)
        .collect::<Vec<_>>();
    chain_ids.sort_unstable();
    chain_ids.dedup();
    Ok(chain_ids)
}

pub(crate) fn required_raindex_chain_ids(client: &RaindexClient) -> Result<Vec<u32>, ApiError> {
    let chain_ids = configured_raindex_chain_ids(client)?;
    if chain_ids.is_empty() {
        tracing::error!("registry has no configured raindexes");
        Err(ApiError::Internal("no configured raindexes".into()))
    } else {
        Ok(chain_ids)
    }
}

pub(crate) fn resolve_raindex_chain_ids(
    client: &RaindexClient,
    requested_chain_ids: Option<Vec<u32>>,
) -> Result<Vec<u32>, ApiError> {
    let configured_chain_ids = required_raindex_chain_ids(client)?;
    match requested_chain_ids {
        Some(chain_ids)
            if chain_ids
                .iter()
                .all(|chain_id| configured_chain_ids.contains(chain_id)) =>
        {
            Ok(chain_ids)
        }
        Some(chain_ids) => {
            tracing::warn!(
                ?chain_ids,
                "one or more chainIds have no configured raindex"
            );
            Err(ApiError::BadRequest("unsupported chainId".into()))
        }
        None => Ok(configured_chain_ids),
    }
}

pub(crate) fn raindex_backed_tokens(client: &RaindexClient) -> Result<Vec<TokenCfg>, ApiError> {
    let chain_ids = required_raindex_chain_ids(client)?;
    let tokens = client.get_all_tokens().map_err(|error| {
        tracing::error!(%error, "failed to retrieve curated tokens");
        ApiError::Internal("failed to retrieve curated tokens".into())
    })?;
    Ok(tokens
        .into_values()
        .filter(|token| chain_ids.contains(&token.network.chain_id))
        .collect())
}

pub(crate) fn validate_raindex_chain_id(
    client: &RaindexClient,
    chain_id: u32,
) -> Result<u32, ApiError> {
    if configured_raindex_chain_ids(client)?.contains(&chain_id) {
        Ok(chain_id)
    } else {
        tracing::warn!(chain_id, "chainId has no configured raindex");
        Err(ApiError::BadRequest("unsupported chainId".into()))
    }
}

pub(crate) fn resolve_required_raindex_chain_id(
    client: &RaindexClient,
    requested_chain_id: Option<u32>,
) -> Result<u32, ApiError> {
    if let Some(chain_id) = requested_chain_id {
        return validate_raindex_chain_id(client, chain_id);
    }

    let chain_ids = required_raindex_chain_ids(client)?;
    match chain_ids.as_slice() {
        [chain_id] => Ok(*chain_id),
        _ => Err(ApiError::BadRequest(
            "chainId is required when multiple networks have configured raindexes".into(),
        )),
    }
}

pub(crate) fn configured_chain_ids(raindex_yaml: &RaindexYaml) -> Result<Vec<u32>, ApiError> {
    let networks = match raindex_yaml.get_networks() {
        Ok(networks) => networks,
        Err(YamlError::Field {
            kind: FieldErrorKind::Missing(field),
            ..
        }) if field == "networks" => Default::default(),
        Err(error) => {
            tracing::error!(error = %error, "failed to read configured networks");
            return Err(ApiError::Internal(
                "failed to read configured networks".into(),
            ));
        }
    };
    let mut chain_ids = networks
        .values()
        .map(|network| network.chain_id)
        .collect::<Vec<_>>();
    chain_ids.sort_unstable();
    chain_ids.dedup();
    Ok(chain_ids)
}

pub(crate) fn validate_chain_id(
    raindex_yaml: &RaindexYaml,
    chain_id: u32,
) -> Result<u32, ApiError> {
    raindex_yaml
        .get_network_by_chain_id(chain_id)
        .map_err(|error| {
            tracing::warn!(chain_id, error = %error, "unsupported chainId");
            ApiError::BadRequest("unsupported chainId".into())
        })?;
    Ok(chain_id)
}

pub(crate) fn resolve_required_chain_id(
    raindex_yaml: &RaindexYaml,
    requested_chain_id: Option<u32>,
) -> Result<u32, ApiError> {
    if let Some(chain_id) = requested_chain_id {
        return validate_chain_id(raindex_yaml, chain_id);
    }

    let chain_ids = configured_chain_ids(raindex_yaml)?;
    match chain_ids.as_slice() {
        [chain_id] => Ok(*chain_id),
        [] => {
            tracing::error!("registry has no configured networks");
            Err(ApiError::Internal("no configured networks".into()))
        }
        _ => Err(ApiError::BadRequest(
            "chainId is required when multiple networks are configured".into(),
        )),
    }
}

pub(crate) fn optional_chain_ids_filter(
    raindex_yaml: &RaindexYaml,
    requested_chain_id: Option<u32>,
) -> Result<Option<Vec<u32>>, ApiError> {
    requested_chain_id
        .map(|chain_id| validate_chain_id(raindex_yaml, chain_id).map(|chain_id| vec![chain_id]))
        .transpose()
}

pub(crate) fn resolve_io_vaults(
    order: &rain_orderbook_common::raindex_client::orders::RaindexOrder,
) -> Result<(RaindexVault, RaindexVault), ApiError> {
    let vaults = order.vaults_list().items();
    let (mut input, mut output) = (None, None);
    for v in &vaults {
        match v.vault_type() {
            Some(RaindexVaultType::Input) if input.is_none() => input = Some(v.clone()),
            Some(RaindexVaultType::Output) if output.is_none() => output = Some(v.clone()),
            Some(RaindexVaultType::InputOutput) => {
                if input.is_none() {
                    input = Some(v.clone());
                }
                if output.is_none() {
                    output = Some(v.clone());
                }
            }
            _ => {}
        }
        if input.is_some() && output.is_some() {
            break;
        }
    }
    let input = input.ok_or_else(|| {
        tracing::error!("order has no input vaults");
        ApiError::Internal("order has no input vaults".into())
    })?;
    let output = output.ok_or_else(|| {
        tracing::error!("order has no output vaults");
        ApiError::Internal("order has no output vaults".into())
    })?;
    Ok((input, output))
}

#[cfg(test)]
mod tests;
