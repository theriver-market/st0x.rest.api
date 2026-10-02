#[macro_use]
extern crate rocket;

mod analytics;
mod app_state;
mod atomic_liquidity;
mod attribution;
mod attribution_reporting;
mod auth;
mod cache;
mod catchers;
mod cli;
mod config;
mod db;
mod denomination;
mod erc4626;
mod error;
mod fairings;
mod market_price;
mod metrics;
mod raindex;
mod registry_artifact;
mod river_takes;
mod routes;
mod swap_capacity;
mod telemetry;
mod types;
mod wrap_ratio;

#[cfg(test)]
mod test_helpers;

use clap::Parser;
use rocket::fs::{FileServer, Options};
use rocket_cors::{AllowedHeaders, AllowedMethods, AllowedOrigins, CorsOptions};
use std::collections::HashSet;
use std::path::PathBuf;
use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};
use utoipa::openapi::Ref;
use utoipa::{Modify, OpenApi};
use utoipa_swagger_ui::SwaggerUi;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            let mut scheme = Http::new(HttpAuthScheme::Basic);
            scheme.description = Some(
                "Use your API key as the username and API secret as the password.".to_string(),
            );
            components.add_security_scheme("basicAuth", SecurityScheme::Http(scheme));
        }
    }
}

struct V1CompatibilityAddon;
struct V3SwapAddon;

const V1_COMPATIBILITY_PATHS: [(&str, &str); 21] = [
    ("/v2/tokens", "/v1/tokens"),
    ("/v2/tokens/wrap-ratio", "/v1/tokens/wrap-ratio"),
    (
        "/v2/tokens/wrap-ratio/{address}",
        "/v1/tokens/wrap-ratio/{address}",
    ),
    (
        "/v2/tokens/wrap-ratio/{address}/history",
        "/v1/tokens/wrap-ratio/{address}/history",
    ),
    ("/v2/tokens/details", "/v1/tokens/details"),
    (
        "/v2/tokens/{address}/details",
        "/v1/tokens/{address}/details",
    ),
    ("/v2/tokens/{address}/proofs", "/v1/tokens/{address}/proofs"),
    ("/v2/prices", "/v1/prices"),
    (
        "/v2/prices/{address}/history",
        "/v1/prices/{address}/history",
    ),
    ("/v2/order/{order_hash}", "/v1/order/{order_hash}"),
    ("/v2/order/cancel", "/v1/order/cancel"),
    ("/v2/orders/owner/{address}", "/v1/orders/owner/{address}"),
    ("/v2/orders/token/{address}", "/v1/orders/token/{address}"),
    ("/v2/orders/query", "/v1/orders/query"),
    ("/v2/vaults", "/v1/vaults"),
    ("/v2/vaults/totals", "/v1/vaults/totals"),
    ("/v2/trades/tx/{tx_hash}", "/v1/trades/tx/{tx_hash}"),
    ("/v2/trades/token/{address}", "/v1/trades/token/{address}"),
    ("/v2/trades/taker/{address}", "/v1/trades/taker/{address}"),
    ("/v2/trades/{address}", "/v1/trades/{address}"),
    ("/v2/trades/query", "/v1/trades/query"),
];

impl Modify for V1CompatibilityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        for (v2_path, v1_path) in V1_COMPATIBILITY_PATHS {
            let Some(mut path_item) = openapi.paths.paths.get(v2_path).cloned() else {
                tracing::error!(
                    v2_path,
                    v1_path,
                    "OpenAPI V1 compatibility source path missing"
                );
                continue;
            };

            for operation in [
                &mut path_item.get,
                &mut path_item.put,
                &mut path_item.post,
                &mut path_item.delete,
                &mut path_item.options,
                &mut path_item.head,
                &mut path_item.patch,
                &mut path_item.trace,
            ]
            .into_iter()
            .flatten()
            {
                operation.operation_id = operation
                    .operation_id
                    .as_ref()
                    .map(|operation_id| format!("{operation_id}_v1"));
            }

            openapi.paths.paths.insert(v1_path.to_string(), path_item);
        }
    }
}

impl Modify for V3SwapAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        for (v2_path, v3_path, request_schema) in [
            ("/v2/swap/quote", "/v3/swap/quote", "SwapQuoteV3RequestBody"),
            (
                "/v2/swap/calldata",
                "/v3/swap/calldata",
                "SwapCalldataV3RequestBody",
            ),
        ] {
            let Some(mut path_item) = openapi.paths.paths.get(v2_path).cloned() else {
                tracing::error!(v2_path, v3_path, "OpenAPI V3 swap source path missing");
                continue;
            };
            if let Some(operation) = path_item.post.as_mut() {
                operation.operation_id = operation
                    .operation_id
                    .as_ref()
                    .map(|operation_id| format!("{operation_id}_v3"));
                let compatibility_note =
                    "chainId is required. Requests without it are rejected; no network is inferred.";
                operation.description = Some(match operation.description.take() {
                    Some(description) => format!("{description}\n\n{compatibility_note}"),
                    None => compatibility_note.to_string(),
                });
                if let Some(content) = operation
                    .request_body
                    .as_mut()
                    .and_then(|body| body.content.get_mut("application/json"))
                {
                    content.schema = Some(Ref::from_schema_name(request_schema).into());
                }
            }
            openapi.paths.paths.insert(v3_path.to_string(), path_item);
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum StartupError {
    #[error("invalid HTTP method in CORS config: {0}")]
    InvalidMethod(String),
    #[error("CORS configuration failed: {0}")]
    Cors(#[from] rocket_cors::Error),
}

const ATTRIBUTION_SIGNER_CREDENTIAL: &str = "attribution-signer";

fn load_attribution_signer_key() -> Result<String, String> {
    if let Ok(key) = std::env::var("ST0X_GATING_SIGNER_KEY") {
        let key = key.trim();
        if !key.is_empty() {
            return Ok(key.to_string());
        }
    }

    let credentials_directory = std::env::var("CREDENTIALS_DIRECTORY").map_err(|_| {
        "ST0X_GATING_SIGNER_KEY or a systemd attribution-signer credential is required".to_string()
    })?;
    let credential_path =
        std::path::Path::new(&credentials_directory).join(ATTRIBUTION_SIGNER_CREDENTIAL);
    let key = std::fs::read_to_string(&credential_path).map_err(|error| {
        format!(
            "failed to read attribution signer credential {}: {error}",
            credential_path.display()
        )
    })?;
    let key = key.trim();
    if key.is_empty() {
        return Err("attribution signer credential is empty".to_string());
    }
    Ok(key.to_string())
}

#[derive(Debug, thiserror::Error)]
enum StartupRegistryError {
    #[error("failed to read private registry artifact")]
    PrivateArtifactRead(#[source] registry_artifact::RegistryArtifactStoreError),
    #[error("failed to query private registry history")]
    PrivateRegistryHistory(#[source] sqlx::Error),
    #[error("configured registry_url is empty")]
    MissingConfiguredRegistry,
    #[error("private registry artifact does not match latest successful history")]
    PrivateRegistryMismatch,
    #[error("private registry artifact exists but no successful history row was found")]
    PrivateArtifactWithoutHistory,
    #[error("private registry history exists but artifact file is missing")]
    HistoryWithoutPrivateArtifact,
    #[error("failed to load private registry")]
    PrivateRegistryLoad(#[source] raindex::RaindexProviderError),
    #[error("failed to load configured registry")]
    ConfiguredRegistryLoad(#[source] raindex::RaindexProviderError),
}

#[derive(OpenApi)]
#[openapi(
    paths(
        routes::health::get_health,
        routes::health::get_health_detailed,
        routes::tokens::get_tokens,
        routes::tokens::get_wrap_ratios,
        routes::tokens::get_wrap_ratio_by_address,
        routes::tokens::get_wrap_ratio_history_by_address,
        routes::tokens::get_token_details,
        routes::tokens::get_token_details_by_address,
        routes::tokens::get_token_proofs,
        routes::prices::get_prices,
        routes::prices::get_price_history,
        routes::swap::post_swap_quote,
        routes::swap::post_swap_quote_v2,
        routes::swap::post_swap_calldata,
        routes::swap::post_swap_calldata_v2,
        routes::order::post_order_dca,
        routes::order::post_order_solver,
        routes::order::get_order,
        routes::order::post_order_cancel,
        routes::orders::get_orders_by_tx,
        routes::orders::get_orders_by_address,
        routes::orders::get_orders_by_token,
        routes::orders::post_orders_query,
        routes::vaults::get_vaults,
        routes::vaults::get_vault_totals,
        routes::admin::put_registry,
        routes::attribution_admin::get_attributed_executions,
        routes::attribution_admin::get_attribution_volume,
        routes::trades::get_by_tx::get_trades_by_tx,
        routes::trades::query::post_trades_query,
        routes::trades::get_by_token::get_trades_by_token,
        routes::trades::get_by_taker::get_trades_by_taker,
        routes::trades::get_by_address::get_trades_by_address,
        routes::registry::get_registry,
        routes::registry::get_registry_history,
    ),
    components(schemas(
        types::swap::SwapQuoteV3RequestBody,
        types::swap::SwapCalldataV3RequestBody,
    )),
    modifiers(&SecurityAddon, &V1CompatibilityAddon, &V3SwapAddon),
    tags(
        (name = "Health", description = "Health check endpoints"),
        (name = "Tokens", description = "Token information endpoints"),
        (name = "Prices", description = "ST0x market price endpoints"),
        (name = "Swap", description = "Swap quote and calldata endpoints"),
        (name = "Order", description = "Order deployment and management endpoints"),
        (name = "Orders", description = "Order listing and query endpoints"),
        (name = "Vaults", description = "Orderbook vault position and total endpoints"),
        (name = "Admin", description = "Administrative endpoints"),
        (name = "Trades", description = "Trade listing and query endpoints"),
        (name = "Registry", description = "Registry information endpoints"),
    ),
    info(
        title = "st0x REST API",
        version = "0.1.0",
        description = "REST API for st0x orderbook operations",
    )
)]
struct ApiDoc;

fn configure_cors() -> Result<rocket_cors::Cors, StartupError> {
    let allowed_methods: AllowedMethods = ["Get", "Post", "Put", "Options"]
        .iter()
        .map(|s| {
            std::str::FromStr::from_str(s).map_err(|_| StartupError::InvalidMethod(s.to_string()))
        })
        .collect::<Result<_, _>>()?;

    Ok(CorsOptions {
        allowed_origins: AllowedOrigins::all(),
        allowed_methods,
        allowed_headers: AllowedHeaders::all(),
        allow_credentials: false,
        expose_headers: HashSet::from([
            "X-Request-Id".to_string(),
            "Retry-After".to_string(),
            "X-RateLimit-Limit".to_string(),
            "X-RateLimit-Remaining".to_string(),
            "X-RateLimit-Reset".to_string(),
        ]),
        ..Default::default()
    }
    .to_cors()?)
}

pub(crate) struct RocketDependencies {
    pool: db::DbPool,
    rate_limiter: fairings::RateLimiter,
    raindex_config: raindex::SharedRaindexProvider,
    app_state: app_state::ApplicationState,
    analytics: analytics::Analytics,
    market_price_state: market_price::MarketPriceState,
    swap_capacity: swap_capacity::SwapCapacity,
}

pub(crate) fn rocket(
    dependencies: RocketDependencies,
    docs_dir: String,
    usage_log_max_concurrency: usize,
) -> Result<rocket::Rocket<rocket::Build>, StartupError> {
    let RocketDependencies {
        pool,
        rate_limiter,
        raindex_config,
        app_state,
        analytics,
        market_price_state,
        swap_capacity,
    } = dependencies;
    let cors = configure_cors()?;

    let figment = rocket::Config::figment().merge((rocket::Config::LOG_LEVEL, "normal"));

    let options = Options::Index | Options::NormalizeDirs;

    Ok(rocket::custom(figment)
        .manage(pool)
        .manage(rate_limiter)
        .manage(raindex_config)
        .manage(app_state)
        .manage(analytics)
        .manage(market_price_state)
        .manage(swap_capacity)
        .mount("/", routes::health::routes())
        .mount("/v1/tokens", routes::tokens::routes())
        .mount("/v2/tokens", routes::tokens::routes_v2())
        .mount("/v1/prices", routes::prices::routes())
        .mount("/v2/prices", routes::prices::routes_v2())
        .mount("/v1/swap", routes::swap::routes())
        .mount("/v2/swap", routes::swap::routes_v2())
        .mount("/v3/swap", routes::swap::routes_v3())
        .mount("/v1/order", routes::order::routes())
        .mount("/v2/order", routes::order::routes_v2())
        .mount("/v1/orders", routes::orders::routes())
        .mount("/v2/orders", routes::orders::routes_v2())
        .mount("/v1/vaults", routes::vaults::routes())
        .mount("/v2/vaults", routes::vaults::routes_v2())
        .mount("/v1/trades", routes::trades::routes())
        .mount("/v2/trades", routes::trades::routes_v2())
        .mount("/", routes::registry::routes())
        .mount("/admin", routes::admin::routes())
        .mount("/admin/attribution", routes::attribution_admin::routes())
        .mount("/docs", FileServer::new(docs_dir, options))
        .mount(
            "/",
            SwaggerUi::new("/swagger/<tail..>").url("/api-doc/openapi.json", ApiDoc::openapi()),
        )
        .register("/", catchers::catchers())
        .attach(fairings::RequestLogger)
        .attach(fairings::UsageLogger::new(usage_log_max_concurrency))
        .attach(fairings::AnalyticsFairing)
        .attach(fairings::RateLimitHeadersFairing)
        .attach(cors))
}

async fn load_configured_raindex(
    cfg: &config::Config,
    local_db_path: PathBuf,
) -> Result<raindex::RaindexProvider, StartupRegistryError> {
    if cfg.registry_url.is_empty() {
        return Err(StartupRegistryError::MissingConfiguredRegistry);
    }

    tracing::info!("loading raindex registry from config");
    raindex::RaindexProvider::load(&cfg.registry_url, Some(local_db_path))
        .await
        .map_err(StartupRegistryError::ConfiguredRegistryLoad)
}

async fn load_startup_raindex(
    cfg: &config::Config,
    pool: &db::DbPool,
    registry_artifact_store: &registry_artifact::RegistryArtifactStore,
    local_db_path: PathBuf,
) -> Result<raindex::RaindexProvider, StartupRegistryError> {
    let private_registry_artifact = registry_artifact_store
        .load()
        .await
        .map_err(StartupRegistryError::PrivateArtifactRead)?;

    let latest_private_registry = db::registry_history::latest_successful_private_registry(pool)
        .await
        .map_err(StartupRegistryError::PrivateRegistryHistory)?;

    let private_registry_source = match (
        private_registry_artifact.filter(|artifact| !artifact.is_empty()),
        latest_private_registry,
    ) {
        (Some(artifact), Some(row)) => {
            let payload_sha256 = registry_artifact::artifact_sha256(&artifact);
            if row.payload_sha256 != payload_sha256 {
                tracing::error!(
                    expected_payload_sha256 = %row.payload_sha256,
                    actual_payload_sha256 = %payload_sha256,
                    path = %registry_artifact_store.path().display(),
                    "private registry artifact does not match latest successful history"
                );
                if cfg.allow_registry_fallback {
                    None
                } else {
                    return Err(StartupRegistryError::PrivateRegistryMismatch);
                }
            } else {
                Some(artifact)
            }
        }
        (Some(artifact), None) => {
            let payload_sha256 = registry_artifact::artifact_sha256(&artifact);
            tracing::error!(
                payload_sha256 = %payload_sha256,
                path = %registry_artifact_store.path().display(),
                "private registry artifact exists but no successful history row was found"
            );
            if cfg.allow_registry_fallback {
                None
            } else {
                return Err(StartupRegistryError::PrivateArtifactWithoutHistory);
            }
        }
        (None, Some(_)) => {
            tracing::error!(
                path = %registry_artifact_store.path().display(),
                "private registry history exists but artifact file is missing"
            );
            if cfg.allow_registry_fallback {
                None
            } else {
                return Err(StartupRegistryError::HistoryWithoutPrivateArtifact);
            }
        }
        (None, None) => None,
    };

    if let Some(private_registry_source) = private_registry_source {
        tracing::info!(
            path = %registry_artifact_store.path().display(),
            "loading private registry artifact from file"
        );
        match raindex::RaindexProvider::load(&private_registry_source, Some(local_db_path.clone()))
            .await
        {
            Ok(provider) => {
                tracing::info!("loaded private raindex registry");
                return Ok(provider);
            }
            Err(e) if cfg.allow_registry_fallback => {
                tracing::error!(
                    error = %e.safe_summary(),
                    path = %registry_artifact_store.path().display(),
                    "failed to load private registry artifact; falling back to configured registry"
                );
            }
            Err(e) => return Err(StartupRegistryError::PrivateRegistryLoad(e)),
        }
    }

    load_configured_raindex(cfg, local_db_path).await
}

#[rocket::main]
async fn main() {
    let parsed = cli::Cli::parse();

    let command = match parsed.command {
        Some(cmd) => cmd,
        None => {
            cli::print_usage();
            return;
        }
    };

    let config_path = match &command {
        cli::Command::Serve { config } | cli::Command::Keys { config, .. } => config.clone(),
    };

    let cfg = match config::Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to load config from {}: {e}", config_path.display());
            std::process::exit(1);
        }
    };

    let log_guard = match telemetry::init(&cfg.log_dir, cfg.telemetry.as_ref()) {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("failed to initialize telemetry: {e}");
            std::process::exit(1);
        }
    };

    // Install the Prometheus recorder + standalone /metrics listener (:8001),
    // reachable only over the tailnet. Non-fatal: a bind failure logs and the
    // API keeps serving.
    if let Err(e) = metrics::install() {
        tracing::warn!(error = %e, "metrics exporter disabled");
    }

    let pool = match db::init(&cfg.database_url, cfg.database_max_connections).await {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(error = %e, "failed to initialize database");
            drop(log_guard);
            std::process::exit(1);
        }
    };
    let response_cache_max_trade_rows = cfg
        .response_cache_max_trade_rows
        .unwrap_or(cfg.response_cache_max_entries);

    tracing::info!(
        global_rpm = cfg.rate_limit_global_rpm,
        per_key_rpm = cfg.rate_limit_per_key_rpm,
        swap_max_concurrent_global = cfg.swap_max_concurrent_global,
        swap_max_concurrent_per_key = cfg.swap_max_concurrent_per_key,
        swap_request_timeout_seconds = cfg.swap_request_timeout_seconds,
        database_max_connections = cfg.database_max_connections,
        usage_log_max_concurrency = cfg.usage_log_max_concurrency,
        response_cache_max_entries = cfg.response_cache_max_entries,
        response_cache_max_trade_rows,
        response_cache_ttl_seconds = cfg.response_cache_ttl_seconds,
        "runtime limits configured"
    );

    match command {
        cli::Command::Serve { .. } => {
            if cfg.response_cache_max_entries > 0
                && cfg.response_cache_ttl_seconds > 0
                && response_cache_max_trade_rows == 0
            {
                tracing::warn!(
                    "batch trades response cache disabled because response_cache_max_trade_rows is zero"
                );
            }
            let registry_artifact_store = registry_artifact::RegistryArtifactStore::new(
                std::path::PathBuf::from(&cfg.private_registry_path),
            );
            let response_caches = cache::RouteResponseCaches::new_with_trade_weight(
                cfg.response_cache_max_entries,
                response_cache_max_trade_rows,
                std::time::Duration::from_secs(cfg.response_cache_ttl_seconds),
            );

            let local_db_path = std::path::PathBuf::from(&cfg.local_db_path);
            if let Some(parent) = local_db_path
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
            {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    tracing::error!(error = %e, path = %parent.display(), "failed to create local db directory");
                    drop(log_guard);
                    std::process::exit(1);
                }
            }

            let raindex_config =
                match load_startup_raindex(&cfg, &pool, &registry_artifact_store, local_db_path)
                    .await
                {
                    Ok(config) => {
                        tracing::info!("raindex registry loaded");
                        config
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to load raindex registry");
                        drop(log_guard);
                        std::process::exit(1);
                    }
                };

            let attribution_chain_ids =
                match routes::configured_chain_ids(raindex_config.raindex_yaml()) {
                    Ok(chain_ids) if !chain_ids.is_empty() => chain_ids,
                    Ok(_) => {
                        tracing::error!("registry has no configured networks");
                        drop(log_guard);
                        std::process::exit(1);
                    }
                    Err(error) => {
                        tracing::error!(%error, "failed to resolve configured networks");
                        drop(log_guard);
                        std::process::exit(1);
                    }
                };

            let shared_raindex = std::sync::Arc::new(tokio::sync::RwLock::new(raindex_config));
            {
                let rpcs = shared_raindex
                    .read()
                    .await
                    .raindex_yaml()
                    .get_network_by_chain_id(river_takes::RIVER_TAKER_CHAIN_ID)
                    .map(|network| network.rpcs)
                    .unwrap_or_default();
                tokio::spawn(river_takes::supervise(pool.clone(), rpcs));
            }
            let rate_limiter =
                fairings::RateLimiter::new(cfg.rate_limit_global_rpm, cfg.rate_limit_per_key_rpm);
            let swap_capacity = swap_capacity::SwapCapacity::new(
                cfg.swap_max_concurrent_global,
                cfg.swap_max_concurrent_per_key,
                std::time::Duration::from_secs(cfg.swap_request_timeout_seconds),
            );
            let market_price_config = match market_price::MarketPriceConfig::try_from(&cfg) {
                Ok(config) => config,
                Err(error) => {
                    tracing::error!(%error, "invalid market price configuration");
                    drop(log_guard);
                    std::process::exit(1);
                }
            };
            let market_price_state = market_price::MarketPriceState::new(
                pool.clone(),
                shared_raindex.clone(),
                market_price_config.clone(),
            );

            if !std::path::Path::new(&cfg.docs_dir).is_dir() {
                tracing::error!(docs_dir = %cfg.docs_dir, "docs_dir is not a valid directory");
                drop(log_guard);
                std::process::exit(1);
            }
            tracing::info!(docs_dir = %cfg.docs_dir, "serving documentation at /docs");

            let attribution_key = match load_attribution_signer_key() {
                Ok(key) => key,
                Err(error) => {
                    tracing::error!(%error, "failed to load attribution signer key");
                    drop(log_guard);
                    std::process::exit(1);
                }
            };
            let attribution_signer =
                match attribution::AttributionSigner::from_hex_key(&attribution_key) {
                    Ok(signer) => signer,
                    Err(error) => {
                        tracing::error!(%error, "failed to initialize attribution signer");
                        drop(log_guard);
                        std::process::exit(1);
                    }
                };
            tracing::info!(
                signer = %attribution_signer.address(),
                "attribution signer loaded"
            );
            let attribution_signer_address = attribution_signer.address();
            let attribution_state = attribution::AttributionState::new(attribution_signer);
            let app_state = app_state::ApplicationState::new(
                registry_artifact_store,
                response_caches,
                attribution_state,
            );

            let analytics = analytics::Analytics::from_env();

            if market_price_config.enabled {
                tokio::spawn(market_price::supervise_market_price_sampler(
                    market_price_state.clone(),
                ));
                tracing::info!(
                    interval_seconds = market_price_config.sample_interval.as_secs(),
                    retention_seconds = market_price_config.retention.as_secs(),
                    "registry-driven market price sampler started"
                );
            } else {
                tracing::warn!("market price sampler is disabled");
            }

            let attribution_pool = pool.clone();
            let mut rocket = match rocket(
                RocketDependencies {
                    pool,
                    rate_limiter,
                    raindex_config: shared_raindex,
                    app_state,
                    analytics,
                    market_price_state,
                    swap_capacity,
                },
                cfg.docs_dir,
                cfg.usage_log_max_concurrency,
            ) {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(error = %e, "failed to build Rocket instance");
                    drop(log_guard);
                    std::process::exit(1);
                }
            };

            if let Some(start_block) = cfg.attribution_start_block {
                tracing::info!(
                    start_block,
                    interval_seconds = cfg.attribution_sync_interval_seconds,
                    batch_size = cfg.attribution_sync_batch_size,
                    "confirmed trade attribution worker enabled"
                );
                rocket = rocket.attach(attribution_reporting::AttributionWorker::new(
                    attribution_pool,
                    std::path::PathBuf::from(&cfg.local_db_path),
                    attribution_chain_ids,
                    attribution_signer_address,
                    start_block,
                    std::time::Duration::from_secs(cfg.attribution_sync_interval_seconds.max(1)),
                    cfg.attribution_sync_batch_size,
                ));
            } else {
                tracing::info!("confirmed trade attribution worker disabled");
            }

            if let Err(e) = rocket.launch().await {
                tracing::error!(error = %e, "Rocket launch failed");
                drop(log_guard);
                std::process::exit(1);
            }
        }
        cli::Command::Keys { command, .. } => {
            if let Err(e) =
                cli::handle_keys_command(command, pool, cfg.swap_max_concurrent_global).await
            {
                tracing::error!(error = %e, "keys command failed");
                drop(log_guard);
                std::process::exit(1);
            }
        }
    }

    drop(log_guard);
}

#[cfg(test)]
mod tests {
    use crate::test_helpers::{basic_auth_header, client, mock_raindex_registry_url, seed_api_key};
    use rocket::http::{Header, Status};
    use utoipa::OpenApi;

    #[rocket::async_test]
    async fn test_health_endpoint() {
        let client = client().await;
        let response = client.get("/health").dispatch().await;
        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        assert_eq!(body["status"], "ok");
    }

    #[test]
    fn test_v2_routes_exclude_unimplemented_v1_handlers() {
        let order_routes = crate::routes::order::routes_v2();
        assert!(order_routes
            .iter()
            .all(|route| route.uri.path() != "/dca" && route.uri.path() != "/solver"));

        let orders_routes = crate::routes::orders::routes_v2();
        assert!(orders_routes
            .iter()
            .all(|route| route.uri.path() != "/tx/<tx_hash>"));
    }

    #[test]
    fn test_openapi_includes_token_proofs_schema() {
        let openapi = serde_json::to_value(super::ApiDoc::openapi()).expect("serialize openapi");
        let swap_quote_v1_path = &openapi["paths"]["/v1/swap/quote"]["post"];
        let proofs_path = &openapi["paths"]["/v2/tokens/{address}/proofs"]["get"];
        let swap_quote_v2_path = &openapi["paths"]["/v2/swap/quote"]["post"];
        let swap_calldata_v2_path = &openapi["paths"]["/v2/swap/calldata"]["post"];
        let orders_query_path = &openapi["paths"]["/v2/orders/query"]["post"];
        let trades_query_path = &openapi["paths"]["/v2/trades/query"]["post"];

        assert_eq!(proofs_path["tags"][0], "Tokens");
        assert_eq!(
            proofs_path["parameters"][0]["description"],
            "Wrapped, unwrapped, or legacy ST0x token address"
        );
        assert_eq!(
            proofs_path["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/TokenProofsResponse"
        );
        assert_eq!(
            proofs_path["responses"]["404"]["description"],
            "Wrapped ST0x token or SFT vault not found"
        );

        let schemas = &openapi["components"]["schemas"];
        assert!(schemas["TokenProofsResponse"]["properties"]["metadata"].is_object());
        assert!(schemas["TokenProofMetadata"]["properties"]["metaHash"].is_object());
        assert!(schemas["TokenProofReceipt"]["properties"]["receiptId"].is_object());
        assert!(schemas["TokenProofReceipt"]["properties"]["txHash"].is_object());
        assert!(schemas["TokenProofReceipt"]["properties"]["type"].is_object());
        assert_eq!(swap_quote_v2_path["tags"][0], "Swap");
        assert_eq!(
            swap_quote_v1_path["responses"]["503"]["description"],
            "Required swap oracle unavailable"
        );
        assert_eq!(
            swap_quote_v2_path["responses"]["503"]["description"],
            "Required swap oracle unavailable"
        );
        assert_eq!(
            swap_quote_v2_path["responses"]["504"]["description"],
            "Swap request timed out"
        );
        assert_eq!(
            swap_quote_v2_path["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/SwapQuoteV2RequestBody"
        );
        assert_eq!(
            schemas["SwapQuoteV2RequestBody"]["oneOf"],
            serde_json::json!([
                { "$ref": "#/components/schemas/SwapQuoteV2PriceCapRequest" },
                { "$ref": "#/components/schemas/SwapQuoteV2SlippageRequest" }
            ])
        );
        assert!(
            schemas["SwapQuoteV2SlippageRequest"]["allOf"][1]["properties"]["slippageBps"]
                ["description"]
                .as_str()
                .is_some_and(|description| description.contains("1 BPS = 0.01%"))
        );
        assert!(
            schemas["SwapQuoteV2SlippageRequest"]["allOf"][1]["properties"]["referenceIoRatio"]
                ["description"]
                .as_str()
                .is_some_and(|description| description.contains("input-token-per-output-token"))
        );
        assert_eq!(swap_calldata_v2_path["tags"][0], "Swap");
        assert_eq!(
            swap_calldata_v2_path["responses"]["504"]["description"],
            "Swap request timed out"
        );
        assert_eq!(
            swap_calldata_v2_path["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/SwapCalldataV2RequestBody"
        );
        assert_eq!(
            schemas["SwapCalldataMode"]["enum"],
            serde_json::json!(["buyUpTo", "spendExact", "spendUpTo"])
        );
        assert_eq!(
            swap_calldata_v2_path["responses"]["200"]["content"]["application/json"]["schema"]
                ["$ref"],
            "#/components/schemas/SwapCalldataV2Response"
        );
        assert_eq!(
            schemas["SwapCalldataV2RequestBody"]["oneOf"],
            serde_json::json!([
                { "$ref": "#/components/schemas/SwapCalldataV2PriceCapRequest" },
                { "$ref": "#/components/schemas/SwapCalldataV2SlippageRequest" }
            ])
        );
        assert!(
            schemas["SwapCalldataV2PriceCapRequest"]["allOf"][1]["required"]
                .as_array()
                .is_some_and(|required| required.contains(&serde_json::json!("priceCap")))
        );
        assert!(
            schemas["SwapCalldataV2SlippageRequest"]["allOf"][1]["required"]
                .as_array()
                .is_some_and(|required| required.contains(&serde_json::json!("slippageBps")))
        );
        assert!(
            schemas["SwapCalldataV2SlippageRequest"]["allOf"][1]["properties"]["referenceIoRatio"]
                .is_object()
        );
        assert!(
            schemas["SwapCalldataV2SlippageRequest"]["allOf"][1]["properties"]["slippageBps"]
                ["description"]
                .as_str()
                .is_some_and(|description| description.contains("1 BPS = 0.01%"))
        );
        assert_eq!(orders_query_path["tags"][0], "Orders");
        assert_eq!(
            orders_query_path["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/OrdersQueryRequest"
        );
        assert_eq!(trades_query_path["tags"][0], "Trades");
        assert_eq!(
            trades_query_path["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/TradesQueryRequest"
        );
        assert_eq!(
            schemas["TradesQueryResponse"]["oneOf"],
            serde_json::json!([
                { "$ref": "#/components/schemas/TradesByOrderHashesResponse" },
                { "$ref": "#/components/schemas/TradesByAddressResponse" }
            ])
        );
        assert!(
            schemas["SwapCalldataV2SlippageRequest"]["allOf"][1]["properties"]["referenceIoRatio"]
                ["description"]
                .as_str()
                .is_some_and(|description| description.contains("input-token-per-output-token"))
        );
        assert!(
            schemas["SwapCalldataV2Response"]["allOf"][1]["properties"]["resolvedPriceCap"]
                .is_object()
        );
    }

    #[test]
    fn test_openapi_includes_v1_compatibility_paths() {
        let openapi = serde_json::to_value(super::ApiDoc::openapi()).expect("serialize openapi");

        for (v2_path, v1_path) in super::V1_COMPATIBILITY_PATHS {
            assert!(
                openapi["paths"][v2_path].is_object(),
                "missing V2 compatibility source path {v2_path}"
            );
            for method in ["get", "post", "put", "delete", "patch"] {
                let Some(v2_operation) = openapi["paths"][v2_path][method].as_object() else {
                    continue;
                };
                let v1_operation = openapi["paths"][v1_path][method]
                    .as_object()
                    .unwrap_or_else(|| panic!("missing {method} operation for {v1_path}"));

                let mut expected_v1 = v2_operation.clone();
                if let Some(operation_id) = v2_operation
                    .get("operationId")
                    .and_then(serde_json::Value::as_str)
                {
                    expected_v1.insert(
                        "operationId".to_string(),
                        serde_json::Value::String(format!("{operation_id}_v1")),
                    );
                }
                assert_eq!(*v1_operation, expected_v1, "OpenAPI mismatch for {v1_path}");
            }
        }

        for (v1_path, v2_path) in [
            ("/v1/order/dca", "/v2/order/dca"),
            ("/v1/order/solver", "/v2/order/solver"),
            ("/v1/orders/tx/{tx_hash}", "/v2/orders/tx/{tx_hash}"),
        ] {
            assert!(openapi["paths"][v1_path].is_object());
            assert!(openapi["paths"][v2_path].is_null());
        }
    }

    #[test]
    fn test_openapi_includes_strict_v3_swap_paths() {
        let openapi = serde_json::to_value(super::ApiDoc::openapi()).expect("serialize openapi");

        for (path, schema_name) in [
            ("/v3/swap/quote", "SwapQuoteV3RequestBody"),
            ("/v3/swap/calldata", "SwapCalldataV3RequestBody"),
        ] {
            let operation = &openapi["paths"][path]["post"];
            assert!(operation.is_object(), "missing V3 swap path {path}");
            assert!(operation["description"]
                .as_str()
                .is_some_and(|description| description.contains("chainId is required")));
            assert!(operation["operationId"]
                .as_str()
                .is_some_and(|operation_id| operation_id.ends_with("_v3")));
            assert_eq!(
                operation["requestBody"]["content"]["application/json"]["schema"]["$ref"],
                format!("#/components/schemas/{schema_name}")
            );

            let required = openapi["components"]["schemas"][schema_name]["allOf"][1]["required"]
                .as_array()
                .expect("V3 request schema has required fields");
            assert!(required.iter().any(|field| field == "chainId"));
        }
    }

    #[test]
    fn test_openapi_documents_multichain_list_validation_errors() {
        let openapi = serde_json::to_value(super::ApiDoc::openapi()).expect("serialize openapi");

        assert_eq!(
            openapi["paths"]["/v2/tokens/wrap-ratio"]["get"]["responses"]["400"]["description"],
            "Unsupported chainId or invalid date"
        );
        assert!(
            openapi["paths"]["/v2/tokens/wrap-ratio"]["get"]["parameters"]
                .as_array()
                .expect("wrap ratio parameters are an array")
                .iter()
                .any(|parameter| parameter["name"] == "date")
        );

        for path in ["/v2/tokens/details", "/v2/vaults/totals"] {
            assert_eq!(
                openapi["paths"][path]["get"]["responses"]["400"]["description"],
                "Unsupported chainId"
            );
        }
    }

    #[test]
    fn test_openapi_documents_token_details_activity_limit() {
        let openapi = serde_json::to_value(super::ApiDoc::openapi()).expect("serialize openapi");
        let details_path = &openapi["paths"]["/v2/tokens/{address}/details"]["get"];
        let parameters = details_path["parameters"]
            .as_array()
            .expect("parameters is an array");

        assert!(parameters
            .iter()
            .any(|parameter| parameter["name"] == "activityLimit"));
        assert!(parameters
            .iter()
            .any(|parameter| parameter["name"] == "chainId"));
        assert!(!parameters
            .iter()
            .any(|parameter| parameter["name"] == "activity_limit"));
    }

    #[test]
    fn test_openapi_documents_attribution_query_names() {
        let openapi = serde_json::to_value(super::ApiDoc::openapi()).expect("serialize openapi");
        let execution_parameters = openapi["paths"]["/admin/attribution/executions"]["get"]
            ["parameters"]
            .as_array()
            .expect("parameters is an array");
        let execution_names: Vec<&str> = execution_parameters
            .iter()
            .filter_map(|parameter| parameter["name"].as_str())
            .collect();
        assert!(execution_names.contains(&"apiKeyHash"));
        assert!(execution_names.contains(&"transactionHash"));
        assert!(execution_names.contains(&"beforeBlock"));
        assert!(execution_names.contains(&"beforeLogIndex"));
        assert!(execution_names.contains(&"beforeTradeId"));
        assert!(!execution_names.contains(&"api_key_hash"));

        let volume_parameters = openapi["paths"]["/admin/attribution/volume"]["get"]["parameters"]
            .as_array()
            .expect("parameters is an array");
        let volume_names: Vec<&str> = volume_parameters
            .iter()
            .filter_map(|parameter| parameter["name"].as_str())
            .collect();
        assert!(volume_names.contains(&"limit"));
        assert!(volume_names.contains(&"afterApiKeyHash"));
        assert!(volume_names.contains(&"afterChainId"));
        assert!(volume_names.contains(&"afterInputToken"));
        assert!(volume_names.contains(&"afterOutputToken"));

        let schemas = &openapi["components"]["schemas"];
        assert_eq!(
            schemas["AttributedExecution"]["properties"]["apiKeyLabel"]["description"],
            "API key identity captured when this execution was attributed."
        );
        assert_eq!(
            schemas["AttributionVolume"]["properties"]["apiKeyLabel"]["description"],
            "Latest known identity for this API key hash, rather than an execution-time snapshot."
        );
    }

    fn test_config(
        registry_url: String,
        private_registry_path: std::path::PathBuf,
        local_db_path: std::path::PathBuf,
        allow_registry_fallback: bool,
    ) -> crate::config::Config {
        crate::config::Config {
            log_dir: "./logs".to_string(),
            database_url: "sqlite::memory:".to_string(),
            database_max_connections: 5,
            usage_log_max_concurrency: 2,
            response_cache_max_entries: 0,
            response_cache_max_trade_rows: None,
            response_cache_ttl_seconds: 0,
            registry_url,
            private_registry_path: private_registry_path.to_string_lossy().into_owned(),
            allow_registry_fallback,
            rate_limit_global_rpm: 600,
            rate_limit_per_key_rpm: 60,
            swap_max_concurrent_global: 12,
            swap_max_concurrent_per_key: 4,
            swap_request_timeout_seconds: 30,
            docs_dir: "./docs/book".to_string(),
            local_db_path: local_db_path.to_string_lossy().into_owned(),
            price_sampler_enabled: false,
            price_sample_interval_seconds: 60,
            price_history_retention_seconds: 604800,
            telemetry: None,
            attribution_start_block: None,
            attribution_sync_interval_seconds: 60,
            attribution_sync_batch_size: 250,
        }
    }

    async fn insert_successful_registry_history(pool: &crate::db::DbPool, artifact: &str) {
        crate::db::registry_history::insert_private_registry_change(
            pool,
            &crate::db::registry_history::NewPrivateRegistryHistory {
                source_commit: "1111111111111111111111111111111111111111",
                payload_sha256: &crate::registry_artifact::artifact_sha256(artifact),
                actor_key_id: "deploy",
                actor_label: "deploy",
                actor_owner: "deploy",
                validation_status: crate::db::registry_history::VALIDATION_STATUS_SUCCESS,
                validation_error: None,
            },
        )
        .await
        .expect("insert registry history");
    }

    #[rocket::async_test]
    async fn test_load_startup_raindex_falls_back_when_private_registry_fails() {
        let dir = tempfile::tempdir().expect("temp dir");
        let private_registry_path = dir.path().join("private-registry.data");
        let local_db_path = dir.path().join("raindex.db");
        let invalid_artifact = "data:text/plain;base64,dGhpcyBpcyBub3QgYSByZWdpc3RyeQo=";
        let fallback_registry_url = mock_raindex_registry_url().await;
        let cfg = test_config(
            fallback_registry_url,
            private_registry_path.clone(),
            local_db_path.clone(),
            true,
        );
        let pool = crate::db::init("sqlite::memory:", 5)
            .await
            .expect("database init");
        let store = crate::registry_artifact::RegistryArtifactStore::new(private_registry_path);

        store
            .persist(invalid_artifact)
            .await
            .expect("persist invalid artifact");
        insert_successful_registry_history(&pool, invalid_artifact).await;

        let provider = super::load_startup_raindex(&cfg, &pool, &store, local_db_path).await;

        assert!(provider.is_ok());
    }

    #[rocket::async_test]
    async fn test_load_startup_raindex_errors_when_fallback_disabled() {
        let dir = tempfile::tempdir().expect("temp dir");
        let private_registry_path = dir.path().join("private-registry.data");
        let local_db_path = dir.path().join("raindex.db");
        let invalid_artifact = "data:text/plain;base64,dGhpcyBpcyBub3QgYSByZWdpc3RyeQo=";
        let fallback_registry_url = mock_raindex_registry_url().await;
        let cfg = test_config(
            fallback_registry_url,
            private_registry_path.clone(),
            local_db_path.clone(),
            false,
        );
        let pool = crate::db::init("sqlite::memory:", 5)
            .await
            .expect("database init");
        let store = crate::registry_artifact::RegistryArtifactStore::new(private_registry_path);

        store
            .persist(invalid_artifact)
            .await
            .expect("persist invalid artifact");
        insert_successful_registry_history(&pool, invalid_artifact).await;

        let err = super::load_startup_raindex(&cfg, &pool, &store, local_db_path)
            .await
            .expect_err("private registry load should fail");

        assert!(matches!(
            err,
            super::StartupRegistryError::PrivateRegistryLoad(_)
        ));
    }

    #[rocket::async_test]
    async fn test_protected_route_returns_401_without_auth() {
        let client = client().await;
        let response = client.get("/v1/tokens").dispatch().await;
        assert_eq!(response.status(), Status::Unauthorized);
    }

    #[rocket::async_test]
    async fn test_protected_route_returns_401_with_wrong_secret() {
        let client = client().await;
        let (key_id, _) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, "wrong-secret");
        let response = client
            .get("/v1/tokens")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Unauthorized);
    }

    #[rocket::async_test]
    async fn test_protected_route_succeeds_with_valid_auth() {
        let client = client().await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);
        let response = client
            .get("/v1/tokens")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;
        assert_ne!(response.status(), Status::Unauthorized);
    }

    #[rocket::async_test]
    async fn test_inactive_key_returns_401() {
        let client = client().await;
        let (key_id, secret) = seed_api_key(&client).await;

        let pool = client
            .rocket()
            .state::<crate::db::DbPool>()
            .expect("pool in state");
        sqlx::query("UPDATE api_keys SET active = 0 WHERE key_id = ?")
            .bind(&key_id)
            .execute(pool)
            .await
            .expect("deactivate key");

        let header = basic_auth_header(&key_id, &secret);
        let response = client
            .get("/v1/tokens")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Unauthorized);
    }
}
