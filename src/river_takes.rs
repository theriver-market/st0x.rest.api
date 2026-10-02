//! Indexes RiverTaker's `RiverTake` events so trade history can attribute
//! RiverTaker fills to the user who made them.
//!
//! Raindex records the immediate caller of `takeOrders4` as the taker, which
//! for fills routed through RiverTaker is the contract, not the user. The
//! event's indexed `user` is `msg.sender` of `RiverTaker.take`, which is the
//! right owner even for smart-wallet users (where `tx.from` is a bundler).

use crate::db::DbPool;
use alloy::primitives::{address, keccak256, Address, B256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::Filter;
use std::time::Duration;
use url::Url;

pub(crate) const RIVER_TAKER_CHAIN_ID: u32 = 8453;
pub(crate) const RIVER_TAKER_BASE: Address = address!("d0daDF272d6c07129c058B937B867428A46c4fDb");
/// A block shortly before RiverTaker was deployed; nothing to index earlier.
const START_BLOCK: u64 = 52_040_000;
/// Stay this far behind the head so a shallow reorg can't drop indexed logs.
const CONFIRMATIONS: u64 = 5;
const MAX_RANGE: u64 = 2_000;
const POLL_EVERY: Duration = Duration::from_secs(30);

fn river_take_topic() -> B256 {
    keccak256("RiverTake(address,address,address,uint256,uint256,uint256)")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RiverTakeRow {
    pub tx_hash: B256,
    pub log_index: u64,
    pub block_number: u64,
    pub user: Address,
}

/// Transaction hashes of the user's RiverTaker fills, newest first.
pub(crate) async fn tx_hashes_for_user(
    pool: &DbPool,
    chain_id: u32,
    user: Address,
) -> Result<Vec<B256>, sqlx::Error> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT tx_hash FROM river_takes \
         WHERE chain_id = ? AND user_address = ? ORDER BY block_number DESC",
    )
    .bind(i64::from(chain_id))
    .bind(format!("{user:#x}"))
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(hash,)| hash.parse::<B256>().ok())
        .collect())
}

async fn cursor(pool: &DbPool, chain_id: u32) -> Result<Option<u64>, sqlx::Error> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT last_block FROM river_takes_cursor WHERE chain_id = ?")
            .bind(i64::from(chain_id))
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(b,)| b as u64))
}

/// Stores one indexed range atomically: the rows and the advanced cursor.
pub(crate) async fn store_range(
    pool: &DbPool,
    chain_id: u32,
    rows: &[RiverTakeRow],
    to_block: u64,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    for row in rows {
        sqlx::query(
            "INSERT OR IGNORE INTO river_takes \
             (chain_id, tx_hash, log_index, block_number, user_address) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(i64::from(chain_id))
        .bind(format!("{:#x}", row.tx_hash))
        .bind(row.log_index as i64)
        .bind(row.block_number as i64)
        .bind(format!("{:#x}", row.user))
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(
        "INSERT INTO river_takes_cursor (chain_id, last_block) VALUES (?, ?) \
         ON CONFLICT(chain_id) DO UPDATE SET last_block = excluded.last_block",
    )
    .bind(i64::from(chain_id))
    .bind(to_block as i64)
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

async fn index_once(pool: &DbPool, rpc: &Url) -> Result<usize, String> {
    let provider = ProviderBuilder::new().connect_http(rpc.clone());
    let head = provider
        .get_block_number()
        .await
        .map_err(|e| e.to_string())?;
    let safe_head = head.saturating_sub(CONFIRMATIONS);
    let mut from = match cursor(pool, RIVER_TAKER_CHAIN_ID)
        .await
        .map_err(|e| e.to_string())?
    {
        Some(last) => last + 1,
        None => START_BLOCK,
    };
    let mut stored = 0;
    while from <= safe_head {
        let to = (from + MAX_RANGE - 1).min(safe_head);
        let filter = Filter::new()
            .address(RIVER_TAKER_BASE)
            .event_signature(river_take_topic())
            .from_block(from)
            .to_block(to);
        let logs = provider
            .get_logs(&filter)
            .await
            .map_err(|e| e.to_string())?;
        let rows = logs
            .iter()
            .filter_map(|log| {
                let user = log.topics().get(1)?;
                Some(RiverTakeRow {
                    tx_hash: log.transaction_hash?,
                    log_index: log.log_index?,
                    block_number: log.block_number?,
                    user: Address::from_word(*user),
                })
            })
            .collect::<Vec<_>>();
        store_range(pool, RIVER_TAKER_CHAIN_ID, &rows, to)
            .await
            .map_err(|e| e.to_string())?;
        stored += rows.len();
        from = to + 1;
    }
    Ok(stored)
}

/// Polls forever; tries each configured RPC in turn and logs failures without
/// advancing the cursor, so a failed range is retried on the next tick.
pub(crate) async fn supervise(pool: DbPool, rpcs: Vec<Url>) {
    if rpcs.is_empty() {
        tracing::warn!("river_takes: no RPC for chain 8453; indexer not started");
        return;
    }
    let mut interval = tokio::time::interval(POLL_EVERY);
    loop {
        interval.tick().await;
        for rpc in &rpcs {
            match index_once(&pool, rpc).await {
                Ok(n) => {
                    if n > 0 {
                        tracing::info!(stored = n, "river_takes: indexed RiverTake events");
                    }
                    break;
                }
                Err(error) => tracing::warn!(%error, "river_takes: indexing failed on one RPC"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::b256;

    #[test]
    fn topic_matches_the_live_contract_event() {
        // topic0 of RiverTaker's log in Base tx 0xa17f52fc…3bec (prod canary, 1 Oct).
        assert_eq!(
            river_take_topic(),
            b256!("0a420e6fe6d669fe652da52b75c03e896e7b746e34bab2bb5b34aa2207e0a8ca")
        );
    }

    #[rocket::async_test]
    async fn stores_rows_and_cursor_and_returns_newest_first_per_user() {
        let pool = crate::db::init("sqlite::memory:", 1).await.unwrap();
        let user = address!("944F1dafb62F6886336660B31b87f9f7613496de");
        let other = address!("1111111111111111111111111111111111111111");
        let older = b256!("00000000000000000000000000000000000000000000000000000000000000aa");
        let newer = b256!("00000000000000000000000000000000000000000000000000000000000000bb");
        let theirs = b256!("00000000000000000000000000000000000000000000000000000000000000cc");
        let rows = vec![
            RiverTakeRow {
                tx_hash: older,
                log_index: 3,
                block_number: 100,
                user,
            },
            RiverTakeRow {
                tx_hash: newer,
                log_index: 1,
                block_number: 200,
                user,
            },
            RiverTakeRow {
                tx_hash: theirs,
                log_index: 1,
                block_number: 150,
                user: other,
            },
        ];
        store_range(&pool, 8453, &rows, 250).await.unwrap();
        // Re-storing the same range is idempotent.
        store_range(&pool, 8453, &rows, 250).await.unwrap();

        assert_eq!(
            tx_hashes_for_user(&pool, 8453, user).await.unwrap(),
            vec![newer, older]
        );
        assert_eq!(
            tx_hashes_for_user(&pool, 8453, other).await.unwrap(),
            vec![theirs]
        );
        assert!(tx_hashes_for_user(&pool, 1, user).await.unwrap().is_empty());
        assert_eq!(cursor(&pool, 8453).await.unwrap(), Some(250));
    }
}
