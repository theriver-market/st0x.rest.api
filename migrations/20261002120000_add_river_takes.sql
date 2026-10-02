-- RiverTaker (The River's fee-taking Raindex taker) emits RiverTake(user, ...)
-- for every fill it executes. Raindex records RiverTaker itself as the taker
-- of those fills, so this maps each RiverTaker transaction back to its user
-- for per-user trade history.
CREATE TABLE IF NOT EXISTS river_takes (
    chain_id INTEGER NOT NULL,
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    user_address TEXT NOT NULL,
    PRIMARY KEY (chain_id, tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_river_takes_user
    ON river_takes (chain_id, user_address, block_number DESC);

CREATE TABLE IF NOT EXISTS river_takes_cursor (
    chain_id INTEGER PRIMARY KEY,
    last_block INTEGER NOT NULL
);
