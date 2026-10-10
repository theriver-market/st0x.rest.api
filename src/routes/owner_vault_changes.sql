-- Every balance change for every vault owned by ?2 on chain ?1, in one query.
-- Vault set = the SDK's fetch_vaults set for one owner: vaults with a running
-- balance plus vaults referenced by the owner's orders (balance never touched),
-- joined to erc20_tokens like the SDK (vaults without token metadata are left
-- out there too). Change rows and senders follow the SDK's
-- fetch_vault_balance_changes query. Vaults without changes come back as one
-- row with NULL change columns.
-- raindex_address is listed per chain first so the (chain, raindex, owner)
-- indexes apply.
WITH raindexes AS (
  SELECT raindex_address FROM target_watermarks WHERE chain_id = ?1
  UNION
  SELECT DISTINCT raindex_address FROM erc20_tokens WHERE chain_id = ?1
),
owned AS (
  SELECT rvb.chain_id, rvb.raindex_address, rvb.owner, rvb.token, rvb.vault_id
  FROM running_vault_balances rvb
  JOIN raindexes r ON r.raindex_address = rvb.raindex_address
  WHERE rvb.chain_id = ?1
    AND rvb.owner = ?2
  UNION
  SELECT io.chain_id, io.raindex_address, oe.order_owner, io.token, io.vault_id
  FROM order_events oe
  JOIN raindexes r ON r.raindex_address = oe.raindex_address
  JOIN order_ios io
    ON io.chain_id = oe.chain_id
   AND io.raindex_address = oe.raindex_address
   AND io.transaction_hash = oe.transaction_hash
   AND io.log_index = oe.log_index
  WHERE oe.chain_id = ?1
    AND oe.order_owner = ?2
)
SELECT
  v.raindex_address AS raindexAddress,
  v.owner,
  v.token,
  v.vault_id AS vaultId,
  et.decimals AS tokenDecimals,
  vbc.transaction_hash AS transactionHash,
  vbc.log_index AS logIndex,
  vbc.block_number AS blockNumber,
  vbc.block_timestamp AS blockTimestamp,
  CASE WHEN vbc.transaction_hash IS NULL THEN NULL ELSE COALESCE(
    (
      SELECT d.sender
      FROM deposits d
      WHERE d.chain_id = vbc.chain_id
        AND d.raindex_address = vbc.raindex_address
        AND d.transaction_hash = vbc.transaction_hash
        AND d.log_index = vbc.log_index
        AND vbc.change_type = 'DEPOSIT'
      LIMIT 1
    ),
    (
      SELECT w.sender
      FROM withdrawals w
      WHERE w.chain_id = vbc.chain_id
        AND w.raindex_address = vbc.raindex_address
        AND w.transaction_hash = vbc.transaction_hash
        AND w.log_index = vbc.log_index
        AND vbc.change_type = 'WITHDRAW'
      LIMIT 1
    ),
    (
      SELECT t.sender
      FROM take_orders t
      WHERE t.chain_id = vbc.chain_id
        AND t.raindex_address = vbc.raindex_address
        AND t.transaction_hash = vbc.transaction_hash
        AND t.log_index = vbc.log_index
        AND vbc.change_type IN ('TAKE_INPUT', 'TAKE_OUTPUT')
      LIMIT 1
    ),
    (
      SELECT c.sender
      FROM clear_v3_events c
      WHERE c.chain_id = vbc.chain_id
        AND c.raindex_address = vbc.raindex_address
        AND c.transaction_hash = vbc.transaction_hash
        AND c.log_index = vbc.log_index
        AND vbc.change_type IN (
          'CLEAR_ALICE_INPUT',
          'CLEAR_ALICE_OUTPUT',
          'CLEAR_BOB_INPUT',
          'CLEAR_BOB_OUTPUT',
          'CLEAR_ALICE_BOUNTY',
          'CLEAR_BOB_BOUNTY'
        )
      LIMIT 1
    ),
    vbc.owner
  ) END AS transactionSender,
  vbc.change_type AS changeType,
  vbc.delta,
  vbc.running_balance AS runningBalance
FROM owned v
JOIN erc20_tokens et
  ON et.chain_id = v.chain_id
 AND et.raindex_address = v.raindex_address
 AND et.token_address = v.token
LEFT JOIN vault_balance_changes vbc
  ON vbc.chain_id = v.chain_id
 AND vbc.raindex_address = v.raindex_address
 AND vbc.owner = v.owner
 AND vbc.token = v.token
 AND vbc.vault_id = v.vault_id
ORDER BY
  v.raindex_address,
  v.token,
  v.vault_id,
  vbc.block_timestamp DESC,
  vbc.block_number DESC,
  vbc.log_index DESC;
