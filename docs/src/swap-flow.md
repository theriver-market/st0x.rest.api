# Swap Flow

Swapping is a two-step process: get a **quote** to preview pricing, then get
**calldata** to build the on-chain transaction.

## Step 1: Get a Quote

For new integrations, use `POST /v3/swap/quote`. It uses the same mode,
slippage, reference-price guard, and SDK simulation as V3 calldata, so the
displayed quote describes the route the API can execute.

### Recommended: V3 Mode-Based Quote

```
POST /v3/swap/quote
```

#### Request

```bash
curl -X POST https://api.st0x.io/v3/swap/quote \
  -H "Authorization: Basic <credentials>" \
  -H "Content-Type: application/json" \
  -d '{
    "chainId": 8453,
    "taker": "0xYourWalletAddress",
    "inputToken": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    "outputToken": "0x4200000000000000000000000000000000000006",
    "mode": "buyUpTo",
    "amount": "1.0",
    "slippageBps": 50,
    "referenceIoRatio": "2500",
    "denomination": "wrapped"
  }'
```

| Field              | Required | Description                                                                                                                                                            |
| ------------------ | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `chainId`          | Yes      | Network chain ID. V3 never infers a network                                                                                                                            |
| `taker`            | No       | Wallet that would execute the swap. Recommended because oracle-backed orders can use it when building signed context                                                   |
| `inputToken`       | Yes      | Address of the token the taker will spend                                                                                                                              |
| `outputToken`      | Yes      | Address of the token the taker will receive                                                                                                                            |
| `mode`             | Yes      | `"buyUpTo"`, `"spendExact"`, or `"spendUpTo"`                                                                                                                          |
| `amount`           | Yes      | Output-token amount for `buyUpTo`; input-token amount for `spendExact` and `spendUpTo`                                                                                 |
| `priceCap`         | One of   | Explicit maximum input-token amount per one output token                                                                                                               |
| `slippageBps`      | One of   | Integer from 1 to 5000. The API derives the final price cap from the selected SDK simulation                                                                           |
| `referenceIoRatio` | No       | Optional trusted input-token-per-output-token reference. Valid only with `slippageBps`; candidate ratios more than 5% above it are excluded before slippage is applied |
| `denomination`     | No       | `"wrapped"` by default. `"unwrapped"` changes the units of numeric fields, but `inputToken` and `outputToken` must still be wrapped/orderbook token addresses          |

Provide exactly one of `priceCap` or `slippageBps`.

#### Response

```json
{
  "chainId": 8453,
  "inputToken": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
  "outputToken": "0x4200000000000000000000000000000000000006",
  "mode": "buyUpTo",
  "amount": "1.0",
  "denomination": "wrapped",
  "estimatedOutput": "1.0",
  "estimatedInput": "2500.0",
  "estimatedIoRatio": "2500.0",
  "fullyFilled": true,
  "resolvedPriceCap": "2512.5"
}
```

| Field              | Description                                                                                                                                    |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `estimatedOutput`  | Output-token amount selected by the simulation                                                                                                 |
| `estimatedInput`   | Input-token amount selected by the simulation                                                                                                  |
| `estimatedIoRatio` | Simulated input-token amount per one output token                                                                                              |
| `fullyFilled`      | Whether the requested amount was completely filled. Up-to modes can return `false`; exact modes fail when they cannot fill the complete amount |
| `resolvedPriceCap` | Final maximum input-token amount per one output token: either the supplied `priceCap` or the cap derived from `slippageBps`                    |
| `denomination`     | Units used by the numeric response fields                                                                                                      |

The quote reflects current orderbook state. Prices may change between quoting
and execution.

### Compatibility: V2 Mode-Based Quote

`POST /v2/swap/quote` retains the existing mode-based request shape for Base
clients. Omitting `chainId` selects Base (`8453`). Use V3 for any other network.

Oracle-backed orders can be temporarily unavailable for evaluation when their
external context fetch fails. When no order for the pair could be quoted because
of that, quote endpoints return HTTP 503 with `SWAP_ORACLE_UNAVAILABLE`. When
other orders were quoted, those orders answer: a full fill, an up-to quote with
`fullyFilled: false` and the amount available, or HTTP 404 `SWAP_NO_LIQUIDITY`
for an exact request they cannot fill. `SWAP_NO_LIQUIDITY` means the evaluated
order set has no executable capacity for the request.

An oracle endpoint that answers HTTP 404, 405 or 410 (or 400 to an
`allowFailure` batch) three times within 10 minutes, for example a retired
oracle URL an order still names, is skipped for 15 minutes. Then one probe
request checks whether it came back. Its orders count as oracle-unavailable
while it is skipped. Timeouts, connection errors and 5xx responses, such as the
oracle's off-hours `no_live_quote` 503, never cause an endpoint to be skipped.

### Understanding Price Limits

`priceCap`, `estimatedIoRatio`, `referenceIoRatio`, and `resolvedPriceCap`
always mean:

```
input-token amount / output-token amount
```

For example:

- USDC -> WETH at 2500 USDC per WETH uses an I/O ratio of `2500`.
- WETH -> USDC at the same market price uses the reciprocal ratio, `0.0004` WETH
  per USDC.

`slippageBps` is an integer tolerance in basis points:

| BPS   | Percentage |
| ----- | ---------- |
| `1`   | 0.01%      |
| `50`  | 0.5%       |
| `100` | 1%         |
| `500` | 5%         |

The API simulates the executable route with the SDK, finds the worst selected
fill ratio, and calculates:

```
resolvedPriceCap = worst selected I/O ratio * (1 + slippageBps / 10,000)
```

`referenceIoRatio` is not another slippage value. It is an optional independent
safety reference, normally derived from a trusted oracle. Before calculating the
slippage cap, the API excludes any candidate whose I/O ratio is more than 5%
above the reference. Omit it when no trusted reference is available.

Alternatively, send `priceCap` to provide the final maximum I/O ratio directly.
Do not send `slippageBps` or `referenceIoRatio` with an explicit `priceCap`.

When `denomination` is omitted or set to `"wrapped"`, all numeric values use
wrapped/orderbook token units. With `"unwrapped"`, the API converts numeric
request and response values for wrapped ST0x/ERC4626 tokens, while token
addresses remain wrapped/orderbook addresses.

### Legacy: V1 Output-Targeted Quote

```
POST /v1/swap/quote
```

V1 accepts `inputToken`, `outputToken`, `outputAmount`, and optional
`denomination`. V1 and V2 swaps remain Base-compatible: omitting `chainId`
selects Base, while multi-network swaps use V3. V1 only supports an
output-targeted quote and does not apply a slippage limit. Existing integrations
can continue using V1 or V2; new multi-network integrations should use V3.

Do not pass unwrapped-normalized quote values into other endpoints unless those
endpoints explicitly support `denomination=unwrapped` and you call them that
way. The calldata endpoints support the same `denomination` field, but swaps
still use wrapped/orderbook token addresses.

## Step 2: Get Calldata

For new integrations, use `POST /v3/swap/calldata`. It supports both
output-targeted swaps and spend-mode swaps. `POST /v1/swap/calldata` remains
available for existing output-targeted clients.

### Recommended: V3 Mode-Based Calldata

```
POST /v3/swap/calldata
```

#### Request

```bash
curl -X POST https://api.st0x.io/v3/swap/calldata \
  -H "Authorization: Basic <credentials>" \
  -H "Content-Type: application/json" \
  -d '{
    "chainId": 8453,
    "taker": "0xYourWalletAddress",
    "inputToken": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    "outputToken": "0x4200000000000000000000000000000000000006",
    "mode": "spendExact",
    "amount": "2500.0",
    "slippageBps": 50,
    "referenceIoRatio": "2500",
    "denomination": "wrapped"
  }'
```

| Field              | Type   | Description                                                                                                                                                                                      |
| ------------------ | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `chainId`          | number | Required network chain ID. V3 never infers a network                                                                                                                                             |
| `taker`            | string | Wallet that will execute the swap and whose allowance is checked                                                                                                                                 |
| `inputToken`       | string | Wrapped/orderbook token the taker will spend                                                                                                                                                     |
| `outputToken`      | string | Wrapped/orderbook token the taker will receive                                                                                                                                                   |
| `mode`             | string | `"buyUpTo"`, `"spendExact"`, or `"spendUpTo"`                                                                                                                                                    |
| `amount`           | string | Output-token amount for `buyUpTo`; input-token amount for `spendExact` and `spendUpTo`                                                                                                           |
| `priceCap`         | string | Explicit maximum input-token amount per one output token. Provide exactly one of `priceCap` or `slippageBps`                                                                                     |
| `slippageBps`      | number | Integer from 1 to 5000. For example, `50` means 0.5% and `100` means 1%. The API derives `resolvedPriceCap` from the selected SDK simulation. Provide exactly one of `priceCap` or `slippageBps` |
| `referenceIoRatio` | string | Optional trusted input-token-per-output-token reference. Valid only with `slippageBps`; candidate ratios more than 5% above it are excluded before slippage is applied                           |
| `denomination`     | string | Optional. `"wrapped"` is the default. `"unwrapped"` changes the units of `amount`, price ratios, and returned estimates; token addresses remain wrapped/orderbook addresses                      |

Mode behavior:

| Mode         | Amount Means                 | Fill Behavior                                   |
| ------------ | ---------------------------- | ----------------------------------------------- |
| `buyUpTo`    | Output-token amount to buy   | Buy up to `amount`; partial fills are allowed   |
| `spendExact` | Input-token amount to spend  | Spend exactly `amount`; fails if not fillable   |
| `spendUpTo`  | Maximum input-token to spend | Spend up to `amount`; partial fills are allowed |

The price-control fields have exactly the same meaning and orientation as in the
V3 quote request described above. No orderbook walking or price-cap calculation
is required in the client. For example, on USDC -> WETH, `"priceCap": "2600"`
means the swap will not spend more than 2600 USDC per one WETH.

### Compatibility: V2 Mode-Based Calldata

`POST /v2/swap/calldata` retains the existing mode-based request shape for Base
clients. Omitting `chainId` selects Base (`8453`). Use V3 for any other network.

### Legacy: V1 Output-Targeted Calldata

```
POST /v1/swap/calldata
```

#### Request

```bash
curl -X POST https://api.st0x.io/v1/swap/calldata \
  -H "Authorization: Basic <credentials>" \
  -H "Content-Type: application/json" \
  -d '{
    "chainId": 8453,
    "taker": "0xYourWalletAddress",
    "inputToken": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    "outputToken": "0x4200000000000000000000000000000000000006",
    "outputAmount": "1.0",
    "maximumIoRatio": "2600.0",
    "denomination": "wrapped"
  }'
```

| Field            | Type   | Description                                                                                                                                                                     |
| ---------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `chainId`        | number | Network chain ID. May be omitted only while the API has exactly one configured network                                                                                          |
| `taker`          | string | Your wallet address that will execute the transaction                                                                                                                           |
| `inputToken`     | string | Wrapped/orderbook token address you are selling                                                                                                                                 |
| `outputToken`    | string | Wrapped/orderbook token address you want to receive                                                                                                                             |
| `outputAmount`   | string | Desired output amount in the selected `denomination`                                                                                                                            |
| `maximumIoRatio` | string | Maximum acceptable IO ratio in the selected `denomination`                                                                                                                      |
| `denomination`   | string | Optional. `"wrapped"` (default) uses orderbook units. `"unwrapped"` interprets `outputAmount` and `maximumIoRatio` as unwrapped display values for wrapped ST0x/ERC4626 tokens. |

V1 is equivalent to V2 with `"mode": "buyUpTo"`. It cannot express spend-based
intent.

Set `maximumIoRatio` slightly above the `estimatedIoRatio` from the quote to
allow for price movement.

For calldata, `denomination=unwrapped` only changes how numeric fields are
interpreted and displayed. The API converts request amounts and price limits to
wrapped/orderbook units before generating calldata. Clients must still pass the
wrapped/orderbook token addresses for `inputToken` and `outputToken`; the
endpoint does not translate unwrapped asset addresses.

### Response

The following examples show the v2 response. V1 returns the same transaction and
approval fields without `resolvedPriceCap`. The content depends on whether your
`taker` address has sufficient token approvals.

**If approvals are needed**, `data` is empty and `approvals` contains the
required transactions:

```json
{
  "chainId": 8453,
  "to": "0xOrderbookContractAddress",
  "data": "0x",
  "value": "0x0",
  "estimatedInput": "2500.0",
  "denomination": "wrapped",
  "resolvedPriceCap": "2613",
  "approvals": [
    {
      "token": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
      "spender": "0xOrderbookContractAddress",
      "amount": "2500.0",
      "symbol": "USDC",
      "approvalData": "0x..."
    }
  ]
}
```

**If approvals are already in place**, `approvals` is empty and `data` contains
the swap calldata:

```json
{
  "chainId": 8453,
  "to": "0xOrderbookContractAddress",
  "data": "0xabcdef...",
  "value": "0x0",
  "estimatedInput": "2500.0",
  "denomination": "wrapped",
  "resolvedPriceCap": "2613",
  "approvals": []
}
```

| Field              | Type   | Description                                                                                                                                                                                    |
| ------------------ | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `chainId`          | number | Network chain ID used to build the transaction                                                                                                                                                 |
| `to`               | string | Contract address to send the transaction to                                                                                                                                                    |
| `data`             | string | Encoded transaction calldata — empty (`"0x"`) when approvals are needed                                                                                                                        |
| `value`            | string | Native token value to send (usually `"0x0"`)                                                                                                                                                   |
| `estimatedInput`   | string | Expected input amount in the requested `denomination` when calldata is ready. When approvals are needed, this is the input-token approval amount/cap required before calldata can be generated |
| `denomination`     | string | Denomination used for `estimatedInput`                                                                                                                                                         |
| `approvals`        | array  | Token approvals needed — if non-empty, approve first then call this endpoint again                                                                                                             |
| `resolvedPriceCap` | string | V2 only. Final maximum input-token amount per one output token. If approvals are required, send it as `priceCap` on the retry and omit `slippageBps` and `referenceIoRatio`                    |

Approval entries always describe the actual on-chain approval requirements in
wrapped/orderbook token units. They are not converted or relabeled when
`denomination=unwrapped`. For approval-required responses, `estimatedInput`
matches the approval requirement expressed in the requested `denomination`, not
the final simulated spend. Call the calldata endpoint again after approving to
receive the ready calldata response with the expected input amount.

## Step 3: Handle Approvals

If the `approvals` array is **not empty**, send the approval transactions first:

1. For each approval, send a transaction to the `token` address with
   `approvalData` as calldata
2. Wait for confirmation
3. **Call the calldata endpoint again** with the first response's
   `resolvedPriceCap` as `priceCap`. Omit `slippageBps` and `referenceIoRatio`.
   Preserve the original `denomination`, including when it is `"unwrapped"`.
   With approvals in place, the response will now contain the swap calldata
   while preserving the original units and price limit

## Step 4: Execute the Swap

Once you receive a response with an empty `approvals` array, send the main
transaction using `to`, `data`, and `value`.

## Complete Example

```bash
# 1. Get quote
QUOTE=$(curl -s -X POST https://api.st0x.io/v3/swap/quote \
  -H "Authorization: Basic <credentials>" \
  -H "Content-Type: application/json" \
  -d '{
    "chainId": 8453,
    "taker": "0xYourWalletAddress",
    "inputToken": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    "outputToken": "0x4200000000000000000000000000000000000006",
    "mode": "buyUpTo",
    "amount": "1.0",
    "slippageBps": 50,
    "referenceIoRatio": "2500",
    "denomination": "wrapped"
  }')

echo "$QUOTE" | jq '{estimatedInput, estimatedOutput, estimatedIoRatio, fullyFilled, resolvedPriceCap}'

# 2. Get fresh executable calldata with the same price controls
CALLDATA=$(curl -s -X POST https://api.st0x.io/v3/swap/calldata \
  -H "Authorization: Basic <credentials>" \
  -H "Content-Type: application/json" \
  -d '{
    "chainId": 8453,
    "taker": "0xYourWalletAddress",
    "inputToken": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    "outputToken": "0x4200000000000000000000000000000000000006",
    "mode": "buyUpTo",
    "amount": "1.0",
    "slippageBps": 50,
    "referenceIoRatio": "2500",
    "denomination": "wrapped"
  }')

RESOLVED_PRICE_CAP=$(echo "$CALLDATA" | jq -r '.resolvedPriceCap')

# 3. Check if approvals are needed
#    The first response only contains approvals — "data" will be empty ("0x").
#    You must send the approval transactions on-chain first, then call
#    the calldata endpoint again to get the actual swap calldata.
APPROVALS=$(echo "$CALLDATA" | jq '.approvals')
if [ "$APPROVALS" != "[]" ]; then
  # Send each approval transaction on-chain...
  # (use approvalData from each entry as calldata to the token address)

  # Now call the calldata endpoint again — this time approvals are in place
  # and the response will contain the swap calldata in "data"
  CALLDATA=$(curl -s -X POST https://api.st0x.io/v3/swap/calldata \
    -H "Authorization: Basic <credentials>" \
    -H "Content-Type: application/json" \
    -d '{
      "chainId": 8453,
      "taker": "0xYourWalletAddress",
      "inputToken": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
      "outputToken": "0x4200000000000000000000000000000000000006",
      "mode": "buyUpTo",
      "amount": "1.0",
      "priceCap": "'"$RESOLVED_PRICE_CAP"'",
      "denomination": "wrapped"
    }')
fi

# 4. Execute the swap transaction using to, data, and value from the response
echo "$CALLDATA" | jq '{to, data, value}'
```
