# Take simulation

## Problem

A Raindex quote (`quote2`) evaluates only an order's calculate-io. Whatever the
order does in handle-io (a minimum trade size, for example) runs only when the
trade is taken. On 13 Oct 2026 `/v3/swap/quote` answered `fullyFilled: true`
for a 0.172879 wtSPYM sell, `/v3/swap/calldata` answered 200, and the wallet's
own simulation then reverted with "Min trade amount".

Orders can contain anything, so the API must not know what any order does. The
orders are the source of truth; the API runs them.

## What is simulated

The real `takeOrders4(config)` call on the orderbook, by `eth_call` at the
latest block, with:

- `from`: the quote's `taker`, which must be the address that will call
  `takeOrders4`: the user's wallet for a direct take, the RiverTaker contract
  when the site executes through it;
- `to`: the orderbook the selection uses;
- `config`: built from the quote's own selection by the SDK's
  `build_take_orders_config_from_simulation`, the function the calldata path
  uses, with the same mode, amount and price cap;
- a state override on the input token only: the taker's balance and the
  taker's allowance to the orderbook are set to 2^95 for that call.

Everything else is live chain state: orderbook, interpreter, store, vault
balances, order bytecode, signed context, the output token. Nothing is
reimplemented.

### Per-order isolation

One reverting order must not cost the route the other orders.

1. Simulate the selected batch.
2. If it reverts and has several legs, simulate each leg alone (same price
   cap, `maximumIO` set to what that leg takes in the batch, `minimumIO` zero
   because it is an aggregate constraint). A single-leg batch is its own
   answer.
3. Legs that revert alone are excluded, with their raw revert data kept.
4. Selection is re-run over the remaining candidates and the new batch is
   simulated. Repeat.

Outcomes:

- a batch executes: the quote describes that batch (`status: executes`), and
  lists what was excluded on the way;
- nothing is left, or the remaining orders cannot form a take (an exact mode
  they no longer fill): `status: reverts`, `fullyFilled: false`, with the
  excluded orders and their reasons. The estimates are those of the last
  selection that was tried;
- a batch reverts but every leg executes alone: `status: reverts` with the
  batch's revert data and no order blamed;
- any call fails to give an answer: `status: notSimulated`.

Bounds: at most 4 rounds; a reverting batch of more than 12 legs is not taken
apart; the whole search has 8 seconds. Worst case is 4 x (1 batch + 2 funding
read-backs + 12 legs) + 2 layout probes = 62 `eth_call`s. The normal case, a
batch that executes for a token seen before, is 1.

## When

- **Quote** (`POST /v2|v3/swap/quote`) whenever the request names a `taker`.
  `/v1/swap/quote` has no taker and is unchanged.
- **Calldata** (`POST /v1|v2|v3/swap/calldata`) only when `taker` is the pinned
  RiverTaker. The SDK builds that calldata with no preflight
  (`get_take_orders_calldata_unchecked`), so it would hand back an order the
  quote had excluded. The same search runs; if orders were excluded the API
  builds the calldata from the surviving selection itself, and if no take
  executes it answers `SWAP_NO_LIQUIDITY`. With nothing excluded, or with no
  simulation answer, the SDK path is used exactly as before.
- Direct takes (taker = user wallet) are unchanged at calldata time: without
  an allowance the SDK returns `approvals`; with one, the SDK's own preflight
  already drops legs that revert.

## Response

`SwapQuoteV2Response` gains `takeSimulation`:

```json
{
  "estimatedInput": "30",
  "estimatedOutput": "10",
  "fullyFilled": true,
  "takeSimulation": {
    "status": "executes",
    "excludedOrders": [
      {
        "orderHash": "0x...",
        "revertData": "0x08c379a0...",
        "revertReason": "Min trade amount"
      }
    ]
  }
}
```

- `status`: `executes` | `reverts` | `notSimulated`.
- `reason` (only `notSimulated`): `noTaker`, `overrideUnavailable`,
  `rpcUnavailable`, `timedOut`, `roundsExhausted`, `buildFailed`.
- `revertData`, `revertReason` (only `reverts`, when the batch reverts as a
  whole and no order could be blamed).
- `excludedOrders[]`: `orderHash`, `revertData`, optional `revertReason`.

`revertData` is the chain's bytes, verbatim. `revertReason` is present only
when those bytes are a standard `Error(string)`, decoded and passed through
unchanged. It is data from the order for display or logging; the API never
branches on it and clients should not either.

`fullyFilled` is forced to `false` when `status` is `reverts`, so clients that
only read `fullyFilled` also stop offering the route. For that case an exact
mode returns 200 instead of `SWAP_NO_LIQUIDITY`, so the reasons reach the
caller. When `status` is `executes` with exclusions, the estimates and
`fullyFilled` describe the surviving route.

## Failure policy

"Not simulated" is never reported as pass or fail.

- No taker, no RPC, an RPC error that is not a revert with data, a timeout, a
  token whose storage cannot be located: `notSimulated`, and the quote is
  otherwise exactly what it was before this change.
- A revert is only reported after the token is read back under the same
  overrides (`balanceOf(taker)` and `allowance(taker, orderbook)` must both
  return the simulated amount). If they do not, the funding did not take, the
  cached layout is dropped, and the answer is `notSimulated`
  (`overrideUnavailable`). This is checked once per batch; the per-leg calls
  of that round reuse the same overrides.
- A revert without data (a bare `revert()`, out of gas) is not distinguishable
  from a node problem and is reported as `notSimulated`.

## Funding the taker: why storage probing

Approvals on the site are exact, so at quote time the taker has no allowance
and may hold too little of the input token. A plain `eth_call` would revert in
the token transfer before reaching order logic.

The balance and allowance mapping slots differ per token, so they are found by
probing: one `eth_call` to `balanceOf(taker)` with every candidate slot
overridden to a distinct value; the returned value names the slot. Same for
`allowance(taker, orderbook)`. Candidates are Solidity mappings at declared
slots 0..31, OpenZeppelin's ERC-7201 upgradeable ERC-20 namespace, and Vyper
mappings at slots 0..31. The result is cached per chain and token in process
memory. Proxies need no special handling, because the override is on the
proxy's storage and the token's own code reads it.

Alternatives considered:

- *Replace the token's code with a stub ERC-20.* Breaks any token with real
  logic (ERC-4626 wrappers, blacklists, pausing), so the simulation would stop
  being the real take.
- *Simulate from an address that already holds balance and allowance.* None
  exists in general, and it changes `msg.sender`, which orders can read as
  the counterparty.
- *Probing* runs the real token code on real storage with two slots changed,
  verifies itself (a wrong guess simply does not match), and degrades to
  `notSimulated` for layouts it does not know (rebasing or share-based
  balances, Solady's layout).

## Why `eth_call` of `takeOrders4`, and what else the Raindex code offers

Looked at in the vendored SDK (`lib/rain.orderbook`) and its dependencies:

- `quote2` on the orderbook, and the SDK's quote paths built on it
  (`crates/quote`, `raindex_client/order_quotes.rs`): calculate-io only, by
  construction. This is the gap.
- The SDK's take preflight (`take_orders/preflight.rs`:
  `simulate_take_orders`, `diagnose_take_orders_failure`, and
  `rebuild_without_failing_leg` in `raindex_client/take_orders/mod.rs`): the
  same idea as here, an `eth_call` of `takeOrders4` from the taker with
  failing legs dropped. It has no state overrides, so it only runs once the
  taker has approved, and the executor path skips it. The clean long-term
  home for this feature is that preflight, taught to fund the taker by
  override; the submodule is not modified from this repository, so that is an
  upstream change. Until then this module does the same job at the API layer.
- `IInterpreterV4.eval4` with `stateOverlay` (`RaindexV6.sol`
  `calculateOrderIO` / `handleIO`): the overlay seeds the interpreter's
  in-memory store for one eval; the orderbook uses it so calculate-io writes
  from earlier orders in a batch are visible to later ones. Handle-io could be
  evaluated directly through `eval4` with `sourceIndex = 1`, but the caller
  would have to rebuild the context the orderbook builds (calculated amounts,
  vault balances before and after, decimals, signed context) and the overlay
  from calculate-io's writes. That is a reimplementation of `takeOrders4` in
  the API, which is the thing to avoid, and it skips the vault and token
  movements entirely.
- `rain-forker` / `rainlang-eval` (`Forker`, `fork_eval` with a
  `state_overlay`; used by the SDK's `QuoteDebugger`, `TradeReplayer`,
  `fuzz`): a revm executor over forked chain state. It can run the real
  `takeOrders4` locally with any state edited. It loads state lazily over RPC
  (many `eth_getStorageAt`/`eth_getCode` round trips per fork), runs
  synchronously, and is a debugging tool today. Heavier than one `eth_call`
  for the same answer at the latest block.

`eth_call` with a two-slot override was chosen because it executes the real
contracts against the real state with nothing modelled, in one round trip.

What it does not cover:

- State that changes between the simulation and the send (another taker, a
  vault withdrawal, an oracle price past its expiry). The site's own pre-send
  simulation stays as the last check.
- Orders that read the taker's real input-token balance or allowance see the
  simulated 2^95 instead.
- The calldata endpoint appends the API's attribution signed context to every
  order and refreshes oracle signed context. The quote-time simulation uses
  the quote's signed context without the attribution entry. An order that
  inspects every signed context could behave differently.
- For RiverTaker the simulated call is the inner `takeOrders4` from the
  RiverTaker address, not `RiverTaker.take` from the user: the fee transfer,
  `minOut` and the user's approval to RiverTaker are outside it.
- `takeOrders4` skips an order that is no longer live rather than reverting.
  Such a take "executes" while filling less than quoted. The return values
  are not inspected yet.

## Longer term: a virtual Raindex

The project owner once started a "virtual raindex": load all the state an
order can reference and simulate many orders for a block in revm. No such code
was found in the vendored SDK or in the other local River/st0x checkouts; the
closest existing piece is `rain-forker` above. Compared with this branch, a
local engine would simulate every candidate once per block rather than per
request, give per-order answers without extra RPC calls, allow any state
overlay (not just two token slots), and let selection use executable amounts
directly. It costs a state-loading and invalidation layer, and its answers are
only as good as that layer's fidelity to the chain. This branch asks the chain
itself, which is exact but pays RPC calls per quote.

## The 13 Oct 2026 incident, replayed

The logged `/v3/swap/calldata` responses were replayed read-only at the blocks
of the two failures (52562140, 52562145 and 52562166; `mainnet.base.org` and
`base.drpc.org` both served that state) with the ignored test
`replay_logged_calldata` in `src/routes/swap/executable.rs`.

- The calldata took two orders: `0x725d6c5e…299f`, an auction-dca order owned
  by the selling wallet itself, then the market maker's `0xf6f1d7c5…3ce7`.
- The inner `takeOrders4` from RiverTaker and the site's outer
  `RiverTaker.take` from the wallet both revert with the same bytes:
  `Error("Min trade amount.")`.
- The search excludes `0x725d6c5e…299f` (it reverts alone), the other order
  executes alone, and `RiverTaker.take` with only that order executes.

The excluded order releases a budget of 3.230446 USDC linearly over 24 hours
and offers what has accrued as its maximum output; its handle-io refuses any
fill under 0.9 USDC. It was added at 15:17:47 UTC, so at 17:47 it offered
0.335 USDC: a quote it could not honour at any size. It offered the better
price then (0.0108897 wtSPYM per USDC against 0.0108964), so selection put it
first. Later the market maker's oracle price moved to the better side and the
order stopped being selected, which is why the same trade executed at 19:30.
Nothing here is specific to that order: the search found it by running it.

## Cost

Per quote with a taker: 1 extra `eth_call` when the take executes and the
token layout is cached. First quote per token and process: +2 probes. A
reverting single-order route: 1 call + 2 read-backs, then 1 call per further
round. Bounds are in "Per-order isolation". Simulation results are not cached:
they depend on taker, amount and block.

## What the website must change

1. Send `taker` on quote requests as the address that will call
   `takeOrders4`: the RiverTaker address when RiverTaker execution is enabled
   (the address already sent to `/v3/swap/calldata`), otherwise the connected
   wallet. Today quotes carry the wallet while calldata carries RiverTaker.
   With RiverTaker enabled the quote can be simulated before a wallet
   connects.
2. Add `takeSimulation` to `ApiSwapQuoteV3Response`.
3. `status === 'reverts'`: do not offer the Raindex route for this amount.
   `fullyFilled` is already `false`. `excludedOrders[].revertReason` and
   `revertData` may be logged or shown verbatim; do not branch on them.
4. `status === 'notSimulated'`: behave as today.
5. `status === 'executes'`: nothing to do; the estimates already leave out
   `excludedOrders`.
6. Keep the pre-send simulation of the final transaction.
