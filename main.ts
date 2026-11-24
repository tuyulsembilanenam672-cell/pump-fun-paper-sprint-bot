// pump-fun-paper-sprint-bot: a deterministic momentum bot for agent-fun/replay/v2.
//
// The harness says hello once, then sends one batch per slot that carries universe
// events, and expects exactly one reply per batch before the next batch arrives
// (lockstep). A batch holds the slot's events, the marks of the mints we hold or
// have orders pending on, the fills that resolved since the previous batch, and
// our account. Orders decided on the batch for slot s fill at slot s + latency.
//
// Strategy, in one breath: watch every fresh coin in the batch stream, buy a
// small fixed slot of it once it shows real momentum, and sell on a fixed
// take-profit, a fixed stop-loss, a trailing stop off the peak since entry,
// or a fixed hold time, whichever comes first. Diversify across many coins
// so the 20-coin round-trip bar is cleared with margin, and keep at most
// max_open_positions open at once.
//
// Everything here is deterministic: no clock, no randomness, no network, no
// environment reads. Every decision comes from the event stream plus hello.
// Two runs over the same window therefore produce byte-identical action logs.
//
// Notes on the words the build uses: hello opens the run, each batch is one
// slot of orders, and the orders in a reply are decided from that slot alone.

import { createInterface } from "node:readline";

// ---- fixed strategy parameters (tuned on the dev window) -----------------
// Dev-window lesson: coins pump fast after creation and then bleed, so the
// bot buys only the coins still rising near their own peak, takes profit
// quickly, and trails the peak down instead of waiting for a big move that
// never comes. Selectivity beats volume here: the most selective filter
// (near its peak, 8+ recorded buys, 3 SOL of real reserve) had the best win
// rate and the smallest loss of every variant tested on the dev window.
const BUY_LAMPORTS = 20_000_000n; // 0.02 SOL spent per coin
const MIN_BUY_LAMPORTS = 10_000_000n; // 0.01 SOL: the round-trip floor
const MIN_REAL_RESERVE_LAMPORTS = 3_000_000_000n; // need >= 3 SOL real reserve:
//   buys on one mint in one reply may spend at most 1% of its real SOL
//   reserve, so a 3 SOL reserve is what lets a ~0.027 SOL buy through.
const ENTRY_GAIN_BPS = 200; // buy once price is +2% over creation
const ENTRY_MAX_GAIN_BPS = 5_000; // ...but not if it already ran +50%
const ENTRY_MIN_BUYS = 8; // need at least 8 recorded buys first
const ENTRY_NEAR_PEAK_BPS = 9_500; // price must be within 5% of its peak
//   since creation: a coin that already dumped off its peak is not bought.
const MAX_ENTRY_AGE_SLOTS = 3_000; // only fresh coins, in slots
const TAKE_PROFIT_BPS = 1_500; // sell at +15% over the entry fill price
const STOP_LOSS_BPS = 1_000; // sell at -10% under the entry fill price
const TRAIL_ARM_BPS = 400; // trailing stop arms once price is +4% over entry
const TRAIL_DROP_BPS = 600; // ...then sell if price falls 6% under its peak
const MAX_HOLD_SLOTS = 400; // sell after this many slots whatever the price
const BUY_SLIPPAGE_BPS = 1_000; // 10% max slippage on buys
const SELL_SLIPPAGE_BPS = 1_000; // 10% max slippage on sells
const BUY_RETRY_COOLDOWN_SLOTS = 50; // wait this long after a failed buy
const MAX_BUY_ATTEMPTS = 3; // then give up on that coin

// ---- protocol types (subset of agent-fun/replay/v2) ---------------------
type ChainEvent = {
  event: string;
  mint?: string;
  slot: number;
  is_buy?: boolean;
  virtual_sol_reserves?: string;
  virtual_token_reserves?: string;
  virtual_quote_reserves?: string;
  real_quote_reserves?: string;
  is_mayhem_mode?: boolean;
  is_cashback_enabled?: boolean;
  quote_mint?: string;
  [k: string]: unknown;
};

type Mark = {
  mint: string;
  slot: number;
  virtual_token_reserves: string;
  virtual_quote_reserves: string;
  real_token_reserves: string;
  real_quote_reserves: string;
};

type Fill = {
  client_id: string;
  mint: string;
  side: string;
  status: string;
  reason: string | null;
  decided_slot: number;
  slot: number;
  quote_lamports: string;
  token_amount: string;
  fee_lamports: string;
  tx_fee_lamports: string;
  fee_fallback: boolean;
};

type Position = { mint: string; token_amount: string; cost_lamports: string };

type Batch = {
  type: "batch";
  seq: number;
  slot: number;
  events: ChainEvent[];
  marks: Mark[];
  fills: Fill[];
  account: { cash_lamports: string; positions: Position[] };
};

type Hello = {
  type: "hello";
  seed: string;
  capital: { lamports: string };
  caps: {
    max_open_positions: number;
    max_order_bps_of_real_reserve: number;
    max_orders_per_batch: number;
  };
  latency_slots: number;
  fees: {
    base_fee_lamports: string;
    priority_fee_lamports: string;
  };
};

type Order =
  | {
      side: "buy";
      client_id: string;
      mint: string;
      quote_lamports: string;
      max_slippage_bps: number;
    }
  | {
      side: "sell";
      client_id: string;
      mint: string;
      token_amount: string;
      max_slippage_bps: number;
    };

// ---- per-coin state ------------------------------------------------------
type Coin = {
  mint: string;
  createdSlot: number;
  p0: number; // creation price, SOL per token (float ratio)
  price: number; // last observed price
  peakSinceCreation: number; // highest price seen since creation
  buys: number; // recorded buy trades
  trades: number;
  realReserve: bigint; // last real_quote_reserves seen
  lastSlot: number;
  complete: boolean;
  traded: boolean; // already had a round trip (never re-enter)
  skipped: boolean; // buy attempts exhausted
  entryPrice: number; // fill price of the open position (SOL per token)
  entrySlot: number; // slot the buy filled
  peak: number; // highest price seen since the entry fill
  tokens: bigint; // tokens currently held
  pendingBuy: string | null; // client_id of a buy in flight
  pendingSell: string | null; // client_id of a sell in flight
  buyAttempts: number;
  cooldownUntil: number;
};

const coins = new Map<string, Coin>();
// client_id -> { mint, side } for orders awaiting a fill
const pending = new Map<string, { mint: string; side: "buy" | "sell" }>();
let orderCounter = 0;
let hello: Hello | null = null;
let buySizeLamports = BUY_LAMPORTS;
let stats = { batches: 0, buys: 0, sells: 0, fills: 0 };

function priceOf(vq: string | undefined, vt: string | undefined): number {
  if (vq === undefined || vt === undefined) return 0;
  const den = Number(vt);
  if (den <= 0) return 0;
  return Number(vq) / den;
}

function nextClientId(kind: string): string {
  orderCounter += 1;
  return `${kind}-${orderCounter}`;
}

// ---- fill handling -------------------------------------------------------
// Fills tell us what actually happened to our orders: a buy fill sets the
// entry price from what the curve really paid, a failed or rejected order
// frees its coin to be retried (after a cooldown), and a harness close
// (_complete_ / _flatten_ / _writeoff_) ends the position for us.
function applyFills(batch: Batch): void {
  for (const f of batch.fills) {
    stats.fills += 1;
    if (f.client_id.startsWith("_")) {
      // Harness close: position flattened or written off by the engine.
      const c = coins.get(f.mint);
      if (c) {
        c.tokens = 0n;
        c.pendingBuy = null;
        c.pendingSell = null;
        c.traded = true;
      }
      continue;
    }
    const p = pending.get(f.client_id);
    if (!p) continue;
    pending.delete(f.client_id);
    const c = coins.get(p.mint);
    if (!c) continue;
    if (p.side === "buy") {
      c.pendingBuy = null;
      if (f.status === "filled") {
        const tokens = BigInt(f.token_amount);
        const quote = BigInt(f.quote_lamports);
        c.tokens = tokens;
        c.entryPrice = tokens > 0n ? Number(quote) / Number(tokens) : 0;
        c.entrySlot = f.slot;
        c.peak = c.entryPrice;
        c.traded = true;
      } else {
        // rejected or failed: tx fees were paid on a failure, nothing on a
        // rejection; either way no position, retry after a cooldown.
        c.cooldownUntil = batch.slot + BUY_RETRY_COOLDOWN_SLOTS;
        if (f.status === "failed") c.buyAttempts += 1;
        if (c.buyAttempts >= MAX_BUY_ATTEMPTS) c.skipped = true;
      }
    } else {
      c.pendingSell = null;
      if (f.status === "filled") {
        c.tokens = 0n;
        c.traded = true;
      }
      // A failed or rejected sell leaves the position held; the next batch
      // will try the exit again.
    }
  }
}

// Reconcile holdings from the account the batch carries: it is authoritative
// for what we hold after the fills above.
function applyAccount(batch: Batch): void {
  const held = new Set<string>();
  for (const pos of batch.account.positions) {
    held.add(pos.mint);
    const c = coins.get(pos.mint);
    if (c) c.tokens = BigInt(pos.token_amount);
  }
  for (const c of coins.values()) {
    if (!held.has(c.mint) && c.pendingBuy === null && c.pendingSell === null) {
      c.tokens = 0n;
    }
  }
}

// ---- event handling ------------------------------------------------------
// TradeEvents carry post-trade reserves, so the price after each trade is
// exactly virtual_quote_reserves / virtual_token_reserves. CreateEvents give
// the creation price; CompleteEvents end a mint's tradable life.
function applyEvents(batch: Batch): void {
  for (const e of batch.events) {
    if (e.event === "CreateEvent") {
      const mint = e.mint;
      if (mint === undefined || coins.has(mint)) continue;
      if (e.is_mayhem_mode === true || e.is_cashback_enabled === true) continue;
      const p0 = priceOf(e.virtual_quote_reserves, e.virtual_token_reserves);
      if (p0 <= 0) continue;
      coins.set(mint, {
        mint,
        createdSlot: e.slot,
        p0,
        price: p0,
        peakSinceCreation: p0,
        buys: 0,
        trades: 0,
        realReserve: 0n,
        lastSlot: e.slot,
        complete: false,
        traded: false,
        skipped: false,
        entryPrice: 0,
        entrySlot: 0,
        peak: 0,
        tokens: 0n,
        pendingBuy: null,
        pendingSell: null,
        buyAttempts: 0,
        cooldownUntil: 0,
      });
    } else if (e.event === "TradeEvent") {
      const mint = e.mint;
      if (mint === undefined) continue;
      const c = coins.get(mint);
      if (!c || c.complete) continue;
      const p = priceOf(e.virtual_quote_reserves, e.virtual_token_reserves);
      if (p > 0) {
        c.price = p;
        if (p > c.peakSinceCreation) c.peakSinceCreation = p;
        // Track the peak while holding: the trailing stop sells when the
        // price falls this far under the best it reached since entry.
        if (c.tokens > 0n && p > c.peak) c.peak = p;
      }
      c.trades += 1;
      if (e.is_buy === true) c.buys += 1;
      if (e.real_quote_reserves !== undefined) {
        c.realReserve = BigInt(e.real_quote_reserves);
      }
      c.lastSlot = e.slot;
    } else if (e.event === "CompleteEvent") {
      const mint = e.mint;
      if (mint === undefined) continue;
      const c = coins.get(mint);
      if (c) c.complete = true;
    }
  }
}

// Drop coins that can no longer be entered or exited, so the map stays small
// over a long window: stale coins, completed coins with no position, coins
// already traded and flat.
function prune(batch: Batch): void {
  for (const [mint, c] of coins) {
    if (c.tokens > 0n || c.pendingBuy !== null || c.pendingSell !== null) continue;
    const stale =
      c.complete ||
      c.traded ||
      c.skipped ||
      batch.slot - c.createdSlot > MAX_ENTRY_AGE_SLOTS + MAX_HOLD_SLOTS;
    if (stale) coins.delete(mint);
  }
}

// ---- decisions -----------------------------------------------------------
// Exits first (never hold a loser past its stop), then entries while a
// position slot is free. Every order is sized against the caps hello gave:
// buys at most max_order_bps_of_real_reserve of the real reserve, and the
// reply never carries more than max_orders_per_batch orders.
function decide(batch: Batch): Order[] {
  const orders: Order[] = [];
  if (hello === null) return orders;
  const caps = hello.caps;
  const marks = new Map<string, number>();
  for (const m of batch.marks) {
    const p = priceOf(m.virtual_quote_reserves, m.virtual_token_reserves);
    if (p > 0) marks.set(m.mint, p);
  }
  const cash = BigInt(batch.account.cash_lamports);

  // Exits: take-profit, stop-loss, or hold-time expiry, whichever comes first.
  for (const c of coins.values()) {
    if (orders.length >= caps.max_orders_per_batch) break;
    if (c.tokens <= 0n || c.pendingSell !== null || c.complete) continue;
    if (c.entryPrice <= 0) continue;
    const p = marks.get(c.mint) ?? c.price;
    if (p <= 0) continue;
    if (p > c.peak) c.peak = p;
    const up = p >= (c.entryPrice * (10_000 + TAKE_PROFIT_BPS)) / 10_000;
    const down = p <= (c.entryPrice * (10_000 - STOP_LOSS_BPS)) / 10_000;
    const armed = p >= (c.entryPrice * (10_000 + TRAIL_ARM_BPS)) / 10_000;
    const trailed =
      armed && p <= (c.peak * (10_000 - TRAIL_DROP_BPS)) / 10_000;
    const timedOut = batch.slot - c.entrySlot >= MAX_HOLD_SLOTS;
    if (!up && !down && !trailed && !timedOut) continue;
    const id = nextClientId("s");
    c.pendingSell = id;
    pending.set(id, { mint: c.mint, side: "sell" });
    orders.push({
      side: "sell",
      client_id: id,
      mint: c.mint,
      token_amount: c.tokens.toString(),
      max_slippage_bps: SELL_SLIPPAGE_BPS,
    });
    stats.sells += 1;
  }

  // Entries: count open slots (held positions plus buys in flight).
  let open = 0;
  for (const c of coins.values()) {
    if (c.tokens > 0n || c.pendingBuy !== null) open += 1;
  }
  for (const c of coins.values()) {
    if (orders.length >= caps.max_orders_per_batch) break;
    if (open >= caps.max_open_positions) break;
    if (c.tokens > 0n || c.pendingBuy !== null) continue;
    if (c.complete || c.traded || c.skipped) continue;
    if (batch.slot - c.createdSlot > MAX_ENTRY_AGE_SLOTS) continue;
    if (batch.slot < c.cooldownUntil) continue;
    if (c.buys < ENTRY_MIN_BUYS) continue;
    const lo = (c.p0 * (10_000 + ENTRY_GAIN_BPS)) / 10_000;
    const hi = (c.p0 * (10_000 + ENTRY_MAX_GAIN_BPS)) / 10_000;
    if (c.price < lo || c.price > hi) continue;
    // Still rising: skip a coin that already dumped off its own peak.
    if (c.price * 10_000 < c.peakSinceCreation * ENTRY_NEAR_PEAK_BPS) continue;
    if (c.realReserve < MIN_REAL_RESERVE_LAMPORTS) continue;
    // 1% of the real reserve, with a safety margin under the cap.
    const capSize =
      (c.realReserve * BigInt(caps.max_order_bps_of_real_reserve - 10)) / 10_000n;
    let size = buySizeLamports < capSize ? buySizeLamports : capSize;
    if (size < MIN_BUY_LAMPORTS) continue;
    const txFee =
      BigInt(hello.fees.base_fee_lamports) + BigInt(hello.fees.priority_fee_lamports);
    if (cash < size + txFee + 1_000_000n) continue;
    const id = nextClientId("b");
    c.pendingBuy = id;
    c.buyAttempts += 1;
    pending.set(id, { mint: c.mint, side: "buy" });
    orders.push({
      side: "buy",
      client_id: id,
      mint: c.mint,
      quote_lamports: size.toString(),
      max_slippage_bps: BUY_SLIPPAGE_BPS,
    });
    stats.buys += 1;
    open += 1;
  }
  return orders;
}

// ---- main loop -----------------------------------------------------------
// One line in, one line out. Diagnostics go to stderr only: stdout carries
// nothing but replies, or the harness fails the run on a bad line.
async function main(): Promise<void> {
  const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });
  for await (const line of rl) {
    if (line.length === 0) continue;
    let msg: { type?: string };
    try {
      msg = JSON.parse(line) as { type?: string };
    } catch {
      continue;
    }
    if (msg.type === "hello") {
      hello = msg as unknown as Hello;
      const capital = BigInt(hello.capital.lamports);
      // Scale the fixed buy down if the capital is ever smaller than ours.
      const scaled = capital / 200n;
      buySizeLamports = scaled < BUY_LAMPORTS ? scaled : BUY_LAMPORTS;
      if (buySizeLamports < MIN_BUY_LAMPORTS) buySizeLamports = MIN_BUY_LAMPORTS;
      continue;
    }
    if (msg.type === "batch") {
      const batch = msg as unknown as Batch;
      stats.batches += 1;
      applyFills(batch);
      applyAccount(batch);
      applyEvents(batch);
      const orders = decide(batch);
      prune(batch);
      process.stdout.write(
        JSON.stringify({ type: "reply", seq: batch.seq, orders }) + "\n",
      );
      continue;
    }
    if (msg.type === "done") {
      const done = msg as { account?: { cash_lamports?: string } };
      const cash = done.account?.cash_lamports ?? "?";
      process.stderr.write(
        `done: batches=${stats.batches} buys=${stats.buys} sells=${stats.sells} fills=${stats.fills} cash=${cash}\n`,
      );
      break;
    }
  }
}

main().catch((err: unknown) => {
  process.stderr.write(`fatal: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(1);
});
