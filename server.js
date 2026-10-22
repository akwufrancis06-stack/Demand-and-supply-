'use strict';

const path = require('path');
const express = require('express');
const WebSocket = require('ws');

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);
const DERIV_WS_URL = 'wss://api.derivws.com/trading/v1/options/ws/public';
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';
const TWELVE_DATA_API_KEY = process.env.TWELVE_DATA_API_KEY || '';
// Optional. If set, the dashboard and /api/status require this token.
const DASHBOARD_TOKEN = process.env.DASHBOARD_TOKEN || '';

const SCAN_INTERVAL_MS = Math.max(60_000, Number(process.env.SCAN_INTERVAL_MS || 300_000));
const REQUEST_TIMEOUT_MS = 25_000;
const REQUEST_GAP_MS = Math.max(100, Number(process.env.REQUEST_GAP_MS || 250));
const TICK_SUBSCRIBE_GAP_MS = 200;
// Twelve Data free plan allows 8 requests per minute. 8 seconds between requests stays under that.
const TD_GAP_MS = 8000;
const CONFIRMATION_MAX_AGE_MS = 30 * 60 * 1000;
const STOCKS = ['AAPL', 'TSLA', 'NVDA', 'AMZN']; // display only; not scanned
const RR = 2;
const ATR_PERIOD = 14;

// Bars downloaded per timeframe. 12H needs about 500 hourly bars;
// 1W needs about 100 weeks of daily bars.
const HISTORY_COUNT = 500;

/* =========================================================
   TIMEFRAMES
========================================================= */

// Timeframes fetched directly from the API.
// seconds: Deriv granularity. tdInterval: Twelve Data interval name.
// maxAgeBars: how many bars old a structure break can be and still make a zone.
const ZONE_TIMEFRAMES = [
  { label: '5M',  seconds: 300,   tdInterval: '5min',  maxAgeBars: 288 }, // about 1 day
  { label: '15M', seconds: 900,   tdInterval: '15min', maxAgeBars: 120 }, // about 30 hours
  { label: '30M', seconds: 1800,  tdInterval: '30min', maxAgeBars: 96 },  // about 2 days
  { label: '1H',  seconds: 3600,  tdInterval: '1h',    maxAgeBars: 120 }, // about 5 days
  { label: '2H',  seconds: 7200,  tdInterval: '2h',    maxAgeBars: 60 },  // about 5 days
  { label: '4H',  seconds: 14400, tdInterval: '4h',    maxAgeBars: 60 },  // about 10 days
  { label: '1D',  seconds: 86400, tdInterval: '1day',  maxAgeBars: 30 }   // about 30 days
];

// Built from other candles, so not fetched directly.
const TF_12H = { label: '12H', maxAgeBars: 40 }; // from 1H candles, about 20 days
const TF_1W = { label: '1W', maxAgeBars: 12 };   // from 1D candles, about 3 months

// The 5M timeframe is also used for rejection confirmation.
const TF_5M = ZONE_TIMEFRAMES[0];

/* =========================================================
   ZONE SETTINGS
========================================================= */

const ZONE_SETTINGS = {
  PIVOT_LEFT: 3,               // bars on the left of a swing point
  PIVOT_RIGHT: 3,              // bars on the right; a swing is only confirmed after these close
  BREAK_LOOKBACK: 8,           // bars searched backwards for the last opposing candle
  DISPLACEMENT_BARS: 5,        // bars after the break used to measure the move away
  MIN_DISPLACEMENT_ATR: 2.5,   // the move away from the zone must be at least this many ATRs
  MAX_ZONE_HEIGHT_ATR: 2.0,    // wider zones are too loose to be useful
  MAX_ZONES_PER_TF: 3,         // keeps slower timeframes from being crowded out
  MAX_ZONES_PER_PAIR: 27       // 3 per timeframe across 9 timeframes
};

/* =========================================================
   MARKETS
========================================================= */

// Forex and gold: resolved from Deriv's active symbols.
const DERIV_PAIRS = [
  // Forex majors and crosses
  'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'USDCAD',
  'AUDUSD', 'NZDUSD', 'EURGBP', 'EURJPY', 'EURCHF',
  'EURCAD', 'EURAUD', 'EURNZD', 'GBPJPY', 'GBPCHF',
  'GBPCAD', 'GBPAUD', 'GBPNZD', 'AUDJPY', 'AUDCHF',
  'AUDCAD', 'AUDNZD', 'NZDJPY', 'NZDCHF', 'NZDCAD',
  'CADJPY', 'CADCHF', 'CHFJPY',

  // Additional forex
  'USDSGD', 'USDHKD', 'EURSEK', 'EURNOK', 'EURPLN',
  'EURTRY', 'GBPSEK', 'GBPNOK', 'GBPSGD', 'CADSGD',
  'CHFSGD',

  // Gold
  'XAUUSD'
];

// Crypto: candles from Twelve Data. Key is the bot's pair name, value is Twelve Data's symbol.
const TWELVE_PAIRS = {
  BTCUSD: 'BTC/USD',
  ETHUSD: 'ETH/USD',
  SOLUSD: 'SOL/USD'
};

const TARGET_PAIRS = [...DERIV_PAIRS, ...Object.keys(TWELVE_PAIRS)];

// Crypto (Twelve Data) limits to stay within the free daily credit limit.
// 5M is always fetched for crypto because rejection confirmation needs it.
const CRYPTO_TIMEFRAMES = ['15M', '1H', '4H', '1D'];
// Crypto is scanned on every 6th cycle: 30 minutes at the default 5-minute interval.
const CRYPTO_SCAN_EVERY_N = 6;

/* =========================================================
   STATE
========================================================= */

const state = {
  startedAt: new Date().toISOString(),
  connected: false,
  lastScan: null,
  lastError: '',
  activeSymbolsCount: 0,
  pairs: {},
  zones: [],
  signals: [],
  pendingConfirmations: [],
  logs: []
};

let scanning = false;
let scanCount = 0;

function log(message) {
  const entry = `${new Date().toISOString()} ${message}`;
  console.log(entry);
  state.logs.unshift(entry);
  state.logs = state.logs.slice(0, 80);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/* =========================================================
   PRICE FORMATTING
========================================================= */

function prettyPair(pair) {
  if (STOCKS.includes(pair)) return pair;
  return pair.length === 6 ? `${pair.slice(0, 3)}/${pair.slice(3)}` : pair;
}

function priceDigits(pair, price) {
  if (['XAUUSD', 'BTCUSD', 'ETHUSD', 'SOLUSD'].includes(pair)) return 2;
  if (pair.endsWith('JPY')) return 3;
  if (price >= 100) return 3;
  if (price >= 10) return 4;
  return 5;
}

function roundPrice(pair, value) {
  return Number(Number(value).toFixed(priceDigits(pair, Number(value))));
}

function fmtPrice(pair, value) {
  return Number(value).toFixed(priceDigits(pair, Number(value)));
}

/* =========================================================
   INDICATORS
========================================================= */

function calcATR(candles, period = ATR_PERIOD) {
  if (!Array.isArray(candles) || candles.length < period + 2) return null;
  const trs = [];
  for (let i = 1; i < candles.length; i++) {
    const c = candles[i];
    const prev = candles[i - 1];
    trs.push(Math.max(
      c.high - c.low,
      Math.abs(c.high - prev.close),
      Math.abs(c.low - prev.close)
    ));
  }
  const slice = trs.slice(-period);
  return slice.reduce((sum, x) => sum + x, 0) / slice.length;
}

/* =========================================================
   BUILT TIMEFRAMES (12H and 1W)
========================================================= */

// Builds 12H candles from 1H candles, using 00:00–12:00 and 12:00–24:00 UTC blocks.
// A block is only used if all 12 hourly candles are present and closed.
function build12H(candles1H) {
  // Drop the 1H candle that is still forming, so a block is never built from partial hours.
  const closed1H = Array.isArray(candles1H) ? candles1H.slice(0, -1) : [];
  if (closed1H.length < 12) return [];

  const groups = new Map();
  for (const c of closed1H) {
    const d = new Date(c.epoch * 1000);
    const half = d.getUTCHours() < 12 ? 0 : 1;
    const key = `${d.getUTCFullYear()}-${d.getUTCMonth()}-${d.getUTCDate()}-${half}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(c);
  }

  const result = [];
  for (const group of groups.values()) {
    if (group.length !== 12) continue;
    group.sort((a, b) => a.epoch - b.epoch);
    result.push({
      epoch: group[0].epoch,
      open: group[0].open,
      high: Math.max(...group.map(c => c.high)),
      low: Math.min(...group.map(c => c.low)),
      close: group[group.length - 1].close
    });
  }

  return result.sort((a, b) => a.epoch - b.epoch);
}

// Start of the Monday (UTC) for the week containing this time, in milliseconds.
function weekStartMs(ms) {
  const d = new Date(ms);
  const daysSinceMonday = (d.getUTCDay() + 6) % 7;
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() - daysSinceMonday);
}

// Builds weekly candles from daily candles. Weeks run Monday to Friday (UTC).
// The current week is skipped because it is still forming. A week needs at
// least 4 daily candles, so a holiday week can still count.
function build1W(candles1D) {
  if (!Array.isArray(candles1D) || candles1D.length < 4) return [];

  const currentWeek = weekStartMs(Date.now());
  const groups = new Map();

  for (const c of candles1D) {
    const key = weekStartMs(c.epoch * 1000);
    if (key === currentWeek) continue;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(c);
  }

  const result = [];
  for (const group of groups.values()) {
    if (group.length < 4) continue;
    group.sort((a, b) => a.epoch - b.epoch);
    result.push({
      epoch: group[0].epoch,
      open: group[0].open,
      high: Math.max(...group.map(c => c.high)),
      low: Math.min(...group.map(c => c.low)),
      close: group[group.length - 1].close
    });
  }

  return result.sort((a, b) => a.epoch - b.epoch);
}

/* =========================================================
   ZONE DETECTION
========================================================= */

// Finds closes beyond the last confirmed swing high (bullish break) or
// swing low (bearish break). Only uses bars that had closed by that point.
function findStructureBreaks(closed) {
  const L = ZONE_SETTINGS.PIVOT_LEFT;
  const R = ZONE_SETTINGS.PIVOT_RIGHT;
  const breaks = [];

  let lastHigh = null;
  let lastLow = null;
  let highBroken = true;
  let lowBroken = true;

  for (let i = L + R; i < closed.length; i++) {
    // The pivot at p = i - R is confirmed now, because R bars have closed after it.
    const p = i - R;
    let isHigh = true;
    let isLow = true;
    for (let j = p - L; j <= p + R; j++) {
      if (j === p) continue;
      if (closed[j].high >= closed[p].high) isHigh = false;
      if (closed[j].low <= closed[p].low) isLow = false;
    }
    if (isHigh) { lastHigh = { price: closed[p].high }; highBroken = false; }
    if (isLow) { lastLow = { price: closed[p].low }; lowBroken = false; }

    const c = closed[i];
    if (lastHigh && !highBroken && c.close > lastHigh.price) {
      breaks.push({ direction: 'BUY', index: i });
      highBroken = true;
    }
    if (lastLow && !lowBroken && c.close < lastLow.price) {
      breaks.push({ direction: 'SELL', index: i });
      lowBroken = true;
    }
  }

  return breaks;
}

// Builds zones from structure breaks. Each zone is the last opposing candle
// before the break, kept only if price then moved strongly away from it.
// lastIsForming: true when the final candle is still open and must be dropped.
function makeZones(pair, tf, candles, lastIsForming = true) {
  const closed = lastIsForming ? candles.slice(0, -1) : candles;
  if (closed.length < 40) return [];

  const atr = calcATR(closed);
  if (!atr) return [];

  const lastIdx = closed.length - 1;
  const zones = [];

  for (const b of findStructureBreaks(closed)) {
    if (lastIdx - b.index > tf.maxAgeBars) continue;

    const isBuy = b.direction === 'BUY';

    // Last opposing candle before the break.
    let srcIdx = -1;
    for (let j = b.index - 1; j >= Math.max(0, b.index - ZONE_SETTINGS.BREAK_LOOKBACK); j--) {
      const c = closed[j];
      const opposing = isBuy ? c.close < c.open : c.close > c.open;
      if (opposing) { srcIdx = j; break; }
    }
    if (srcIdx < 0) continue;

    const source = closed[srcIdx];
    const top = Math.max(source.open, source.close);
    const bottom = Math.min(source.open, source.close);
    const height = top - bottom;
    if (height < atr * 0.1 || height > atr * ZONE_SETTINGS.MAX_ZONE_HEIGHT_ATR) continue;

    // Strong move away: how far price travelled from the zone after the break.
    const end = Math.min(lastIdx, b.index + ZONE_SETTINGS.DISPLACEMENT_BARS);
    const windowBars = closed.slice(b.index, end + 1);
    const move = isBuy
      ? Math.max(...windowBars.map(c => c.high)) - bottom
      : top - Math.min(...windowBars.map(c => c.low));
    const displacement = move / atr;
    if (displacement < ZONE_SETTINGS.MIN_DISPLACEMENT_ATR) continue;

    // Invalidated if a later candle closed through the far side of the zone.
    const later = closed.slice(srcIdx + 1);
    const invalidated = later.some(c => isBuy ? c.close < bottom : c.close > top);
    if (invalidated) continue;

    zones.push({
      id: `${pair}-${tf.label}-${isBuy ? 'D' : 'S'}-${source.epoch}`,
      pair,
      timeframe: tf.label,
      direction: isBuy ? 'BUY' : 'SELL',
      top,
      bottom,
      createdAt: source.epoch,
      breakEpoch: closed[b.index].epoch,
      displacementATR: Number(displacement.toFixed(2)),
      score: Number(displacement.toFixed(2)),
      tested: false
    });
  }

  return zones;
}

// Keeps the strongest zones, limited per timeframe, and drops any zone that
// overlaps a stronger zone in the same direction.
function pruneZones(zones) {
  const sorted = [...zones].sort((a, b) => b.score - a.score);
  const kept = [];
  const countByTf = {};

  for (const z of sorted) {
    if ((countByTf[z.timeframe] || 0) >= ZONE_SETTINGS.MAX_ZONES_PER_TF) continue;

    const overlapsStronger = kept.some(k =>
      k.direction === z.direction &&
      z.bottom <= k.top &&
      z.top >= k.bottom
    );
    if (overlapsStronger) continue;

    kept.push(z);
    countByTf[z.timeframe] = (countByTf[z.timeframe] || 0) + 1;
  }

  return kept.slice(0, ZONE_SETTINGS.MAX_ZONES_PER_PAIR);
}

/* =========================================================
   DERIV SOCKET
========================================================= */

class DerivSocket {
  constructor() {
    this.ws = null;
    this.reqId = 0;
    this.pending = new Map();
    this.connecting = null;
    this.reconnectTimer = null;
    this.tickSymbols = [];
  }

  connect() {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) return Promise.resolve();
    if (this.connecting) return this.connecting;

    this.connecting = new Promise((resolve, reject) => {
      const ws = new WebSocket(DERIV_WS_URL);
      let settled = false;

      const timeout = setTimeout(() => {
        if (!settled) {
          settled = true;
          try { ws.terminate(); } catch (_) {}
          reject(new Error('Deriv WebSocket connection timed out'));
        }
      }, 15000);

      ws.on('open', () => {
        settled = true;
        clearTimeout(timeout);
        this.ws = ws;
        state.connected = true;
        state.lastError = '';
        log('Connected to Deriv public market-data WebSocket.');
        resolve();
        // Ticks are per-connection, so restore them after every reconnect.
        if (this.tickSymbols.length) this.subscribeTicks(this.tickSymbols);
      });

      ws.on('message', raw => {
        let data;
        try { data = JSON.parse(raw.toString()); } catch (_) { return; }

        if (data.req_id && this.pending.has(data.req_id)) {
          const pending = this.pending.get(data.req_id);
          this.pending.delete(data.req_id);
          clearTimeout(pending.timeout);
          if (data.error) pending.reject(new Error(data.error.message || 'Deriv API error'));
          else pending.resolve(data);
        }

        if (data.msg_type === 'tick' && data.tick) {
          this.handleTick(data.tick);
        }
      });

      ws.on('error', err => {
        state.lastError = `WebSocket: ${err.message}`;
        log(state.lastError);
        if (!settled) {
          settled = true;
          clearTimeout(timeout);
          reject(err);
        }
      });

      ws.on('close', () => {
        clearTimeout(timeout);
        if (!settled) {
          settled = true;
          reject(new Error('Deriv WebSocket closed before opening'));
        }
        // Only handle the close of the live socket. A failed attempt is retried by its caller.
        if (this.ws !== ws) return;

        state.connected = false;
        this.ws = null;
        for (const [id, pending] of this.pending) {
          clearTimeout(pending.timeout);
          pending.reject(new Error('Deriv WebSocket closed'));
          this.pending.delete(id);
        }
        log('Deriv WebSocket closed; reconnect scheduled.');
        this.scheduleReconnect();
      });
    }).finally(() => { this.connecting = null; });

    return this.connecting;
  }

  scheduleReconnect() {
    if (this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect().catch(err => {
        state.lastError = err.message;
        log(`Reconnect failed: ${err.message}`);
        this.scheduleReconnect();
      });
    }, 5000);
  }

  async request(payload) {
    await this.connect();
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new Error('Deriv socket is not open');
    }
    const req_id = ++this.reqId;
    const message = { ...payload, req_id };
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(req_id);
        reject(new Error(`Deriv request timed out: ${Object.keys(payload)[0]}`));
      }, REQUEST_TIMEOUT_MS);
      this.pending.set(req_id, { resolve, reject, timeout });
      this.ws.send(JSON.stringify(message), err => {
        if (err) {
          clearTimeout(timeout);
          this.pending.delete(req_id);
          reject(err);
        }
      });
    });
  }

  async history(symbol, granularity) {
    const data = await this.request({
      ticks_history: symbol,
      adjust_start_time: 1,
      count: HISTORY_COUNT,
      end: 'latest',
      granularity,
      style: 'candles'
    });
    return (data.candles || []).map(c => ({
      epoch: Number(c.epoch),
      open: Number(c.open),
      high: Number(c.high),
      low: Number(c.low),
      close: Number(c.close)
    })).filter(c => [c.epoch, c.open, c.high, c.low, c.close].every(Number.isFinite));
  }

  handleTick(tick) {
    const symbol = tick.symbol;
    const quote = Number(tick.quote);
    if (!symbol || !Number.isFinite(quote)) return;

    for (const pair of Object.keys(state.pairs)) {
      const p = state.pairs[pair];
      if (p.symbol !== symbol) continue;
      p.price = quote;
      p.priceTime = Number(tick.epoch || Math.floor(Date.now() / 1000));
      p.priceSource = 'tick';
      checkZoneTouches(pair, quote);
      break;
    }
  }

  // Subscribes one market at a time. A closed or invalid market only affects itself.
  subscribeTicks(symbols) {
    this.tickSymbols = [...symbols];
    if (!symbols.length) return;

    (async () => {
      let ok = 0, closed = 0, invalid = 0, other = 0;

      for (const symbol of symbols) {
        try {
          await this.request({ ticks: symbol, subscribe: 1 });
          ok++;
        } catch (e) {
          if (/presently closed/i.test(e.message)) closed++;
          else if (/invalid symbol/i.test(e.message)) {
            invalid++;
            log(`Invalid tick symbol sent: "${symbol}"`);
          } else {
            other++;
            log(`Tick error for "${symbol}": ${e.message}`);
          }
        }
        await sleep(TICK_SUBSCRIBE_GAP_MS);
      }

      log(`Live ticks: ${ok} subscribed, ${closed} closed, ${invalid} invalid, ${other} other.`);
    })();
  }
}

const deriv = new DerivSocket();

/* =========================================================
   TWELVE DATA (crypto candles)
========================================================= */

// Serialises Twelve Data requests so they never exceed the per-minute limit,
// even when several scan workers run at once.
let tdChain = Promise.resolve();
let tdLastCall = 0;

function tdRequest(url) {
  const run = tdChain.then(async () => {
    const wait = TD_GAP_MS - (Date.now() - tdLastCall);
    if (wait > 0) await sleep(wait);
    tdLastCall = Date.now();
    const res = await fetch(url);
    return res.json();
  });
  tdChain = run.catch(() => {});
  return run;
}

async function twelveHistory(tdSymbol, interval) {
  if (!TWELVE_DATA_API_KEY) throw new Error('TWELVE_DATA_API_KEY is not set');

  const params = new URLSearchParams({
    symbol: tdSymbol,
    interval,
    outputsize: String(HISTORY_COUNT),
    timezone: 'UTC',
    apikey: TWELVE_DATA_API_KEY
  });

  const data = await tdRequest(`https://api.twelvedata.com/time_series?${params}`);

  if (data.status === 'error' || !Array.isArray(data.values)) {
    throw new Error(`Twelve Data: ${data.message || 'no values returned'}`);
  }

  // Twelve Data returns newest first. The bot expects oldest first.
  return data.values
    .map(v => ({
      epoch: Math.floor(Date.parse(String(v.datetime).replace(' ', 'T') + 'Z') / 1000),
      open: Number(v.open),
      high: Number(v.high),
      low: Number(v.low),
      close: Number(v.close)
    }))
    .filter(c => [c.epoch, c.open, c.high, c.low, c.close].every(Number.isFinite))
    .reverse();
}

// Returns candles for a pair and timeframe, from whichever source the pair uses.
async function getCandles(p, tf) {
  if (p.source === 'twelvedata') {
    return twelveHistory(p.symbol, tf.tdInterval);
  }
  return deriv.history(p.symbol, tf.seconds);
}

/* =========================================================
   TELEGRAM
========================================================= */

async function sendTelegram(text) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    log('Telegram not configured; signal logged but not sent.');
    return false;
  }
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      chat_id: TELEGRAM_CHAT_ID,
      text,
      disable_web_page_preview: true
    })
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok || !body.ok) throw new Error(body.description || `Telegram HTTP ${response.status}`);
  return true;
}

/* =========================================================
   ZONE TOUCH (immediate warning)
========================================================= */

// Alerts once per zone, on its first touch. Overlapping zones touched together
// produce a single alert for the strongest one.
function checkZoneTouches(pair, price) {
  const pairState = state.pairs[pair];
  if (!pairState || !Number.isFinite(price)) return;

  const touched = state.zones.filter(z =>
    z.pair === pair && !z.tested && price >= z.bottom && price <= z.top
  );
  if (!touched.length) return;

  // Every touched zone is now tested, so none of them can alert again.
  for (const z of touched) z.tested = true;
  touched.sort((a, b) => b.score - a.score);
  const zone = touched[0];

  // Do not stack a second warning while one is still waiting for its 5M confirmation.
  if (state.pendingConfirmations.some(item =>
    item.pair === pair && item.direction === zone.direction && !item.completed
  )) return;

  const isBuy = zone.direction === 'BUY';
  const atr = pairState.atr5m || (zone.top - zone.bottom) || price * 0.0005;
  const buffer = Math.max(atr * 0.15, price * 0.00005);
  const sl = roundPrice(pair, isBuy ? zone.bottom - buffer : zone.top + buffer);
  const entry = roundPrice(pair, price);
  const risk = Math.abs(entry - sl);
  if (!(risk > 0) || (isBuy ? entry <= sl : entry >= sl)) return;
  const tp = roundPrice(pair, isBuy ? entry + RR * risk : entry - RR * risk);

  const now = Date.now();
  const pending = {
    id: `${now}-${pair}-${zone.direction}-touch`,
    zoneId: zone.id,
    pair,
    direction: zone.direction,
    timeframe: zone.timeframe,
    score: zone.score,
    touchedAt: now,
    zoneTop: zone.top,
    zoneBottom: zone.bottom,
    sl,
    tp,
    completed: false
  };
  state.pendingConfirmations.push(pending);
  state.pendingConfirmations = state.pendingConfirmations
    .filter(item => now - item.touchedAt < CONFIRMATION_MAX_AGE_MS * 2)
    .slice(-200);

  state.signals.unshift({
    id: pending.id,
    time: new Date(now).toISOString(),
    pair,
    direction: zone.direction,
    timeframe: zone.timeframe,
    type: 'ZONE_TOUCH',
    entry,
    sl,
    tp,
    zoneTop: zone.top,
    zoneBottom: zone.bottom,
    note: 'First touch of a zone. Wait for a separate 5M rejection confirmation.'
  });
  state.signals = state.signals.slice(0, 100);

  const source = pairState.source === 'twelvedata' ? 'Twelve Data' : 'Deriv';
  const message = [
    `🔔 ZONE TOUCH — ${zone.direction} ${prettyPair(pair)}`,
    `Zone: ${zone.timeframe} ${isBuy ? 'DEMAND' : 'SUPPLY'} (strength ${zone.score} ATR)`,
    `Zone range: ${fmtPrice(pair, zone.bottom)} – ${fmtPrice(pair, zone.top)}`,
    `Price at touch: ${fmtPrice(pair, entry)}`,
    `Reference SL: ${fmtPrice(pair, sl)}`,
    `Reference TP (1:${RR}): ${fmtPrice(pair, tp)}`,
    touched.length > 1 ? `Also touched: ${touched.length - 1} overlapping zone(s)` : null,
    `Next: waiting for a completed 5M rejection candle.`,
    `Price source: ${source}`,
    `⚠️ Warning only; this is not yet a confirmed entry.`
  ].filter(Boolean).join('\n');

  sendTelegram(message)
    .then(ok => log(`${ok ? 'Telegram sent' : 'Signal recorded'}: ZONE TOUCH ${zone.direction} ${pair}`))
    .catch(err => {
      log(`Telegram send failed: ${err.message}`);
      state.lastError = `Telegram: ${err.message}`;
    });
}

/* =========================================================
   5M REJECTION CONFIRMATION
========================================================= */

// Checks every closed 5M candle since the touch. Each pending touch fires at most once.
function confirmRejections(pair, candles) {
  if (!Array.isArray(candles) || candles.length < 3) return;
  const now = Date.now();
  const closed = candles.slice(0, -1); // the last candle may still be forming

  for (const pending of state.pendingConfirmations) {
    if (pending.completed || pending.pair !== pair) continue;
    if (now - pending.touchedAt > CONFIRMATION_MAX_AGE_MS) {
      pending.completed = true;
      continue;
    }

    for (const candle of closed) {
      const candleEnd = (candle.epoch + 300) * 1000;
      if (candleEnd <= pending.touchedAt) continue; // closed before the touch

      const overlapsZone = candle.low <= pending.zoneTop && candle.high >= pending.zoneBottom;
      if (!overlapsZone) continue;

      const range = Math.max(candle.high - candle.low, 1e-12);
      const lowerWick = Math.min(candle.open, candle.close) - candle.low;
      const upperWick = candle.high - Math.max(candle.open, candle.close);
      const bullishReject = pending.direction === 'BUY' && candle.close > candle.open && lowerWick / range >= 0.25 && candle.close >= pending.zoneBottom;
      const bearishReject = pending.direction === 'SELL' && candle.close < candle.open && upperWick / range >= 0.25 && candle.close <= pending.zoneTop;
      if (!bullishReject && !bearishReject) continue;

      const entry = roundPrice(pair, candle.close);
      const risk = pending.direction === 'BUY' ? entry - pending.sl : pending.sl - entry;
      if (risk <= 0) {
        log(`Confirmation skipped: invalid risk for ${pair}`);
        continue;
      }
      const tp = roundPrice(pair, pending.direction === 'BUY' ? entry + RR * risk : entry - RR * risk);

      pending.completed = true;

      state.signals.unshift({
        id: `${pending.id}-confirmed`,
        time: new Date().toISOString(),
        pair,
        direction: pending.direction,
        timeframe: '5M',
        type: 'CONFIRMED_ENTRY',
        entry,
        sl: pending.sl,
        tp,
        zoneTop: pending.zoneTop,
        zoneBottom: pending.zoneBottom,
        note: 'Completed 5M rejection candle detected. Confirm execution conditions.'
      });
      state.signals = state.signals.slice(0, 100);

      const source = state.pairs[pair] && state.pairs[pair].source === 'twelvedata' ? 'Twelve Data' : 'Deriv';
      const message = [
        `✅ 5M REJECTION CONFIRMED — ${pending.direction} ${prettyPair(pair)}`,
        `Zone: ${pending.timeframe} (strength ${pending.score} ATR)`,
        `Reference entry (5M close): ${fmtPrice(pair, entry)}`,
        `Stop Loss: ${fmtPrice(pair, pending.sl)}`,
        `Take Profit (1:${RR}): ${fmtPrice(pair, tp)}`,
        `Zone range: ${fmtPrice(pair, pending.zoneBottom)} – ${fmtPrice(pair, pending.zoneTop)}`,
        `Confirmation candle: ${new Date(candle.epoch * 1000).toISOString()}`,
        `Price source: ${source}`,
        `⚠️ Confirmation is rule-based, not a guarantee of reversal.`
      ].join('\n');

      sendTelegram(message)
        .then(ok => log(`${ok ? 'Telegram sent' : 'Signal recorded'}: CONFIRMED ${pending.direction} ${pair}`))
        .catch(err => {
          log(`Telegram send failed: ${err.message}`);
          state.lastError = `Telegram: ${err.message}`;
        });
      break; // one confirmation per pending touch
    }
  }
}

/* =========================================================
   SYMBOL RESOLUTION
========================================================= */

async function resolveSymbols() {
  // Crypto pairs use Twelve Data and need no Deriv lookup.
  for (const [pair, tdSymbol] of Object.entries(TWELVE_PAIRS)) {
    state.pairs[pair] = {
      pair,
      display: prettyPair(pair),
      source: 'twelvedata',
      symbol: tdSymbol,
      status: TWELVE_DATA_API_KEY ? 'Waiting for first scan' : 'Missing TWELVE_DATA_API_KEY',
      price: null,
      priceTime: null,
      priceSource: null,
      lastScan: null,
      error: ''
    };
  }

  // Forex and gold use Deriv.
  const response = await deriv.request({ active_symbols: 'brief' });
  const symbols = response.active_symbols || [];
  state.activeSymbolsCount = symbols.length;
  log(`Deriv returned ${symbols.length} active symbols.`);

  const available = new Map();
  const normalize = value => String(value || '').replace(/[^A-Z0-9]/gi, '').toUpperCase();

  for (const item of symbols) {
    // Keep the original case. Deriv symbols are case-sensitive (e.g. frxUSDCAD).
    const symbol = String(item.symbol || item.underlying_symbol || '');
    const display = String(
      item.display_name || item.underlying_symbol_name || item.name || ''
    ).toUpperCase();
    const submarket = String(item.submarket || '').toLowerCase();

    // Skip OTC markets.
    if (`${symbol} ${display}`.toUpperCase().includes('OTC') || submarket.includes('otc')) continue;

    // Normalise first (removes non-letters and uppercases), then strip the FRX prefix.
    const symbolKey = normalize(symbol).replace(/^FRX/, '');
    const displayKey = normalize(display);

    for (const target of DERIV_PAIRS) {
      if (symbolKey === target || displayKey === target) {
        if (!available.has(target)) available.set(target, symbol);
      }
    }
  }

  for (const pair of DERIV_PAIRS) {
    const resolvedSymbol = available.get(pair);
    state.pairs[pair] = {
      pair,
      display: prettyPair(pair),
      source: 'deriv',
      symbol: resolvedSymbol || null,
      status: resolvedSymbol ? 'Waiting for first scan' : 'Unavailable on Deriv',
      price: null,
      priceTime: null,
      priceSource: null,
      lastScan: null,
      error: ''
    };
  }

  const resolvedDeriv = DERIV_PAIRS.filter(pair => state.pairs[pair].symbol);
  const missing = DERIV_PAIRS.filter(pair => !state.pairs[pair].symbol);

  log(`Deriv resolved ${resolvedDeriv.length}/${DERIV_PAIRS.length} forex/gold markets.`);
  if (missing.length) {
    log(`Unavailable on Deriv: ${missing.map(prettyPair).join(', ')}`);
  }

  if (resolvedDeriv.length) {
    deriv.subscribeTicks(resolvedDeriv.map(pair => state.pairs[pair].symbol));
  }
}

/* =========================================================
   SCANNING
========================================================= */

async function scanPair(pair) {
  const p = state.pairs[pair];
  if (!p || !p.symbol) return;

  if (p.source === 'twelvedata' && !TWELVE_DATA_API_KEY) {
    p.status = 'Missing TWELVE_DATA_API_KEY';
    return;
  }

  try {
    // 1. Fetch candles for every timeframe. The 5M set is also used for confirmation.
    const newZones = [];
    const candlesByLabel = {};
    let candles5m = null;

    for (const tf of ZONE_TIMEFRAMES) {
      const isCrypto = p.source === 'twelvedata';
      const needed = !isCrypto || tf === TF_5M || CRYPTO_TIMEFRAMES.includes(tf.label);
      if (!needed) continue;

      const candles = await getCandles(p, tf);
      await sleep(REQUEST_GAP_MS);
      candlesByLabel[tf.label] = candles;
      if (tf === TF_5M) candles5m = candles;
      newZones.push(...makeZones(pair, tf, candles));
    }

    // 2. Built timeframes. Both are made only from closed candles, so none is treated as forming.
    const candles12h = build12H(candlesByLabel['1H']);
    newZones.push(...makeZones(pair, TF_12H, candles12h, false));

    const candles1w = build1W(candlesByLabel['1D']);
    newZones.push(...makeZones(pair, TF_1W, candles1w, false));

    if (!candles5m || candles5m.length < 3) throw new Error('Not enough 5M candles');

    const last5 = candles5m[candles5m.length - 1];
    const atr5m = calcATR(candles5m.slice(0, -1));
    if (atr5m) p.atr5m = atr5m;
    confirmRejections(pair, candles5m);

    // 3. Decide which price to use for touch checks.
    const nowSec = Date.now() / 1000;
    const hasFreshTick = p.priceSource === 'tick' && p.priceTime && nowSec - p.priceTime < 600;
    const refPrice = hasFreshTick ? p.price : last5.close;

    // 4. Keep the strongest zones across all timeframes, then carry over tested state.
    const previous = new Map(
      state.zones.filter(z => z.pair === pair).map(z => [z.id, z])
    );
    const pruned = pruneZones(newZones);

    for (const z of pruned) {
      const old = previous.get(z.id);
      if (old) {
        z.tested = old.tested;
      } else if (refPrice >= z.bottom && refPrice <= z.top) {
        // Already inside when first found: mark silently so it does not alert.
        z.tested = true;
      }
    }

    state.zones = state.zones.filter(z => z.pair !== pair).concat(pruned);

    // 5. Fallback touch check when no live tick is available (e.g. Twelve Data crypto).
    if (!hasFreshTick) {
      p.price = last5.close;
      p.priceTime = last5.epoch;
      p.priceSource = 'candle';
      checkZoneTouches(pair, last5.close);
    }

    p.status = 'Scanning';
    p.zoneCount = pruned.length;
    p.lastScan = new Date().toISOString();
    p.error = '';
  } catch (err) {
    p.status = 'Scan error';
    p.error = err.message;
    state.lastError = `${pair}: ${err.message}`;
    log(`Scan failed for ${pair}: ${err.message}`);
  }
}

async function runScan() {
  // Prevent overlapping scans if one runs longer than the interval.
  if (scanning) {
    log('Previous scan still running; skipping this interval.');
    return;
  }
  scanning = true;
  try {
    if (!state.connected) {
      await deriv.connect();
    }

    scanCount++;
    const runCrypto = scanCount % CRYPTO_SCAN_EVERY_N === 1;

    let pairs = TARGET_PAIRS.filter(pair => state.pairs[pair] && state.pairs[pair].symbol);
    if (!runCrypto) {
      pairs = pairs.filter(pair => state.pairs[pair].source !== 'twelvedata');
    }

    // Three workers. Twelve Data calls are serialised separately inside tdRequest.
    let cursor = 0;
    const workers = Array.from({ length: 3 }, async () => {
      while (cursor < pairs.length) {
        const pair = pairs[cursor++];
        await scanPair(pair);
      }
    });
    await Promise.all(workers);
    state.lastScan = new Date().toISOString();
    log(`Scan completed. Active configured markets: ${pairs.length}. Zones tracked: ${state.zones.length}.`);
  } catch (err) {
    state.lastError = err.message;
    log(`Scan aborted: ${err.message}`);
  } finally {
    scanning = false;
  }
}

async function startUp() {
  while (true) {
    try {
      await deriv.connect();
      if (!Object.values(state.pairs).some(p => p.symbol)) await resolveSymbols();
      break;
    } catch (err) {
      state.lastError = err.message;
      log(`Startup failed, retrying in 30s: ${err.message}`);
      await sleep(30_000);
    }
  }

  await runScan();
  setInterval(() => {
    runScan().catch(err => {
      state.lastError = err.message;
      log(`Scheduled scan error: ${err.message}`);
    });
  }, SCAN_INTERVAL_MS);
}

/* =========================================================
   WEB SERVER
========================================================= */

function requireDashboardAuth(req, res, next) {
  if (!DASHBOARD_TOKEN) return next();
  const header = req.headers.authorization || '';
  const provided = header.startsWith('Bearer ') ? header.slice(7) : String(req.query.token || '');
  if (provided === DASHBOARD_TOKEN) return next();
  res.status(401).json({ error: 'Unauthorized' });
}

app.get('/', requireDashboardAuth, (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/api/status', requireDashboardAuth, (_req, res) => {
  res.json({
    startedAt: state.startedAt,
    connected: state.connected,
    lastScan: state.lastScan,
    lastError: state.lastError,
    activeSymbolsCount: state.activeSymbolsCount,
    targetPairCount: TARGET_PAIRS.length,
    timeframes: [...ZONE_TIMEFRAMES.map(tf => tf.label), TF_12H.label, TF_1W.label],
    pairs: Object.values(state.pairs),
    zones: state.zones.slice().sort((a, b) => b.createdAt - a.createdAt),
    signals: state.signals,
    logs: state.logs
  });
});

// Health check stays open so hosting platforms can probe it.
app.get('/health', (_req, res) => res.json({ ok: true, connected: state.connected }));

app.listen(PORT, () => {
  log(`Dashboard listening on port ${PORT}`);
  startUp();
});
