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
const TD_GAP_MS = Math.max(1000, Number(process.env.TD_GAP_MS || 8000));
// The free Twelve Data plan also allows only 800 requests PER DAY. The bot stops at this
// budget (and shows "Paused") instead of running into errors. Raise it only if your plan
// has a higher daily limit.
const TD_DAILY_BUDGET = Math.max(10, Number(process.env.TD_DAILY_BUDGET || 750));
// Crypto candles come from Twelve Data, so crypto is scanned on a slower schedule than forex.
const CRYPTO_SCAN_INTERVAL_MS = Math.max(SCAN_INTERVAL_MS, Number(process.env.CRYPTO_SCAN_INTERVAL_MIN || 20) * 60_000);
const CONFIRMATION_MAX_AGE_MS = 30 * 60 * 1000;
const STOCKS = ['AAPL', 'TSLA', 'NVDA', 'AMZN']; // display only; not scanned
const RR = 2;
const ATR_PERIOD = 14;

/* =========================================================
   TIMEFRAMES
========================================================= */

// Every timeframe a zone can be built from.
// seconds: Deriv granularity. tdInterval: Twelve Data interval name.
// maxAgeBars: how many bars old a structure break can be and still make a zone.
const ZONE_TIMEFRAMES = [
  { label: '5M',  seconds: 300,   tdInterval: '5min',  maxAgeBars: 288 }, // about 1 day
  { label: '15M', seconds: 900,   tdInterval: '15min', maxAgeBars: 120 }, // about 30 hours
  { label: '30M', seconds: 1800,  tdInterval: '30min', maxAgeBars: 96 },  // about 2 days
  { label: '1H',  seconds: 3600,  tdInterval: '1h',    maxAgeBars: 120 }, // about 5 days
  { label: '4H',  seconds: 14400, tdInterval: '4h',    maxAgeBars: 60 },  // about 10 days
  { label: '1D',  seconds: 86400, tdInterval: '1day',  maxAgeBars: 30 },  // about 30 days
  // 1W is built from the daily candles (Deriv has no weekly data) and is used for CRT only,
  // not for zones. It needs no extra requests.
  { label: '1W',  seconds: 604800, tdInterval: '1week', maxAgeBars: 0, zones: false, derivedFrom: '1D' }
];

// The 5M timeframe is also used for rejection confirmation.
const TF_5M = ZONE_TIMEFRAMES[0];

/* =========================================================
   CRT (CANDLE RANGE THEORY) SETTINGS
========================================================= */

// CRT_MODE:
//   off    = CRT is ignored (bot behaves exactly as before)
//   tag    = CRT info is added to messages, but signals are NOT blocked
//   filter = signals only fire when an aligned CRT exists (default)
const CRT_MODE = ['off', 'tag', 'filter'].includes(String(process.env.CRT_MODE || '').toLowerCase())
  ? String(process.env.CRT_MODE).toLowerCase()
  : 'filter';

// CRT_TIMEFRAMES: comma-separated list of timeframes checked for CRT.
// Any of 5M, 15M, 30M, 1H, 4H, 1D, or ALL for every timeframe.
// Default: 15M,30M,1H,4H,1D,1W (every timeframe except 5M, which is noisy).
// (The old single CRT_TIMEFRAME variable still works if CRT_TIMEFRAMES is not set.)
const CRT_TF_LABELS = String(process.env.CRT_TIMEFRAMES || process.env.CRT_TIMEFRAME || '15M,30M,1H,4H,1D,1W')
  .split(',').map(x => x.trim().toUpperCase()).filter(Boolean);
const CRT_TFS = ZONE_TIMEFRAMES.filter(tf => CRT_TF_LABELS.includes('ALL') || CRT_TF_LABELS.includes(tf.label));
if (!CRT_TFS.length) CRT_TFS.push(ZONE_TIMEFRAMES.find(tf => tf.label === '4H'));

// CRT_VALID_BARS: how many closed CRT-timeframe candles back a sweep candle can be
// and still count. 1 = only the most recent closed candle, 2 = also the one before.
const CRT_VALID_BARS = Math.max(1, Math.min(5, Number(process.env.CRT_VALID_BARS || 2)));

// CRT_CONFIRM_CANDLE3: when true (default), a CRT only counts once candle 3 (the candle right
// after the sweep candle, on the same timeframe) has CLOSED in the trade direction (green for
// a buy, red for a sell) AND inside candle 1's range. This is the strict version: candle 3 is
// the only chance. If it closes the wrong colour, or outside the range, that CRT is dropped.
// With this on, CRT_VALID_BARS counts the closed candles back from the confirming candle 3.
// Alerts arrive one candle of that timeframe later, because the bot waits for candle 3 to close.
// Set CRT_CONFIRM_CANDLE3=false to use the sweep candle's close as the confirmation.
const CRT_CONFIRM_CANDLE3 = String(process.env.CRT_CONFIRM_CANDLE3 || 'true').toLowerCase() !== 'false';

// CRT_AT_ZONE: when true (default), the CRT sweep must happen AT the supply/demand zone.
// Bullish: the swept area (sweep low up to the range low) must overlap the demand zone.
// Bearish: the swept area (range high up to the sweep high) must overlap the supply zone.
// Set CRT_AT_ZONE=false to accept any CRT that points the same way as the zone.
const CRT_AT_ZONE = String(process.env.CRT_AT_ZONE || 'true').toLowerCase() !== 'false';

// CRT_ZONE_TOLERANCE: extra room around the swept area, as a fraction of the CRT range.
// 0.1 = 10% of the range. Raise it to be less strict.
const CRT_ZONE_TOLERANCE = Math.max(0, Number(process.env.CRT_ZONE_TOLERANCE || 0.1));

// CRT_REQUIRE_COLOR: when true (default), candle 2 (the sweep candle) must close in the
// trade direction: GREEN (close above open) for a buy CRT, RED (close below open) for a
// sell CRT. A CRT whose sweep candle has the wrong colour, or no body, is ignored
// everywhere: no CRT message, and it is not used to pass a zone alert.
// Set CRT_REQUIRE_COLOR=false to turn this rule off.
const CRT_REQUIRE_COLOR = String(process.env.CRT_REQUIRE_COLOR || 'true').toLowerCase() !== 'false';

// CRT_MANAGE_TRADE: when true (default) and a CRT backs an alert, the stop goes beyond the
// CRT sweep extreme (or the zone edge, if that is farther) and the take profit is the CRT
// target (the opposite end of candle 1's range). Alerts without a CRT keep the zone stop
// and the 1:2 target.
// CRT_MAX_SL_MULT: if the sweep stop is more than this many times wider than the zone stop,
// the zone stop is kept instead and the message says so. Default 3.
// CRT_MIN_RR: the CRT target is only used if it gives at least this reward-to-risk,
// otherwise the 1:2 target is used. Default 1.
const CRT_MANAGE_TRADE = String(process.env.CRT_MANAGE_TRADE || 'true').toLowerCase() !== 'false';
const CRT_MAX_SL_MULT = Math.max(1, Number(process.env.CRT_MAX_SL_MULT || 3));
const CRT_MIN_RR = Math.max(0.5, Number(process.env.CRT_MIN_RR || 1));

// CRT_ALERTS: when true (default), a standalone Telegram message is sent each time a
// NEW CRT forms on one of the CRT_ALERT_TIMEFRAMES, even if price is not at any zone.
// CRT_ALERT_TIMEFRAMES: default 1H,4H,1D,1W. Remove 1H if it sends too many messages.
// CRT_ALERT_MAX_AGE_MIN: a CRT is only announced if its sweep candle closed within this
// many minutes, so restarting the bot does not replay old CRTs. Default 60.
const CRT_ALERTS = String(process.env.CRT_ALERTS || 'true').toLowerCase() !== 'false';
const CRT_ALERT_TFS = String(process.env.CRT_ALERT_TIMEFRAMES || '1H,4H,1D,1W')
  .split(',').map(x => x.trim().toUpperCase()).filter(Boolean);
const CRT_ALERT_MAX_AGE_MS = Math.max(5, Number(process.env.CRT_ALERT_MAX_AGE_MIN || 60)) * 60 * 1000;

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
  MAX_ZONES_PER_PAIR: 18       // 3 per timeframe across 6 timeframes
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
   CRT (CANDLE RANGE THEORY) DETECTION
========================================================= */

// Looks at the last few CLOSED candles of the CRT timeframe.
//   Reference candle = the range. Sweep candle = the next candle.
//   Bullish CRT: sweep candle takes out the reference LOW, closes back inside,
//                and does not take out the reference HIGH. Target = reference high.
//   Bearish CRT: sweep candle takes out the reference HIGH, closes back inside,
//                and does not take out the reference LOW. Target = reference low.
// A CRT is dropped once a later closed candle closes outside the range or
// the target has already been reached.
function detectCRT(candles, tf) {
  if (!Array.isArray(candles)) return null;
  const closed = candles.slice(0, -1); // the last candle is still forming
  if (closed.length < 5) return null;

  const last = closed.length - 1;

  // With the candle-3 rule, candle 3 is the last closed candle at the earliest (k = 1).
  const firstK = CRT_CONFIRM_CANDLE3 ? 1 : 0;
  const lastK = CRT_CONFIRM_CANDLE3 ? CRT_VALID_BARS : CRT_VALID_BARS - 1;

  for (let k = firstK; k <= lastK; k++) {
    const si = last - k;
    const ri = si - 1;
    if (ri < 0) break;

    const ref = closed[ri];
    const sw = closed[si];

    let direction = null;
    if (sw.low < ref.low && sw.high <= ref.high && sw.close > ref.low && sw.close < ref.high) {
      direction = 'BUY';
    } else if (sw.high > ref.high && sw.low >= ref.low && sw.close < ref.high && sw.close > ref.low) {
      direction = 'SELL';
    }
    if (!direction) continue;

    // Colour rule: the sweep candle must close in the trade direction.
    if (CRT_REQUIRE_COLOR) {
      if (direction === 'BUY' && !(sw.close > sw.open)) continue;
      if (direction === 'SELL' && !(sw.close < sw.open)) continue;
    }

    // Strict candle-3 rule: the next candle must close in the trade direction and inside
    // candle 1's range. One chance only: otherwise this CRT is dropped.
    let c3 = null;
    if (CRT_CONFIRM_CANDLE3) {
      c3 = closed[si + 1];
      const inside = c3.close > ref.low && c3.close < ref.high;
      const rightColour = direction === 'BUY' ? c3.close > c3.open : c3.close < c3.open;
      if (!inside || !rightColour) continue;
    }

    const later = closed.slice(si + 1);
    const spent = later.some(c => direction === 'BUY'
      ? (c.close < ref.low || c.high >= ref.high)
      : (c.close > ref.high || c.low <= ref.low));
    if (spent) continue;

    return {
      direction,
      timeframe: tf.label,
      rangeHigh: ref.high,
      rangeLow: ref.low,
      mid: (ref.high + ref.low) / 2,
      target: direction === 'BUY' ? ref.high : ref.low,
      sweepExtreme: direction === 'BUY' ? sw.low : sw.high,
      sweepEpoch: sw.epoch,
      confirmed: !!c3,
      confirmEpoch: c3 ? c3.epoch : null
    };
  }

  return null;
}

// Returns the pair's CRT if it points the same way as the zone, otherwise null.
// With several CRT timeframes, the highest-timeframe match supplies the range and
// target, and `confluence` lists every timeframe that agrees.
function crtAtZone(c, zone) {
  const tol = (c.rangeHigh - c.rangeLow) * CRT_ZONE_TOLERANCE;
  const lo = (c.direction === 'BUY' ? c.sweepExtreme : c.rangeHigh) - tol;
  const hi = (c.direction === 'BUY' ? c.rangeLow : c.sweepExtreme) + tol;
  return zone.bottom <= hi && zone.top >= lo;
}

function alignedCRT(pairState, direction, zone) {
  if (CRT_MODE === 'off' || !pairState || !Array.isArray(pairState.crts)) return null;
  const matches = pairState.crts.filter(c =>
    c.direction === direction && (!CRT_AT_ZONE || !zone || crtAtZone(c, zone))
  );
  if (!matches.length) return null;
  const best = matches[matches.length - 1]; // crts are stored in ascending timeframe order
  return { ...best, confluence: matches.map(m => m.timeframe) };
}

// Telegram lines describing the CRT that backs a signal.
function crtLines(pair, crt) {
  if (CRT_MODE === 'off') return [];
  if (!crt) return CRT_MODE === 'tag' ? ['CRT: none at this zone'] : [];
  return [
    `CRT (${(crt.confluence || [crt.timeframe]).join(' + ')}): ${crt.direction === 'BUY' ? 'bullish' : 'bearish'} sweep, closed back inside${crt.confirmed ? ', candle 3 confirmed' : ''}`,
    `CRT range (${crt.timeframe}): ${fmtPrice(pair, crt.rangeLow)} – ${fmtPrice(pair, crt.rangeHigh)}`,
    `CRT sweep extreme: ${fmtPrice(pair, crt.sweepExtreme)}`,
    `CRT target: ${fmtPrice(pair, crt.target)} (midpoint ${fmtPrice(pair, crt.mid)})`
  ];
}

/* =========================================================
   TRADE PLAN (stop loss and take profit)
========================================================= */

// Target for an alert. Default 1:RR. With a CRT behind the alert (CRT_MANAGE_TRADE),
// the target is the CRT target if it gives at least CRT_MIN_RR, otherwise 1:RR.
function planTargets(pair, direction, entry, sl, crt) {
  const isBuy = direction === 'BUY';
  const risk = Math.abs(entry - sl);
  let tp = isBuy ? entry + RR * risk : entry - RR * risk;
  let tpNote = `1:${RR}`;
  if (CRT_MANAGE_TRADE && crt && Number.isFinite(crt.target)) {
    const dist = isBuy ? crt.target - entry : entry - crt.target;
    if (dist >= risk * CRT_MIN_RR) {
      tp = crt.target;
      tpNote = 'CRT target';
    } else {
      tpNote = `1:${RR}, CRT target too close`;
    }
  }
  tp = roundPrice(pair, tp);
  return { tp, tpNote, rr: Math.abs(tp - entry) / risk };
}

// Stop and target for a zone touch. Without a CRT the stop sits just beyond the zone.
// With a CRT the stop goes beyond the sweep extreme when that is farther than the zone
// stop, unless it would be more than CRT_MAX_SL_MULT times wider.
function planTrade(pair, direction, entry, zone, crt, atr) {
  const isBuy = direction === 'BUY';
  const buffer = Math.max(atr * 0.15, entry * 0.00005);
  const zoneSl = isBuy ? zone.bottom - buffer : zone.top + buffer;
  let sl = zoneSl;
  let slNote = 'beyond zone';

  if (CRT_MANAGE_TRADE && crt && Number.isFinite(crt.sweepExtreme)) {
    const sweepSl = isBuy ? crt.sweepExtreme - buffer : crt.sweepExtreme + buffer;
    const farther = isBuy ? Math.min(zoneSl, sweepSl) : Math.max(zoneSl, sweepSl);
    const zoneRisk = Math.abs(entry - zoneSl);
    if (zoneRisk > 0 && Math.abs(entry - farther) > zoneRisk * CRT_MAX_SL_MULT) {
      slNote = 'CRT stop too wide, zone stop used';
    } else if (farther === sweepSl && sweepSl !== zoneSl) {
      sl = sweepSl;
      slNote = 'beyond CRT sweep';
    }
  }

  sl = roundPrice(pair, sl);
  const risk = Math.abs(entry - sl);
  if (!(risk > 0) || (isBuy ? entry <= sl : entry >= sl)) return null;
  return { sl, slNote, ...planTargets(pair, direction, entry, sl, crt) };
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
function makeZones(pair, tf, candles) {
  const closed = candles.slice(0, -1); // the last candle is still forming
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
      count: 220,
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

// Free plan: 8 requests per minute AND 800 per day. To stay inside both, the bot
//  - spaces requests out (TD_GAP_MS),
//  - caches candles and only re-fetches a timeframe once a new candle has formed,
//  - scans crypto every CRYPTO_SCAN_INTERVAL_MS instead of on every forex scan,
//  - stops at TD_DAILY_BUDGET and shows "Paused" instead of "Scan error".
class TdLimitError extends Error {
  constructor(message, resumeAtMs) {
    super(message);
    this.name = 'TdLimitError';
    this.resumeAtMs = resumeAtMs;
  }
}

const td = { day: '', used: 0, pausedUntil: 0, pauseReason: '' };
const tdCache = new Map();

function nextUtcResumeMs() {
  const d = new Date();
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 1, 0, 1, 0);
}

// Throws TdLimitError if Twelve Data should not be called right now.
function tdCheckAllowed(needed = 1) {
  const today = new Date().toISOString().slice(0, 10);
  if (td.day !== today) { // new UTC day: the daily counter resets
    td.day = today;
    td.used = 0;
    if (td.pauseReason === 'daily') { td.pausedUntil = 0; td.pauseReason = ''; }
  }
  if (td.pausedUntil && Date.now() >= td.pausedUntil) { td.pausedUntil = 0; td.pauseReason = ''; }
  if (td.pausedUntil) {
    throw new TdLimitError(
      td.pauseReason === 'daily'
        ? 'Twelve Data daily limit reached, resumes 00:01 UTC'
        : 'Twelve Data per-minute limit hit, waiting a minute',
      td.pausedUntil
    );
  }
  if (td.used + needed > TD_DAILY_BUDGET) {
    td.pausedUntil = nextUtcResumeMs();
    td.pauseReason = 'daily';
    throw new TdLimitError('Twelve Data daily budget used, resumes 00:01 UTC', td.pausedUntil);
  }
}

// Serialises Twelve Data requests so they never exceed the per-minute limit,
// even when several scan workers run at once.
let tdChain = Promise.resolve();
let tdLastCall = 0;

function tdRequest(url) {
  const run = tdChain.then(async () => {
    tdCheckAllowed(1);
    const wait = TD_GAP_MS - (Date.now() - tdLastCall);
    if (wait > 0) await sleep(wait);
    tdCheckAllowed(1); // check again after waiting; a limit may have been hit meanwhile
    tdLastCall = Date.now();
    td.used++;
    const res = await fetch(url);
    const data = await res.json();
    const msg = String((data && data.message) || '');
    if (data && (data.code === 429 || /run out of API credits|too many requests/i.test(msg))) {
      if (/for the day/i.test(msg)) {
        td.pausedUntil = nextUtcResumeMs();
        td.pauseReason = 'daily';
        td.used = Math.max(td.used, TD_DAILY_BUDGET);
        throw new TdLimitError('Twelve Data daily limit reached, resumes 00:01 UTC', td.pausedUntil);
      }
      td.pausedUntil = Date.now() + 65_000;
      td.pauseReason = 'minute';
      throw new TdLimitError('Twelve Data per-minute limit hit, waiting a minute', td.pausedUntil);
    }
    return data;
  });
  tdChain = run.catch(() => {});
  return run;
}

async function twelveHistory(tdSymbol, interval) {
  if (!TWELVE_DATA_API_KEY) throw new Error('TWELVE_DATA_API_KEY is not set');

  const params = new URLSearchParams({
    symbol: tdSymbol,
    interval,
    outputsize: '220',
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

// Re-fetches a timeframe only once a new candle has formed since the last fetch.
async function twelveCandles(tdSymbol, tf) {
  const key = `${tdSymbol}|${tf.tdInterval}`;
  const cached = tdCache.get(key);
  if (cached && cached.length) {
    const last = cached[cached.length - 1];
    if (Date.now() / 1000 < last.epoch + tf.seconds + 15) return cached;
  }
  const candles = await twelveHistory(tdSymbol, tf.tdInterval);
  tdCache.set(key, candles);
  return candles;
}

// Returns candles for a pair and timeframe, from whichever source the pair uses.
async function getCandles(p, tf) {
  if (p.source === 'twelvedata') {
    return twelveCandles(p.symbol, tf);
  }
  return deriv.history(p.symbol, tf.seconds);
}

// Builds weekly candles (weeks start Monday 00:00 UTC) from daily candles.
// shiftSec: forex and gold open on Sunday evening, so their Sunday daily candle belongs to
// the NEXT week (shift of one day). Crypto trades all weekend, so it uses no shift.
function weeklyFromDaily(daily, shiftSec) {
  const WEEK = 604800;
  const MONDAY0 = 345600; // 1970-01-05 00:00 UTC was a Monday
  const weeks = new Map();
  for (const c of daily) {
    const idx = Math.floor((c.epoch + shiftSec - MONDAY0) / WEEK);
    const w = weeks.get(idx);
    if (!w) {
      weeks.set(idx, { epoch: MONDAY0 + idx * WEEK, open: c.open, high: c.high, low: c.low, close: c.close });
    } else {
      w.high = Math.max(w.high, c.high);
      w.low = Math.min(w.low, c.low);
      w.close = c.close;
    }
  }
  return [...weeks.entries()].sort((a, b) => a[0] - b[0]).map(e => e[1]);
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
   STANDALONE CRT ALERT (4H / 1D by default)
========================================================= */

const crtAlerted = new Set();

// Announces a newly formed CRT, whether or not price is at a zone.
function sendCRTAlerts(pair, p, zones) {
  if (!CRT_ALERTS || CRT_MODE === 'off' || !Array.isArray(p.crts)) return;
  const now = Date.now();

  for (const c of p.crts) {
    if (!CRT_ALERT_TFS.includes(c.timeframe)) continue;
    const tf = ZONE_TIMEFRAMES.find(t => t.label === c.timeframe);
    if (!tf) continue;

    const key = `${pair}-${c.timeframe}-${c.direction}-${c.sweepEpoch}`;
    if (crtAlerted.has(key)) continue;
    crtAlerted.add(key);
    if (crtAlerted.size > 3000) crtAlerted.clear(); // old keys fail the age check anyway

    // Only announce fresh CRTs: the sweep candle must have closed recently.
    const closeMs = ((c.confirmEpoch || c.sweepEpoch) + tf.seconds) * 1000;
    if (now - closeMs > CRT_ALERT_MAX_AGE_MS) continue;

    const isBuy = c.direction === 'BUY';
    const atZones = [...new Set(
      (zones || [])
        .filter(z => z.direction === c.direction && crtAtZone(c, z))
        .map(z => `${z.timeframe} ${isBuy ? 'demand' : 'supply'}`)
    )];
    const source = p.source === 'twelvedata' ? 'Twelve Data' : 'Deriv';

    const message = [
      `🕯 CRT DETECTED — ${c.direction} ${prettyPair(pair)} (${c.timeframe})`,
      isBuy
        ? `Bullish: the range low was swept, then price closed back inside.`
        : `Bearish: the range high was swept, then price closed back inside.`,
      c.confirmed ? `Candle 3 confirmed: closed ${isBuy ? 'green' : 'red'} inside the range.` : null,
      `CRT range: ${fmtPrice(pair, c.rangeLow)} – ${fmtPrice(pair, c.rangeHigh)}`,
      `Sweep extreme: ${fmtPrice(pair, c.sweepExtreme)}`,
      `Target: ${fmtPrice(pair, c.target)} (midpoint ${fmtPrice(pair, c.mid)})`,
      atZones.length ? `Zone at the sweep: ${atZones.join(', ')}` : `Zone at the sweep: none found`,
      Number.isFinite(p.price) ? `Price now: ${fmtPrice(pair, p.price)}` : null,
      `Price source: ${source}`,
      `ℹ️ Informational only. No zone touch is needed for this message.`
    ].filter(Boolean).join('\n');

    state.signals.unshift({
      id: `${key}-crt`,
      time: new Date().toISOString(),
      pair,
      direction: c.direction,
      timeframe: c.timeframe,
      type: 'CRT',
      zoneTop: null,
      zoneBottom: null,
      crt: { ...c },
      note: 'Standalone CRT alert; no zone touch required.'
    });
    state.signals = state.signals.slice(0, 100);

    sendTelegram(message)
      .then(ok => log(`${ok ? 'Telegram sent' : 'Signal recorded'}: CRT ${c.direction} ${pair} ${c.timeframe}`))
      .catch(err => {
        log(`Telegram send failed: ${err.message}`);
        state.lastError = `Telegram: ${err.message}`;
      });
  }
}

/* =========================================================
   ZONE TOUCH (immediate warning)
========================================================= */

// Alerts once per zone, on its first touch. Overlapping zones touched together
// produce a single alert for the strongest one.
// In CRT filter mode, a zone only counts as touched while an aligned CRT exists
// (and, with CRT_AT_ZONE on, only if that CRT's sweep happened at this zone).
// If price sits in the zone before the CRT forms, the zone stays untested and
// can still alert once the CRT appears.
function checkZoneTouches(pair, price) {
  const pairState = state.pairs[pair];
  if (!pairState || !Number.isFinite(price)) return;

  let touched = state.zones.filter(z =>
    z.pair === pair && !z.tested && price >= z.bottom && price <= z.top
  );

  // CRT filter: keep only zones that agree with the current CRT direction.
  if (CRT_MODE === 'filter') {
    touched = touched.filter(z => alignedCRT(pairState, z.direction, z));
  }
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
  const crt = alignedCRT(pairState, zone.direction, zone);
  const atr = pairState.atr5m || (zone.top - zone.bottom) || price * 0.0005;
  const entry = roundPrice(pair, price);
  const plan = planTrade(pair, zone.direction, entry, zone, crt, atr);
  if (!plan) return;
  const { sl, tp } = plan;

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
    slNote: plan.slNote,
    tp,
    crt: crt ? { ...crt } : null,
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
    crt: crt ? { ...crt } : null,
    note: 'First touch of a zone. Wait for a separate 5M rejection confirmation.'
  });
  state.signals = state.signals.slice(0, 100);

  const source = pairState.source === 'twelvedata' ? 'Twelve Data' : 'Deriv';
  const message = [
    `🔔 ZONE TOUCH — ${zone.direction} ${prettyPair(pair)}`,
    `Zone: ${zone.timeframe} ${isBuy ? 'DEMAND' : 'SUPPLY'} (strength ${zone.score} ATR)`,
    `Zone range: ${fmtPrice(pair, zone.bottom)} – ${fmtPrice(pair, zone.top)}`,
    ...crtLines(pair, crt),
    `Price at touch: ${fmtPrice(pair, entry)}`,
    `Reference SL: ${fmtPrice(pair, sl)} (${plan.slNote})`,
    `Reference TP: ${fmtPrice(pair, tp)} (${plan.tpNote}, R:R 1:${plan.rr.toFixed(1)})`,
    touched.length > 1 ? `Also touched: ${touched.length - 1} overlapping zone(s)` : null,
    `Next: waiting for a completed 5M rejection candle.`,
    `Price source: ${source}`,
    `⚠️ Warning only; this is not yet a confirmed entry.`
  ].filter(Boolean).join('\n');

  sendTelegram(message)
    .then(ok => log(`${ok ? 'Telegram sent' : 'Signal recorded'}: ZONE TOUCH ${zone.direction} ${pair}${crt ? ' + CRT' : ''}`))
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
      const plan = planTargets(pair, pending.direction, entry, pending.sl, pending.crt);
      const tp = plan.tp;

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
        crt: pending.crt || null,
        note: 'Completed 5M rejection candle detected. Confirm execution conditions.'
      });
      state.signals = state.signals.slice(0, 100);

      const source = state.pairs[pair] && state.pairs[pair].source === 'twelvedata' ? 'Twelve Data' : 'Deriv';
      const message = [
        `✅ 5M REJECTION CONFIRMED — ${pending.direction} ${prettyPair(pair)}`,
        `Zone: ${pending.timeframe} (strength ${pending.score} ATR)`,
        ...crtLines(pair, pending.crt),
        `Reference entry (5M close): ${fmtPrice(pair, entry)}`,
        `Stop Loss: ${fmtPrice(pair, pending.sl)}${pending.slNote ? ` (${pending.slNote})` : ''}`,
        `Take Profit: ${fmtPrice(pair, tp)} (${plan.tpNote}, R:R 1:${plan.rr.toFixed(1)})`,
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
      crt: null,
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
      crt: null,
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

  if (p.source === 'twelvedata') {
    try {
      tdCheckAllowed(6); // a full scan of one pair needs up to 6 requests
    } catch (e) {
      if (e instanceof TdLimitError) {
        p.status = `Paused: ${e.message}`;
        p.error = '';
        return;
      }
      throw e;
    }
    // The next crypto scan is due after the crypto interval; runScan skips it until then.
    p.nextDueMs = Date.now() + CRYPTO_SCAN_INTERVAL_MS - 60_000;
  }

  try {
    // 1. Fetch candles for every timeframe. The 5M set is also used for confirmation,
    //    and the CRT timeframe's candles are used for CRT detection (no extra requests).
    const newZones = [];
    let candles5m = null;
    const crtFound = [];

    let daily = null;
    for (const tf of ZONE_TIMEFRAMES) {
      let candles;
      if (tf.derivedFrom) {
        // Weekly candles are built from the daily candles fetched just before.
        if (!daily) continue;
        candles = weeklyFromDaily(daily, p.source === 'deriv' ? 86400 : 0);
      } else {
        candles = await getCandles(p, tf);
        await sleep(REQUEST_GAP_MS);
        if (tf.label === '1D') daily = candles;
      }
      if (tf === TF_5M) candles5m = candles;
      if (CRT_TFS.some(t => t.label === tf.label)) {
        const found = detectCRT(candles, tf);
        if (found) crtFound.push(found);
      }
      if (tf.zones !== false) newZones.push(...makeZones(pair, tf, candles));
    }

    if (!candles5m || candles5m.length < 3) throw new Error('Not enough 5M candles');

    // Update CRT before any touch check so the filter sees the latest setup.
    const crtKey = c => `${c.timeframe}-${c.direction}-${c.sweepEpoch}`;
    const prevKeys = new Set((p.crts || []).map(crtKey));
    p.crts = CRT_MODE === 'off' ? [] : crtFound;
    p.crt = p.crts.length ? p.crts[p.crts.length - 1] : null; // highest timeframe, for the dashboard
    for (const c of p.crts) {
      if (!prevKeys.has(crtKey(c))) {
        log(`CRT ${c.direction} on ${pair} (${c.timeframe}), target ${fmtPrice(pair, c.target)}`);
      }
    }

    const last5 = candles5m[candles5m.length - 1];
    const atr5m = calcATR(candles5m.slice(0, -1));
    if (atr5m) p.atr5m = atr5m;
    confirmRejections(pair, candles5m);

    // 2. Decide which price to use for touch checks.
    const nowSec = Date.now() / 1000;
    const hasFreshTick = p.priceSource === 'tick' && p.priceTime && nowSec - p.priceTime < 600;
    const refPrice = hasFreshTick ? p.price : last5.close;

    // 3. Keep the strongest zones across all timeframes, then carry over tested state.
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

    // Standalone CRT announcement (4H / 1D by default), independent of zone touches.
    sendCRTAlerts(pair, p, pruned);

    // 4. Fallback touch check when no live tick is available (e.g. Twelve Data crypto).
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
    p.tdPauseLogged = false;
  } catch (err) {
    if (err instanceof TdLimitError) {
      // Not a real error: Twelve Data's free limit was reached. Wait it out quietly.
      p.status = `Paused: ${err.message}`;
      p.error = '';
      p.nextDueMs = err.resumeAtMs || 0;
      if (!p.tdPauseLogged) {
        log(`${pair}: ${err.message}`);
        p.tdPauseLogged = true;
      }
      return;
    }
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
    const nowMs = Date.now();
    const pairs = TARGET_PAIRS.filter(pair => {
      const p = state.pairs[pair];
      if (!p || !p.symbol) return false;
      // Crypto (Twelve Data) runs on its own, slower schedule.
      if (p.source === 'twelvedata' && p.nextDueMs && nowMs < p.nextDueMs) return false;
      return true;
    });
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
  log(`Twelve Data: ${TD_DAILY_BUDGET}/day budget, crypto scanned every ${Math.round(CRYPTO_SCAN_INTERVAL_MS / 60000)} min`);
  log(`CRT mode: ${CRT_MODE}${CRT_MODE === 'off' ? '' : ` | timeframes ${CRT_TFS.map(t => t.label).join(',')} | valid bars ${CRT_VALID_BARS} | at-zone ${CRT_AT_ZONE} | color rule ${CRT_REQUIRE_COLOR} | candle-3 rule ${CRT_CONFIRM_CANDLE3} | trade plan ${CRT_MANAGE_TRADE} | CRT alerts ${CRT_ALERTS ? CRT_ALERT_TFS.join(',') : 'off'}`}`);

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
    timeframes: ZONE_TIMEFRAMES.map(tf => tf.label),
    crt: { mode: CRT_MODE, timeframes: CRT_TFS.map(t => t.label), validBars: CRT_VALID_BARS, atZone: CRT_AT_ZONE, requireColor: CRT_REQUIRE_COLOR, manageTrade: CRT_MANAGE_TRADE, confirmCandle3: CRT_CONFIRM_CANDLE3, zoneTolerance: CRT_ZONE_TOLERANCE, alerts: CRT_ALERTS ? CRT_ALERT_TFS : [] },
    twelveData: {
      usedToday: td.used,
      dailyBudget: TD_DAILY_BUDGET,
      pausedUntil: td.pausedUntil ? new Date(td.pausedUntil).toISOString() : null,
      cryptoScanEveryMin: Math.round(CRYPTO_SCAN_INTERVAL_MS / 60000)
    },
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
