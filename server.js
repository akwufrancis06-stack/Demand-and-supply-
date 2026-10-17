'use strict';

const path = require('path');
const express = require('express');
const WebSocket = require('ws');

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);
const DERIV_APP_ID = process.env.DERIV_APP_ID || '1089';
const DERIV_WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public";
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';
// Optional. If set, the dashboard and /api/status require this token.
const DASHBOARD_TOKEN = process.env.DASHBOARD_TOKEN || '';

const SCAN_INTERVAL_MS = Math.max(60_000, Number(process.env.SCAN_INTERVAL_MS || 300_000));
const REQUEST_TIMEOUT_MS = 25_000;
const REQUEST_GAP_MS = Math.max(100, Number(process.env.REQUEST_GAP_MS || 250));
const ZONE_LOOKBACK = 160;
const MAX_ZONES_PER_PAIR_TF = 8;
const ZONE_TOUCH_COOLDOWN_MS = 30 * 60 * 1000;
const CONFIRMATION_MAX_AGE_MS = 30 * 60 * 1000;
const STOCKS = ['AAPL', 'TSLA', 'NVDA', 'AMZN'];
const RR = 2;
const ATR_PERIOD = 14;

// 28 Forex pairs, followed by 4 stock slots.
// Deriv active_symbols is authoritative; unavailable instruments are skipped.
const TARGET_PAIRS = [
  'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'USDCAD', 'AUDUSD', 'NZDUSD',
  'EURGBP', 'EURJPY', 'EURCHF', 'EURCAD', 'EURAUD', 'EURNZD',
  'GBPJPY', 'GBPCHF', 'GBPCAD', 'GBPAUD', 'GBPNZD',
  'AUDJPY', 'AUDCHF', 'AUDCAD', 'AUDNZD',
  'NZDJPY', 'NZDCHF', 'NZDCAD',
  'CADJPY', 'CADCHF', 'CHFJPY',
  ...STOCKS
];

const TIMEFRAMES = [
  { label: '4H', seconds: 14400, source: 'HTF' },
  { label: '15M', seconds: 900, source: 'MTF' }
];

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
const lastAlertAt = new Map();

function log(message) {
  const entry = `${new Date().toISOString()} ${message}`;
  console.log(entry);
  state.logs.unshift(entry);
  state.logs = state.logs.slice(0, 80);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function normalizePair(value) {
  return String(value || '').toUpperCase().replace(/[^A-Z]/g, '');
}

function prettyPair(pair) {
  if (STOCKS.includes(pair)) return pair;
  return pair.length === 6 ? `${pair.slice(0, 3)}/${pair.slice(3)}` : pair;
}

function normalizeStock(value) {
  const text = String(value || '').toUpperCase();
  for (const stock of STOCKS) {
    if (new RegExp(`(^|[^A-Z])${stock}([.]|$|[^A-Z])`).test(text) || text.startsWith(`${stock}.`)) return stock;
  }
  return '';
}

function priceDigits(pair, price) {
  if (pair.endsWith('JPY')) return 3;
  if (price >= 100) return 3;
  if (price >= 10) return 4;
  return 5;
}

function fmtPrice(pair, price) {
  return Number(price).toFixed(priceDigits(pair, Number(price)));
}

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

function confirmedPivots(candles, left = 2, right = 2) {
  const highs = [], lows = [];
  // A pivot is only accepted after right-side candles have closed.
  for (let i = left; i < candles.length - right; i++) {
    let isHigh = true, isLow = true;
    for (let j = i - left; j <= i + right; j++) {
      if (j === i) continue;
      if (candles[j].high >= candles[i].high) isHigh = false;
      if (candles[j].low <= candles[i].low) isLow = false;
    }
    if (isHigh) highs.push({ index: i, price: candles[i].high, epoch: candles[i].epoch });
    if (isLow) lows.push({ index: i, price: candles[i].low, epoch: candles[i].epoch });
  }
  return { highs, lows };
}

function analyseStructure(candles) {
  const pivots = confirmedPivots(candles);
  const events = [];
  let lastHigh = null, lastLow = null, protectedHigh = null, protectedLow = null;
  let bias = 'NEUTRAL';
  const seenHighs = new Set(), seenLows = new Set();
  const byIndex = new Map();

  for (const p of pivots.highs) byIndex.set(p.index, [...(byIndex.get(p.index) || []), { ...p, kind: 'HIGH' }]);
  for (const p of pivots.lows) byIndex.set(p.index, [...(byIndex.get(p.index) || []), { ...p, kind: 'LOW' }]);

  for (let i = 0; i < candles.length; i++) {
    for (const p of (byIndex.get(i) || [])) {
      if (p.kind === 'HIGH') lastHigh = p;
      else lastLow = p;
    }
    const c = candles[i];
    if (lastHigh && c.close > lastHigh.price && !seenHighs.has(lastHigh.index)) {
      events.push({ index: i, epoch: c.epoch, type: bias === 'SELL' ? 'CHoCH' : 'BOS', direction: 'BUY', level: lastHigh.price, pivotIndex: lastHigh.index });
      seenHighs.add(lastHigh.index);
      bias = 'BUY';
      protectedLow = lastLow;
    }
    if (lastLow && c.close < lastLow.price && !seenLows.has(lastLow.index)) {
      events.push({ index: i, epoch: c.epoch, type: bias === 'BUY' ? 'CHoCH' : 'BOS', direction: 'SELL', level: lastLow.price, pivotIndex: lastLow.index });
      seenLows.add(lastLow.index);
      bias = 'SELL';
      protectedHigh = lastHigh;
    }
  }

  const rangeHigh = lastHigh ? lastHigh.price : null;
  const rangeLow = lastLow ? lastLow.price : null;
  return { pivots, events, bias, rangeHigh, rangeLow, protectedHigh, protectedLow };
}

function makeZones(pair, tf, candles) {
  const result = [];
  if (!Array.isArray(candles) || candles.length < ATR_PERIOD + 12) return result;
  const atr = calcATR(candles);
  if (!atr || !Number.isFinite(atr)) return result;

  const structure = analyseStructure(candles);
  const start = Math.max(1, candles.length - ZONE_LOOKBACK);
  const recentEvents = structure.events.filter(e => e.index >= start && e.index < candles.length - 1);

  // Approximation of the described behavior: confirmed close-based BOS/CHoCH
  // events create an order block from the nearest opposite-colour candle.
  // This is not the protected indicator's code and will not match it exactly.
  for (const event of recentEvents) {
    const impulse = candles[event.index];
    const body = Math.abs(impulse.close - impulse.open);
    if (body < atr * 0.35) continue;

    let sourceIndex = -1;
    for (let j = event.index - 1; j >= Math.max(0, event.index - 8); j--) {
      const c = candles[j];
      const opposite = event.direction === 'BUY' ? c.close < c.open : c.close > c.open;
      if (opposite) { sourceIndex = j; break; }
    }
    if (sourceIndex < 0) continue;

    const source = candles[sourceIndex];
    const top = Math.max(source.open, source.high);
    const bottom = Math.min(source.open, source.low);
    if (!(top > bottom)) continue;

    const direction = event.direction;
    const current = candles[candles.length - 2]; // latest fully closed candle
    const invalidated = direction === 'BUY' ? current.close < bottom : current.close > top;
    const rangeHigh = structure.rangeHigh;
    const rangeLow = structure.rangeLow;
    const midpoint = Number.isFinite(rangeHigh) && Number.isFinite(rangeLow) ? (rangeHigh + rangeLow) / 2 : null;
    const rangePosition = midpoint == null ? 'UNKNOWN' : (current.close >= midpoint ? 'PREMIUM' : 'DISCOUNT');

    result.push({
      id: `${pair}-${tf.label}-${direction === 'BUY' ? 'D' : 'S'}-${source.epoch}-${event.type}`,
      pair, timeframe: tf.label, direction, top, bottom,
      createdAt: source.epoch, eventType: event.type, eventEpoch: event.epoch,
      breakLevel: event.level, rangeHigh, rangeLow, equilibrium: midpoint,
      rangePosition, mitigated: invalidated,
      structureBias: structure.bias, sourceCandleEpoch: source.epoch
    });
  }

  // Annotate sweeps: a wick beyond a structural boundary that closes back inside.
  const lastClosed = candles[candles.length - 2];
  for (const z of result) {
    if (Number.isFinite(z.rangeHigh) && lastClosed.high > z.rangeHigh && lastClosed.close < z.rangeHigh) z.rangeSweep = 'HIGH_SWEEP';
    else if (Number.isFinite(z.rangeLow) && lastClosed.low < z.rangeLow && lastClosed.close > z.rangeLow) z.rangeSweep = 'LOW_SWEEP';
    else z.rangeSweep = null;
  }

  // De-duplicate identical zones, keeping the most recent ones.
  const seen = new Set();
  return result.reverse().filter(z => {
    const key = `${z.direction}:${z.top.toFixed(6)}:${z.bottom.toFixed(6)}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  }).reverse().slice(-MAX_ZONES_PER_PAIR_TF);
}

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
      checkZoneTouches(pair, quote);
      updateMitigation(pair, quote);
      break;
    }
  }

  subscribeTicks(symbols) {
    this.tickSymbols = [...symbols];
    if (!symbols.length) return;

    this.request({ ticks: symbols, subscribe: 1 })
      .then(() => log(`Subscribed to live ticks for ${symbols.length} available pairs.`))
      .catch(err => {
        log(`Batch tick subscription failed; trying per-symbol subscriptions: ${err.message}`);
        symbols.forEach((symbol, i) => {
          setTimeout(() => {
            this.request({ ticks: symbol, subscribe: 1 })
              .catch(e => log(`Tick subscription failed for ${symbol}: ${e.message}`));
          }, i * 150);
        });
      });
  }
}

const deriv = new DerivSocket();

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

function checkZoneTouches(pair, price) {
  const now = Date.now();
  const pairState = state.pairs[pair];
  if (!pairState) return;

  for (const zone of state.zones) {
    if (zone.pair !== pair || zone.mitigated) continue;
    if (price < zone.bottom || price > zone.top) continue;

    const cooldownKey = `${zone.id}:${zone.direction}`;
    const lastAt = lastAlertAt.get(cooldownKey) || 0;
    if (now - lastAt < ZONE_TOUCH_COOLDOWN_MS) continue;
    if (state.pendingConfirmations.some(item => item.zoneId === zone.id && !item.completed)) continue;

    const atr = pairState.atr5m || Math.abs(zone.top - zone.bottom) || price * 0.0005;
    const buffer = Math.max(atr * 0.15, price * 0.00005);
    const sl = zone.direction === 'BUY' ? zone.bottom - buffer : zone.top + buffer;
    const risk = zone.direction === 'BUY' ? price - sl : sl - price;
    if (risk <= 0) continue;
    const tp = zone.direction === 'BUY' ? price + RR * risk : price - RR * risk;

    lastAlertAt.set(cooldownKey, now);
    zone.alerted = true;

    const pending = {
      id: `${now}-${pair}-${zone.direction}-touch`,
      zoneId: zone.id,
      pair, direction: zone.direction, timeframe: zone.timeframe,
      touchedAt: now, touchPrice: price, zoneTop: zone.top, zoneBottom: zone.bottom,
      eventType: zone.eventType || 'OB', rangePosition: zone.rangePosition || 'UNKNOWN',
      sl, tp, completed: false
    };
    state.pendingConfirmations.push(pending);
    state.pendingConfirmations = state.pendingConfirmations
      .filter(item => now - item.touchedAt < CONFIRMATION_MAX_AGE_MS * 2)
      .slice(-200);

    const signal = {
      id: pending.id, time: new Date(now).toISOString(), pair,
      direction: zone.direction, timeframe: zone.timeframe, type: 'ZONE_TOUCH',
      entry: price, sl, tp, zoneTop: zone.top, zoneBottom: zone.bottom,
      note: 'Immediate zone-touch warning; wait for separate 5M rejection confirmation.'
    };
    state.signals.unshift(signal);
    state.signals = state.signals.slice(0, 100);

    const message = [
      `🔔 ZONE TOUCH — ${zone.direction} ${prettyPair(pair)}`,
      `Zone source: ${zone.timeframe} ${zone.direction === 'BUY' ? 'DEMAND' : 'SUPPLY'}`,
      `Structure event: ${zone.eventType || 'OB'}${zone.rangeSweep ? ` | ${zone.rangeSweep}` : ''}`,
      `Range position: ${zone.rangePosition || 'UNKNOWN'}`,
      `Live price at touch: ${fmtPrice(pair, price)}`,
      `Zone: ${fmtPrice(pair, zone.bottom)} – ${fmtPrice(pair, zone.top)}`,
      `Next: waiting for a completed 5M rejection candle.`,
      `Price source: Deriv`,
      `⚠️ Warning only; this is not yet a confirmed entry.`
    ].join('\n');

    sendTelegram(message)
      .then(ok => log(`${ok ? 'Telegram sent' : 'Signal recorded'}: ZONE TOUCH ${zone.direction} ${pair}`))
      .catch(err => {
        log(`Telegram send failed: ${err.message}`);
        state.lastError = `Telegram: ${err.message}`;
      });
  }
}

// Checks every closed 5M candle since the touch, not just the latest one.
// Each pending touch can fire at most one confirmation.
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

      const entry = candle.close;
      const risk = pending.direction === 'BUY' ? entry - pending.sl : pending.sl - entry;
      if (risk <= 0) {
        log(`Confirmation skipped: invalid risk for ${pair}`);
        continue;
      }
      const tp = pending.direction === 'BUY' ? entry + RR * risk : entry - RR * risk;

      pending.completed = true;

      const signal = {
        id: `${pending.id}-confirmed`, time: new Date().toISOString(), pair,
        direction: pending.direction, timeframe: '5M', type: 'CONFIRMED_ENTRY',
        entry, sl: pending.sl, tp, zoneTop: pending.zoneTop, zoneBottom: pending.zoneBottom,
        note: 'Completed 5M rejection candle detected; confirm execution conditions.'
      };
      state.signals.unshift(signal);
      state.signals = state.signals.slice(0, 100);

      const message = [
        `✅ 5M REJECTION CONFIRMED — ${pending.direction} ${prettyPair(pair)}`,
        `Reference entry (5M close): ${fmtPrice(pair, entry)}`,
        `Stop Loss: ${fmtPrice(pair, pending.sl)}`,
        `Take Profit (1:${RR}): ${fmtPrice(pair, tp)}`,
        `Zone: ${fmtPrice(pair, pending.zoneBottom)} – ${fmtPrice(pair, pending.zoneTop)}`,
        `Structure event: ${pending.eventType || 'OB'} | Range: ${pending.rangePosition || 'UNKNOWN'}`,
        `Confirmation candle: ${new Date(candle.epoch * 1000).toISOString()}`,
        `Price source: Deriv`,
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

function updateMitigation(pair, price) {
  for (const zone of state.zones) {
    if (zone.pair !== pair || zone.mitigated) continue;
    const fullyBroken = zone.direction === 'BUY' ? price < zone.bottom : price > zone.top;
    if (fullyBroken) zone.mitigated = true;
  }
}

async function resolveSymbols() {
    const response = await deriv.request({
    active_symbols: 'brief'
  });
  const symbols = response.active_symbols || [];
  state.activeSymbolsCount = symbols.length;

  const available = new Map();
  for (const item of symbols) {
    const symbol = String(item.symbol || item.underlying_symbol || '');
    const display = String(item.display_name || item.underlying_symbol_name || item.name || '');
    const market = String(item.market || '').toLowerCase();
    const submarket = String(item.submarket || '').toLowerCase();

    // Never use OTC instruments.
    if (display.toUpperCase().includes('OTC') || symbol.toUpperCase().includes('OTC')) continue;
    if (submarket.includes('otc')) continue;

    const stockCandidate = normalizeStock(`${symbol} ${display}`);
    if (stockCandidate && (market.includes('stock') || market.includes('financial') || /\.(OQ|N)$/.test(symbol))) {
      if (!available.has(stockCandidate)) available.set(stockCandidate, symbol);
      continue;
    }

    const pair = normalizePair(display);
    const symbolPair = normalizePair(symbol.replace(/^frx/i, ''));
    const candidate = pair.length === 6 ? pair : symbolPair.length === 6 ? symbolPair : '';
    if (!candidate || !/^[A-Z]{6}$/.test(candidate)) continue;
    if (market && !market.includes('forex') && !market.includes('financial')) continue;
    if (!available.has(candidate)) available.set(candidate, symbol);
  }

  for (const pair of TARGET_PAIRS) {
    const symbol = available.get(pair);
    state.pairs[pair] = {
      pair,
      display: prettyPair(pair),
      symbol: symbol || null,
      status: symbol ? 'Waiting for first scan' : 'Unavailable on Deriv',
      price: null, priceTime: null, lastScan: null, error: ''
    };
  }

  const resolved = TARGET_PAIRS.filter(pair => state.pairs[pair].symbol);
  const missing = TARGET_PAIRS.filter(pair => !state.pairs[pair].symbol);
  log(`Deriv resolved ${resolved.length}/${TARGET_PAIRS.length} target pairs.`);
  if (missing.length) log(`Unavailable pairs: ${missing.map(prettyPair).join(', ')}`);

  deriv.subscribeTicks(resolved.map(pair => state.pairs[pair].symbol));
}

async function scanPair(pair) {
  const p = state.pairs[pair];
  if (!p || !p.symbol) return;
  try {
    const allZones = [];
    for (const tf of TIMEFRAMES) {
      const candles = await deriv.history(p.symbol, tf.seconds);
      await sleep(REQUEST_GAP_MS);
      if (tf.label === '15M') {
        const atr = calcATR(candles);
        if (atr) p.atr15m = atr;
      }
      allZones.push(...makeZones(pair, tf, candles));
    }

    const candles5m = await deriv.history(p.symbol, 300);
    await sleep(REQUEST_GAP_MS);
    const atr5m = calcATR(candles5m);
    if (atr5m) p.atr5m = atr5m;
    confirmRejections(pair, candles5m);

    // Carry over the mitigated flag for zones that were already broken.
    const oldForPair = state.zones.filter(z => z.pair === pair);
    const newById = new Map(allZones.map(z => [z.id, z]));
    for (const old of oldForPair) {
      if (old.mitigated) {
        const existing = newById.get(old.id);
        if (existing) existing.mitigated = true;
      }
    }
    state.zones = state.zones.filter(z => z.pair !== pair).concat(allZones);
    if (Number.isFinite(p.price)) updateMitigation(pair, p.price);

    p.status = 'Scanning';
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
    const pairs = TARGET_PAIRS.filter(pair => state.pairs[pair] && state.pairs[pair].symbol);
    // Three workers keeps load on the public API reasonable.
    let cursor = 0;
    const workers = Array.from({ length: 3 }, async () => {
      while (cursor < pairs.length) {
        const pair = pairs[cursor++];
        await scanPair(pair);
      }
    });
    await Promise.all(workers);
    state.lastScan = new Date().toISOString();
    log(`Scan completed. Active configured pairs: ${pairs.length}. Zones tracked: ${state.zones.length}.`);
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
    targetForexCount: TARGET_PAIRS.length - STOCKS.length,
    targetStocks: STOCKS,
    pairs: Object.values(state.pairs),
    zones: state.zones.slice().sort((a, b) => b.createdAt - a.createdAt),
    structureSummary: state.zones.reduce((acc, z) => {
      const key = z.eventType || 'OB';
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {}),
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
