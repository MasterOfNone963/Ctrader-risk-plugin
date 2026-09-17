import { createClientAdapter } from '@spotware-web-team/sdk-external-api';
import {
  handleConfirmEvent,
  registerEvent,
  getLightSymbolList,
  getSymbol,
  subscribeQuotes,
  quoteEvent,
  createNewOrder,
  getTrendbarList,
  getAccountInformation,
} from '@spotware-web-team/sdk';
import { take, tap, catchError } from 'rxjs/operators';
import { createLogger } from '@veksa/logger';

// ---------- DOM refs ----------
const symbolSelect = document.getElementById('symbolSelect');
const bidPriceEl = document.getElementById('bidPrice');
const askPriceEl = document.getElementById('askPrice');
const buySideBtn = document.getElementById('buySideBtn');
const sellSideBtn = document.getElementById('sellSideBtn');
const slInput = document.getElementById('slInput');
const tpInput = document.getElementById('tpInput');
const riskInput = document.getElementById('riskInput');
const lotResultEl = document.getElementById('lotResult');
const warnBox = document.getElementById('warnBox');
const marginInfoEl = document.getElementById('marginInfo');
const debugBox = document.getElementById('debugBox');
const currCandleEl = document.getElementById('currCandle');
const prevCandleEl = document.getElementById('prevCandle');
const confirmBtn = document.getElementById('confirmBtn');
const statusBox = document.getElementById('statusBox');

// ---------- state ----------
let adapter;
let symbols = [];
let currentSymbol = null;
let liveBid = null;
let liveAsk = null;
let selectedSide = null; // 'BUY' | 'SELL'
let accountBalance = null;
let lastAutoTP = null;

// ============================================================
// DEBUG HELPERS
// ============================================================
function setStatus(msg) {
  statusBox.textContent = msg;
}
// Debug messages accumulate (rather than overwrite each other) so
// multiple independent issues stay visible at once.
function logDebug(msg) {
  const prev = debugBox.textContent;
  debugBox.textContent = prev ? prev + '\n---\n' + msg : msg;
}
window.onerror = function (message, source, lineno) {
  setStatus('خطای جاوااسکریپت: ' + message + ' (خط ' + lineno + ')');
};
window.addEventListener('unhandledrejection', (event) => {
  setStatus('خطای Promise: ' + (event.reason?.message || JSON.stringify(event.reason)));
});

// ============================================================
// FIELD-ACCESS HELPERS
// The cTrader host sends raw protobuf-style JSON: fields are
// PascalCase (Id, Name, Digits...) and the real data usually sits
// one level deeper, under `.payload`. These helpers read a value
// under any of several possible key spellings so the UI works
// regardless of exact casing.
// ============================================================
function unwrap(res) {
  return res?.payload ?? res;
}
function ciGet(obj, name) {
  if (!obj) return undefined;
  const key = Object.keys(obj).find((k) => k.toLowerCase() === name.toLowerCase());
  return key !== undefined ? obj[key] : undefined;
}
function pick(obj, ...keys) {
  if (!obj) return undefined;
  for (const k of keys) {
    if (obj[k] !== undefined) return obj[k];
  }
  // fallback: case-insensitive match against every key name given
  for (const k of keys) {
    const v = ciGet(obj, k);
    if (v !== undefined) return v;
  }
  return undefined;
}

// ============================================================
// 1) Connect to the cTrader host (handshake)
// ============================================================
setStatus('در حال ساخت adapter...');
let handshakeDone = false;

try {
  const logger = createLogger(false);
  adapter = createClientAdapter({ logger });
  setStatus('adapter ساخته شد، در حال ارسال register...');
} catch (e) {
  setStatus('خطا در ساخت adapter: ' + e.message);
}

handleConfirmEvent(adapter, {}).pipe(take(1)).subscribe({
  error: (e) => setStatus('خطا در confirm اولیه: ' + (e?.message || e)),
});

registerEvent(adapter)
  .pipe(
    take(1),
    tap(() => {
      handshakeDone = true;
      handleConfirmEvent(adapter, {}).pipe(take(1)).subscribe();
      setStatus('متصل به cTrader ✔');
      loadSymbols();
      loadAccountInfo();
    }),
    catchError((err) => {
      setStatus('خطا در register: ' + (err?.message || JSON.stringify(err)));
      return [];
    })
  )
  .subscribe();

setTimeout(() => {
  if (!handshakeDone) {
    setStatus('هاست cTrader به درخواست register جواب نداد (timeout).');
  }
}, 6000);

// ============================================================
// 2) Load symbol list -> fill dropdown
// ============================================================
// Fetch account balance once, used later for the margin/affordability check.
function loadAccountInfo() {
  let responded = false;
  getAccountInformation(adapter, {})
    .pipe(take(1))
    .subscribe({
      next: (res) => {
        responded = true;
        const data = unwrap(res);
        const balRaw = pick(data, 'Balance', 'balance');
        if (balRaw == null) {
          logDebug('دیباگ حساب: فیلد Balance پیدا نشد. پاسخ: ' + JSON.stringify(res).slice(0, 400));
          return;
        }
        accountBalance = Number(balRaw) / 100;
        recalculate();
      },
      error: (err) => {
        responded = true;
        logDebug('خطا در getAccountInformation: ' + (err?.message || JSON.stringify(err)));
      },
    });
  setTimeout(() => {
    if (!responded) {
      logDebug('هاست به درخواست getAccountInformation بعد از ۶ ثانیه جواب نداد.');
    }
  }, 6000);
}

function loadSymbols() {
  getLightSymbolList(adapter, {}).pipe(take(1)).subscribe({
    next: (res) => {
      const data = unwrap(res);
      symbols = pick(data, 'Symbol', 'symbol', 'Symbols', 'symbols') || [];
      if (!symbols.length) {
        logDebug('دیباگ getLightSymbolList: ' + JSON.stringify(res).slice(0, 500));
        return;
      }
      symbolSelect.innerHTML = symbols
        .map((s) => {
          const id = pick(s, 'Id', 'id', 'SymbolId', 'symbolId');
          const name = pick(s, 'Name', 'name', 'SymbolName', 'symbolName');
          return `<option value="${id}">${name}</option>`;
        })
        .join('');
      // Default to US100 if it exists in the list; otherwise fall back to the first symbol.
      const defaultSymbol = symbols.find((s) => {
        const name = pick(s, 'Name', 'name', 'SymbolName', 'symbolName');
        return (name || '').toUpperCase() === 'US100';
      }) || symbols[0];
      const defaultId = pick(defaultSymbol, 'Id', 'id', 'SymbolId', 'symbolId');
      symbolSelect.value = defaultId;
      loadSymbolDetails(defaultId);
    },
    error: (err) => {
      logDebug('خطا در getLightSymbolList: ' + (err?.message || JSON.stringify(err)));
    },
  });
}

symbolSelect.addEventListener('change', () => {
  loadSymbolDetails(Number(symbolSelect.value));
});

// ============================================================
// 3) Full symbol details (digits, pip position, volume limits)
// ============================================================
function loadSymbolDetails(symbolId) {
  getSymbol(adapter, { symbolId: [symbolId] }).pipe(take(1)).subscribe({
    next: (res) => {
      const data = unwrap(res);
      const list = pick(data, 'Symbol', 'symbol', 'Symbols', 'symbols') || [];
      const raw = list[0];
      if (!raw) {
        logDebug('دیباگ getSymbol: ' + JSON.stringify(res).slice(0, 500));
        return;
      }
      currentSymbol = {
        symbolId,
        lotSize: pick(raw, 'LotSize', 'lotSize') || 10000000,
        minVolume: pick(raw, 'MinVolume', 'minVolume'),
        maxVolume: pick(raw, 'MaxVolume', 'maxVolume'),
        stepVolume: pick(raw, 'StepVolume', 'stepVolume'),
        leverage: pick(raw, 'Leverage', 'leverage'),
      };
      debugBox.textContent = '';
      subscribeToQuotes(symbolId);
      currCandleHigh = null;
      currCandleLow = null;
      currCandlePeriodStart = null;
      loadPrevCandle(symbolId);
      recalculate();
    },
    error: (err) => {
      logDebug('خطا در getSymbol: ' + (err?.message || JSON.stringify(err)));
    },
  });
}

// ============================================================
// 4) Live quotes
// ============================================================
let quoteSub;
function subscribeToQuotes(symbolId) {
  subscribeQuotes(adapter, { symbolId: [symbolId] }).pipe(take(1)).subscribe({
    error: (err) => {
      logDebug('خطا در subscribeQuotes: ' + (err?.message || JSON.stringify(err)));
    },
  });
  if (quoteSub) quoteSub.unsubscribe();
  quoteSub = quoteEvent(adapter).subscribe((res) => {
    const q = unwrap(res);
    const qSymbolId = pick(q, 'SymbolId', 'symbolId');
    if (qSymbolId !== symbolId) return;
    const bid = pick(q, 'Bid', 'bid');
    const ask = pick(q, 'Ask', 'ask');
    // Ignore spurious zero/empty ticks (e.g. keep-alive pings) —
    // real bid/ask prices are never 0.
    if (bid != null && Number(bid) > 0) liveBid = fromServerPrice(bid);
    if (ask != null && Number(ask) > 0) liveAsk = fromServerPrice(ask);
    bidPriceEl.textContent = liveBid ?? '--';
    askPriceEl.textContent = liveAsk ?? '--';
    if (liveBid != null && liveBid > 0) trackLiveCandle(liveBid);
    recalculate();
  });
}

// cTrader always transmits prices scaled by a fixed factor of 100000,
// regardless of the symbol's own "Digits" field.
function fromServerPrice(raw) {
  return Number(raw) / 100000;
}

// ============================================================
// Candle high/low — tracked entirely from the live quote ticks we
// already receive. No extra server call, so nothing new can break.
// ============================================================
const CANDLE_MINUTES = 5;
let currCandleHigh = null;
let currCandleLow = null;
let currCandlePeriodStart = null;

function periodStartFor(date) {
  const ms = CANDLE_MINUTES * 60 * 1000;
  return Math.floor(date.getTime() / ms) * ms;
}

function trackLiveCandle(price) {
  const start = periodStartFor(new Date());
  if (currCandlePeriodStart !== start) {
    // New period started — the old "current" candle becomes "prev".
    if (currCandlePeriodStart != null && currCandleHigh != null) {
      prevCandleEl.textContent = `prev h:${currCandleHigh.toFixed(2)} l:${currCandleLow.toFixed(2)}`;
    }
    currCandlePeriodStart = start;
    currCandleHigh = price;
    currCandleLow = price;
  } else {
    currCandleHigh = Math.max(currCandleHigh, price);
    currCandleLow = Math.min(currCandleLow, price);
  }
  currCandleEl.textContent = `now h:${currCandleHigh.toFixed(2)} l:${currCandleLow.toFixed(2)}`;
}

// Fetch real M5 history once when a symbol loads: shows "prev" (last
// fully-completed candle) immediately, and seeds the live "now"
// tracker with the real in-progress candle's current high/low
// instead of starting fresh from whatever price happens to arrive
// first — so "now" stays accurate even if the panel was opened
// mid-candle.
function loadPrevCandle(symbolId) {
  const now = Date.now();
  const fromTs = now - 30 * 60 * 1000; // last 30 minutes, enough for several M5 bars
  getTrendbarList(adapter, {
    symbolId,
    period: 5, // ProtoOATrendbarPeriod enum: M5 = 5
    fromTimestamp: fromTs,
    toTimestamp: now,
  })
    .pipe(take(1))
    .subscribe({
      next: (res) => {
        const data = unwrap(res);
        const bars = pick(data, 'Trendbar', 'trendbar', 'Trendbars', 'trendbars') || [];
        if (!bars.length) {
          logDebug('دیباگ کندل: پاسخ خالی بود. ' + JSON.stringify(res).slice(0, 400));
          return;
        }
        const sorted = [...bars].sort((a, b) => {
          const ta = pick(a, 'UtcTimestampInMinutes', 'utcTimestampInMinutes') || 0;
          const tb = pick(b, 'UtcTimestampInMinutes', 'utcTimestampInMinutes') || 0;
          return ta - tb;
        });
        const barToHL = (bar) => {
          const low = pick(bar, 'Low', 'low');
          const deltaHigh = pick(bar, 'DeltaHigh', 'deltaHigh') || 0;
          if (low == null || Number(low) <= 0) return null;
          return {
            low: fromServerPrice(low),
            high: fromServerPrice(Number(low) + Number(deltaHigh)),
          };
        };

        if (sorted.length >= 2) {
          const prevHL = barToHL(sorted[sorted.length - 2]);
          if (prevHL) {
            prevCandleEl.textContent = `prev h:${prevHL.high.toFixed(2)} l:${prevHL.low.toFixed(2)}`;
          }
        }

        const currBar = sorted[sorted.length - 1];
        const currHL = barToHL(currBar);
        if (currHL) {
          currCandlePeriodStart = periodStartFor(new Date());
          currCandleHigh = currHL.high;
          currCandleLow = currHL.low;
          currCandleEl.textContent = `now h:${currCandleHigh.toFixed(2)} l:${currCandleLow.toFixed(2)}`;
        }
      },
      error: (err) => {
        logDebug('خطا در getTrendbarList: ' + (err?.message || JSON.stringify(err)));
      },
    });
}

// ============================================================
// 5) Side selection (Buy / Sell) — step 1
// ============================================================
function selectSide(side) {
  selectedSide = side;
  buySideBtn.classList.toggle('active', side === 'BUY');
  sellSideBtn.classList.toggle('active', side === 'SELL');
  updateConfirmButton();
  recalculate();
}
buySideBtn.addEventListener('click', () => selectSide('BUY'));
sellSideBtn.addEventListener('click', () => selectSide('SELL'));

// ============================================================
// 6) Risk -> lot size calculation
//
// ASSUMPTION: account currency == symbol quote currency
// (e.g. USD account on EURUSD/XAUUSD). For cross pairs, a
// conversion step is needed — marked as TODO below.
// ============================================================
function recalculate() {
  warnBox.textContent = '';
  marginInfoEl.textContent = '';
  const risk = parseFloat(riskInput.value);
  const sl = parseFloat(slInput.value);
  const entryPrice = selectedSide === 'SELL' ? liveBid : liveAsk;

  // Auto-calc TP1 (1:1 risk:reward) — independent of the risk field,
  // so it updates as soon as SL/side/price are known, and keeps
  // following the live price. Skipped if the user has typed their
  // own TP value that doesn't match our last auto-computed one.
  if (currentSymbol && selectedSide && sl && entryPrice) {
    const distance = Math.abs(entryPrice - sl);
    const rrRatio = 1; // 1:1 per the user's choice
    const autoTP = selectedSide === 'BUY' ? entryPrice + distance * rrRatio : entryPrice - distance * rrRatio;
    const roundedTP = parseFloat(autoTP.toFixed(5));
    const currentTpText = tpInput.value.trim();
    if (currentTpText === '' || (lastAutoTP != null && parseFloat(currentTpText) === lastAutoTP)) {
      tpInput.value = roundedTP;
      lastAutoTP = roundedTP;
    }

    // Max affordable risk — independent of whatever is in the risk
    // box right now, purely from SL distance + live price + account
    // balance + leverage. Updates live with every price tick.
    if (accountBalance != null && currentSymbol.leverage) {
      const maxRisk = (accountBalance * distance * currentSymbol.leverage) / entryPrice;
      marginInfoEl.textContent = `تا $${maxRisk.toFixed(2)} دلار می‌تونی با این SL ریسک کنی`;
    } else if (accountBalance == null) {
      marginInfoEl.textContent = 'در حال دریافت موجودی حساب...';
    }
  }

  if (!currentSymbol || !selectedSide || !risk || !sl || !entryPrice) {
    lotResultEl.textContent = '--';
    updateConfirmButton();
    return;
  }

  if (selectedSide === 'BUY' && sl >= entryPrice) {
    warnBox.textContent = 'برای Buy، قیمت SL باید پایین‌تر از قیمت فعلی باشد';
    lotResultEl.textContent = '--';
    updateConfirmButton();
    return;
  }
  if (selectedSide === 'SELL' && sl <= entryPrice) {
    warnBox.textContent = 'برای Sell، قیمت SL باید بالاتر از قیمت فعلی باشد';
    lotResultEl.textContent = '--';
    updateConfirmButton();
    return;
  }

  const priceDistance = Math.abs(entryPrice - sl);
  const rawLotSize = currentSymbol.lotSize || 10000000;
  // For RISK math we need the REAL contract size (instrument units
  // per 1.0 lot) — the raw LotSize field is scaled by 100.
  const realContractSize = rawLotSize / 100;

  const volumeInUnits = risk / priceDistance;
  let volumeInLots = volumeInUnits / realContractSize;

  const minVol = (currentSymbol.minVolume || 100) / 100;
  const stepVol = (currentSymbol.stepVolume || 100) / 100;
  const maxVol = (currentSymbol.maxVolume || 10000000) / 100;

  volumeInLots = Math.max(minVol, Math.min(maxVol, volumeInLots));
  volumeInLots = Math.round(volumeInLots / stepVol) * stepVol;

  lotResultEl.textContent = volumeInLots.toFixed(2) + ' لات';
  // The order's "volume" field expects the RAW LotSize scale directly
  // (this is the value that has already been confirmed to execute
  // correctly against the broker).
  lotResultEl.dataset.units = Math.round(volumeInLots * rawLotSize);

  // Flag it if the risk they actually typed exceeds what the account can afford.
  if (accountBalance != null && currentSymbol.leverage) {
    const maxRisk = (accountBalance * priceDistance * currentSymbol.leverage) / entryPrice;
    if (risk > maxRisk) {
      warnBox.textContent = `⚠️ ریسک واردشده ($${risk}) بیشتر از حداکثر مجاز ($${maxRisk.toFixed(2)}) است.`;
    }
  }

  updateConfirmButton();
}

riskInput.addEventListener('input', recalculate);
slInput.addEventListener('input', recalculate);
tpInput.addEventListener('input', recalculate);

// ============================================================
// 7) Confirm button state + label
// ============================================================
function updateConfirmButton() {
  if (!selectedSide) {
    confirmBtn.disabled = true;
    confirmBtn.textContent = 'ابتدا جهت معامله را انتخاب کنید';
    return;
  }
  const hasLot = lotResultEl.dataset.units && Number(lotResultEl.dataset.units) > 0;
  confirmBtn.disabled = !hasLot;
  confirmBtn.textContent = hasLot
    ? `ثبت پوزیشن ${selectedSide === 'BUY' ? 'Buy' : 'Sell'} — ${lotResultEl.textContent}`
    : 'مقادیر SL و ریسک را کامل کنید';
}

// ============================================================
// 8) Send the order (Market + your SL + optional TP)
// ============================================================
confirmBtn.addEventListener('click', () => {
  const sl = parseFloat(slInput.value);
  const tp = parseFloat(tpInput.value);
  const units = Number(lotResultEl.dataset.units);

  if (!currentSymbol || !selectedSide || !units || !sl) {
    warnBox.textContent = 'اطلاعات ناقص است';
    return;
  }

  const orderPayload = {
    symbolId: currentSymbol.symbolId,
    orderType: 'MARKET',
    tradeSide: selectedSide,
    volume: units,
    stopLoss: sl,
  };
  if (!isNaN(tp) && tp > 0) {
    orderPayload.takeProfit = tp;
  }

  confirmBtn.disabled = true;
  createNewOrder(adapter, orderPayload)
    .pipe(take(1))
    .subscribe({
      next: () => {
        setStatus('سفارش ارسال شد ✔');
        confirmBtn.disabled = false;
      },
      error: (err) => {
        setStatus('خطا در ارسال سفارش: ' + (err?.errorCode || err?.message || JSON.stringify(err)));
        confirmBtn.disabled = false;
      },
    });
});
