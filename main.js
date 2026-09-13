import { createClientAdapter } from '@spotware-web-team/sdk-external-api';
import {
  handleConfirmEvent,
  registerEvent,
  getLightSymbolList,
  getSymbol,
  subscribeQuotes,
  quoteEvent,
  createNewOrder,
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
const debugBox = document.getElementById('debugBox');
const confirmBtn = document.getElementById('confirmBtn');
const statusBox = document.getElementById('statusBox');

// ---------- state ----------
let adapter;
let symbols = [];
let currentSymbol = null;
let liveBid = null;
let liveAsk = null;
let selectedSide = null; // 'BUY' | 'SELL'

// ============================================================
// DEBUG HELPERS
// ============================================================
function setStatus(msg) {
  statusBox.textContent = msg;
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
function loadSymbols() {
  getLightSymbolList(adapter, {}).pipe(take(1)).subscribe({
    next: (res) => {
      const data = unwrap(res);
      symbols = pick(data, 'Symbol', 'symbol', 'Symbols', 'symbols') || [];
      if (!symbols.length) {
        debugBox.textContent = 'دیباگ getLightSymbolList: ' + JSON.stringify(res).slice(0, 500);
        return;
      }
      symbolSelect.innerHTML = symbols
        .map((s) => {
          const id = pick(s, 'Id', 'id', 'SymbolId', 'symbolId');
          const name = pick(s, 'Name', 'name', 'SymbolName', 'symbolName');
          return `<option value="${id}">${name}</option>`;
        })
        .join('');
      const firstId = pick(symbols[0], 'Id', 'id', 'SymbolId', 'symbolId');
      loadSymbolDetails(firstId);
    },
    error: (err) => {
      debugBox.textContent = 'خطا در getLightSymbolList: ' + (err?.message || JSON.stringify(err));
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
        debugBox.textContent = 'دیباگ getSymbol: ' + JSON.stringify(res).slice(0, 500);
        return;
      }
      currentSymbol = {
        symbolId,
        lotSize: pick(raw, 'LotSize', 'lotSize') || 10000000,
        minVolume: pick(raw, 'MinVolume', 'minVolume'),
        maxVolume: pick(raw, 'MaxVolume', 'maxVolume'),
        stepVolume: pick(raw, 'StepVolume', 'stepVolume'),
      };
      debugBox.textContent = '';
      subscribeToQuotes(symbolId);
      recalculate();
    },
    error: (err) => {
      debugBox.textContent = 'خطا در getSymbol: ' + (err?.message || JSON.stringify(err));
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
      debugBox.textContent = 'خطا در subscribeQuotes: ' + (err?.message || JSON.stringify(err));
    },
  });
  if (quoteSub) quoteSub.unsubscribe();
  quoteSub = quoteEvent(adapter).subscribe((res) => {
    const q = unwrap(res);
    const qSymbolId = pick(q, 'SymbolId', 'symbolId');
    if (qSymbolId !== symbolId) return;
    const bid = pick(q, 'Bid', 'bid');
    const ask = pick(q, 'Ask', 'ask');
    if (bid != null) liveBid = fromServerPrice(bid);
    if (ask != null) liveAsk = fromServerPrice(ask);
    bidPriceEl.textContent = liveBid ?? '--';
    askPriceEl.textContent = liveAsk ?? '--';
    recalculate();
  });
}

// cTrader always transmits prices scaled by a fixed factor of 100000,
// regardless of the symbol's own "Digits" field.
function fromServerPrice(raw) {
  return Number(raw) / 100000;
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
  const risk = parseFloat(riskInput.value);
  const sl = parseFloat(slInput.value);
  const entryPrice = selectedSide === 'SELL' ? liveBid : liveAsk;

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
