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
// DEBUG HELPERS — show everything on screen since mobile has no console
// ============================================================
function setStatus(msg) {
  statusBox.textContent = msg;
}

window.onerror = function (message, source, lineno, colno, error) {
  setStatus('خطای جاوااسکریپت: ' + message + ' (خط ' + lineno + ')');
};
window.addEventListener('unhandledrejection', (event) => {
  setStatus('خطای Promise: ' + (event.reason?.message || JSON.stringify(event.reason)));
});

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

// If nothing happens after a few seconds, tell the user clearly
setTimeout(() => {
  if (!handshakeDone) {
    setStatus('هاست cTrader به درخواست register جواب نداد (timeout). این پلاگین احتمالاً بیرون از محیط cTrader باز شده یا SDK با نسخه‌ی هاست هماهنگ نیست.');
  }
}, 6000);

// ============================================================
// 2) Load symbol list -> fill dropdown
// ============================================================
function loadSymbols() {
  getLightSymbolList(adapter, {}).pipe(take(1)).subscribe((res) => {
    symbols = res.symbol || res.symbols || [];
    symbolSelect.innerHTML = symbols
      .map((s) => `<option value="${s.symbolId}">${s.symbolName || s.name}</option>`)
      .join('');
    if (symbols.length) loadSymbolDetails(symbols[0].symbolId);
  });
}

symbolSelect.addEventListener('change', () => {
  loadSymbolDetails(Number(symbolSelect.value));
});

// ============================================================
// 3) Full symbol details (digits, pip position, volume limits)
// ============================================================
function loadSymbolDetails(symbolId) {
  getSymbol(adapter, { symbolId: [symbolId] }).pipe(take(1)).subscribe((res) => {
    currentSymbol = (res.symbol || res.symbols || [])[0];
    subscribeToQuotes(symbolId);
    recalculate();
  });
}

// ============================================================
// 4) Live quotes
// ============================================================
let quoteSub;
function subscribeToQuotes(symbolId) {
  subscribeQuotes(adapter, { symbolId: [symbolId] }).pipe(take(1)).subscribe();
  if (quoteSub) quoteSub.unsubscribe();
  quoteSub = quoteEvent(adapter).subscribe((q) => {
    if (q.symbolId !== symbolId) return;
    if (q.bid != null) liveBid = fromServerPrice(q.bid, currentSymbol);
    if (q.ask != null) liveAsk = fromServerPrice(q.ask, currentSymbol);
    bidPriceEl.textContent = liveBid ?? '--';
    askPriceEl.textContent = liveAsk ?? '--';
    recalculate();
  });
}

function fromServerPrice(raw, sym) {
  if (!sym) return raw;
  return Number(raw) / Math.pow(10, sym.digits);
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
  const entryPrice = selectedSide === 'SELL' ? liveBid : liveAsk; // sell fills on bid, buy fills on ask

  if (!currentSymbol || !selectedSide || !risk || !sl || !entryPrice) {
    lotResultEl.textContent = '--';
    updateConfirmButton();
    return;
  }

  // Sanity check: SL must be on the correct side of price
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
  const contractSize = currentSymbol.lotSize || 100000;

  // TODO (cross pairs): convert risk/priceDistance through the
  // conversion rate here if account currency !== quote currency.
  const volumeInUnits = risk / priceDistance;
  let volumeInLots = volumeInUnits / contractSize;

  const minVol = (currentSymbol.minVolume || 1000) / 100 / contractSize;
  const stepVol = (currentSymbol.stepVolume || 1000) / 100 / contractSize;
  const maxVol = (currentSymbol.maxVolume || 10000000) / 100 / contractSize;

  volumeInLots = Math.max(minVol, Math.min(maxVol, volumeInLots));
  volumeInLots = Math.round(volumeInLots / stepVol) * stepVol;

  lotResultEl.textContent = volumeInLots.toFixed(2) + ' لات';
  lotResultEl.dataset.units = Math.round(volumeInLots * contractSize);
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
  const tp = parseFloat(tpInput.value); // optional
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
        statusBox.textContent = 'سفارش ارسال شد ✔';
        confirmBtn.disabled = false;
      },
      error: (err) => {
        statusBox.textContent = 'خطا در ارسال سفارش: ' + (err?.errorCode || err);
        confirmBtn.disabled = false;
      },
    });
});
