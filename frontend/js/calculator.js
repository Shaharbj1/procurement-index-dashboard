import { api, showToast, fmtPct, fmtVal, pctClass, downloadUrl, sourceLink, SEGMENT_BADGES } from './api.js';

let sparkChart = null;
let lastResult = null;
let lastIndexDetail = null;

// ── Populate index dropdown ────────────────────────────────────────────────
let _allIndices = [];

async function loadIndices() {
  try {
    _allIndices = await api.get('/indices');
    const select = document.getElementById('calc-index');

    // Group by segment
    const groups = {};
    _allIndices.forEach(idx => {
      if (!groups[idx.segment]) groups[idx.segment] = [];
      groups[idx.segment].push(idx);
    });

    const SEGMENT_ORDER = ['secondary_packaging','emn','logistics','primary_pkg_md','api_chemicals','regional'];
    SEGMENT_ORDER.forEach(seg => {
      const items = groups[seg];
      if (!items || !items.length) return;
      const segMeta = SEGMENT_BADGES[seg] || { label: seg };
      const og = document.createElement('optgroup');
      og.label = segMeta.label || seg;
      items.forEach(idx => {
        const opt = document.createElement('option');
        opt.value = idx.id;
        opt.dataset.periodType = idx.period_type || 'monthly';
        opt.textContent = idx.name + (idx.paid_source ? ' 🔒' : '');
        og.appendChild(opt);
      });
      select.appendChild(og);
    });

    // Pre-select from URL param
    const params = new URLSearchParams(location.search);
    const preId = params.get('index_id');
    if (preId) {
      select.value = preId;
      updatePeriodPicker(preId);
    }
  } catch (err) {
    showToast('Failed to load indices: ' + err.message, 'error');
  }
}

// ── Period picker (monthly / quarterly / semi-annual) ──────────────────────
function updatePeriodPicker(indexId) {
  const idx = _allIndices.find(i => i.id === indexId);
  const pt  = idx ? (idx.period_type || 'monthly') : 'monthly';
  _buildPicker('calc-start-wrap', 'calc-start-label', pt, 'start');
  _buildPicker('calc-end-wrap',   'calc-end-label',   pt, 'end');
}

function _buildPicker(wrapId, labelId, periodType, which) {
  const wrap  = document.getElementById(wrapId);
  const label = document.getElementById(labelId);
  if (!wrap) return;

  if (periodType === 'monthly') {
    label.textContent = `${which === 'start' ? 'Start' : 'End'} Period (YYYY-MM)`;
    wrap.innerHTML = `<input type="month" id="calc-${which}" placeholder="${which === 'start' ? '2022-01' : '2024-12'}" style="width:100%"/>`;
    return;
  }

  const currentYear = new Date().getFullYear();
  const years = Array.from({length: 20}, (_, i) => currentYear - 15 + i);

  if (periodType === 'quarterly') {
    label.textContent = `${which === 'start' ? 'Start' : 'End'} Quarter`;
    wrap.innerHTML = `
      <div style="display:flex;gap:6px">
        <select id="calc-${which}-year" style="flex:1;padding:8px;border:1px solid var(--border);border-radius:4px">
          ${years.map(y => `<option value="${y}">${y}</option>`).join('')}
        </select>
        <select id="calc-${which}-sub" style="flex:1;padding:8px;border:1px solid var(--border);border-radius:4px">
          <option value="Q1">Q1</option><option value="Q2">Q2</option>
          <option value="Q3">Q3</option><option value="Q4">Q4</option>
        </select>
      </div>`;
    // Default: start = Q1 of 3 years ago, end = most recent quarter
    const yr = which === 'start' ? currentYear - 3 : currentYear - 1;
    const q  = which === 'start' ? 'Q1' : 'Q4';
    const yrEl = document.getElementById(`calc-${which}-year`);
    const subEl = document.getElementById(`calc-${which}-sub`);
    if (yrEl) yrEl.value = yr;
    if (subEl) subEl.value = q;
    return;
  }

  if (periodType === 'semi-annual') {
    label.textContent = `${which === 'start' ? 'Start' : 'End'} Half`;
    wrap.innerHTML = `
      <div style="display:flex;gap:6px">
        <select id="calc-${which}-year" style="flex:1;padding:8px;border:1px solid var(--border);border-radius:4px">
          ${years.map(y => `<option value="${y}">${y}</option>`).join('')}
        </select>
        <select id="calc-${which}-sub" style="flex:1;padding:8px;border:1px solid var(--border);border-radius:4px">
          <option value="S1">S1 (Jan–Jun)</option>
          <option value="S2">S2 (Jul–Dec)</option>
        </select>
      </div>`;
    const yr = which === 'start' ? currentYear - 3 : currentYear - 1;
    const s  = which === 'start' ? 'S1' : 'S2';
    const yrEl = document.getElementById(`calc-${which}-year`);
    const subEl = document.getElementById(`calc-${which}-sub`);
    if (yrEl) yrEl.value = yr;
    if (subEl) subEl.value = s;
  }
}

function _getPeriodValue(which) {
  const direct = document.getElementById(`calc-${which}`);
  if (direct) return direct.value;
  const yr  = document.getElementById(`calc-${which}-year`);
  const sub = document.getElementById(`calc-${which}-sub`);
  if (yr && sub) return `${yr.value}-${sub.value}`;
  return '';
}

document.getElementById('calc-index').addEventListener('change', function() {
  updatePeriodPicker(this.value);
});

// ── Calculate ──────────────────────────────────────────────────────────────
async function calculate() {
  const index_id     = document.getElementById('calc-index').value;
  const start_period = _getPeriodValue('start');
  const end_period   = _getPeriodValue('end');

  hideMessages();

  if (!index_id)     return showError('Please select an index.');
  if (!start_period) return showError('Please enter a start period.');
  if (!end_period)   return showError('Please enter an end period.');
  if (end_period <= start_period) return showError('End period must be after start period.');

  // Warn if > 10 years
  const startYear = parseInt(start_period.slice(0,4));
  const endYear   = parseInt(end_period.slice(0,4));
  if (endYear - startYear > 10) showWarn('Range is greater than 10 years — results may span multiple economic cycles.');

  const btn = document.getElementById('calc-btn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span>';

  try {
    const [res, detail] = await Promise.all([
      api.post('/calculate', { index_id, start_period, end_period }),
      api.get(`/indices/${index_id}`).catch(() => null),
    ]);
    lastResult = res;
    lastIndexDetail = detail;
    renderResult(res, detail);
  } catch (err) {
    showError(err.message);
    document.getElementById('result-card').classList.remove('visible');
    document.getElementById('download-wrap').style.display = 'none';
  } finally {
    btn.disabled = false;
    btn.textContent = 'Calculate';
  }
}

function renderResult(res, detail) {
  const pctVal = res.pct_change;
  const cls    = pctClass(pctVal);
  const sign   = pctVal >= 0 ? '+' : '';

  const srcUrl = detail && detail.source_url ? detail.source_url : null;
  document.getElementById('result-meta').innerHTML =
    `<span class="index-name-cell">${escHtml(res.index_name)}${sourceLink(srcUrl)}</span>`;
  document.getElementById('result-pct').textContent  = `${sign}${pctVal.toFixed(2)}%`;
  document.getElementById('result-pct').className    = `result-big ${cls}`;
  document.getElementById('result-abs').textContent  = `Absolute change: ${sign}${res.abs_change.toFixed(2)}`;

  document.getElementById('res-start-period').textContent = res.start_period;
  document.getElementById('res-start-val').textContent    = fmtVal(res.start_value, 2);
  document.getElementById('res-end-period').textContent   = res.end_period;
  document.getElementById('res-end-val').textContent      = fmtVal(res.end_value, 2);

  // Sparkline
  if (res.monthly_series && res.monthly_series.length) {
    const labels = res.monthly_series.map(d => d.period);
    const values = res.monthly_series.map(d => d.value);
    renderSparkline(labels, values);
  }

  document.getElementById('result-card').classList.add('visible');
  document.getElementById('download-wrap').style.display = 'inline-flex';
}

function renderSparkline(labels, values) {
  const ctx = document.getElementById('sparkline-chart').getContext('2d');
  if (sparkChart) sparkChart.destroy();
  sparkChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        data: values,
        borderColor: '#4A7FAA',
        backgroundColor: 'rgba(74,127,170,.10)',
        borderWidth: 2,
        pointRadius: 1.5,
        fill: true,
        tension: 0.3,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: {
        callbacks: {
          label: ctx => `Value: ${ctx.parsed.y.toFixed(2)}`
        }
      }},
      scales: {
        x: { ticks: { font: { size: 9 }, maxRotation: 45, autoSkip: true, maxTicksLimit: 18 }},
        y: { ticks: { font: { size: 9 }}},
      }
    }
  });
}

// ── Copy ───────────────────────────────────────────────────────────────────
document.getElementById('btn-copy').addEventListener('click', () => {
  if (!lastResult) return;
  const r    = lastResult;
  const sign = r.pct_change >= 0 ? '+' : '';
  const text = `${r.index_name}  ${r.start_period} → ${r.end_period}: ${sign}${r.pct_change.toFixed(2)}%`;
  navigator.clipboard.writeText(text).then(
    () => showToast('Copied to clipboard!', 'success'),
    () => showToast('Could not copy to clipboard', 'error')
  );
});

// ── Download ───────────────────────────────────────────────────────────────
function buildDownloadUrl(fmt) {
  const r = lastResult;
  const params = new URLSearchParams({
    index_id:     document.getElementById('calc-index').value,
    start_period: r.start_period,
    end_period:   r.end_period,
    format:       fmt,
  });
  return '/api/export/calculator?' + params.toString();
}

document.getElementById('btn-dl-xlsx').addEventListener('click', () => downloadUrl(buildDownloadUrl('xlsx')));
document.getElementById('dl-xlsx').addEventListener('click', () => {
  downloadUrl(buildDownloadUrl('xlsx'));
  document.getElementById('dl-menu').classList.remove('open');
});
document.getElementById('dl-csv').addEventListener('click', () => {
  downloadUrl(buildDownloadUrl('csv'));
  document.getElementById('dl-menu').classList.remove('open');
});
document.getElementById('btn-dl-toggle').addEventListener('click', (e) => {
  e.stopPropagation();
  document.getElementById('dl-menu').classList.toggle('open');
});
document.addEventListener('click', () => document.getElementById('dl-menu').classList.remove('open'));

function escHtml(str) {
  return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// ── Helpers ────────────────────────────────────────────────────────────────
function showError(msg) {
  const el = document.getElementById('calc-error');
  el.textContent = '⚠ ' + msg;
  el.classList.add('visible');
}
function showWarn(msg) {
  const el = document.getElementById('calc-warn');
  el.textContent = '⚠ ' + msg;
  el.classList.add('visible');
}
function hideMessages() {
  document.getElementById('calc-error').classList.remove('visible');
  document.getElementById('calc-warn').classList.remove('visible');
}

document.getElementById('calc-btn').addEventListener('click', calculate);
// Allow Enter key on month inputs (only present for monthly indices)
document.getElementById('calc-end-wrap').addEventListener('keydown', e => { if (e.key === 'Enter') calculate(); });

// ── Init ───────────────────────────────────────────────────────────────────
loadIndices();
