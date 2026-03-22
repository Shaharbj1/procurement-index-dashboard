import { api, showToast, fmtPct, fmtVal, pctClass, downloadUrl, SEGMENT_BADGES } from './api.js';

let sparkChart = null;
let lastResult = null;

// ── Populate index dropdown ────────────────────────────────────────────────
async function loadIndices() {
  try {
    const indices = await api.get('/indices');
    const select = document.getElementById('calc-index');

    // Group by segment
    const groups = {};
    indices.forEach(idx => {
      if (!groups[idx.segment]) groups[idx.segment] = [];
      groups[idx.segment].push(idx);
    });

    Object.entries(groups).forEach(([seg, items]) => {
      const segMeta = SEGMENT_BADGES[seg] || { label: seg };
      const og = document.createElement('optgroup');
      og.label = segMeta.label;
      items.forEach(idx => {
        const opt = document.createElement('option');
        opt.value = idx.id;
        opt.textContent = idx.name + (idx.paid_source ? ' 🔒' : '');
        og.appendChild(opt);
      });
      select.appendChild(og);
    });

    // Pre-select from URL param
    const params = new URLSearchParams(location.search);
    const preId = params.get('index_id');
    if (preId) select.value = preId;
  } catch (err) {
    showToast('Failed to load indices: ' + err.message, 'error');
  }
}

// ── Calculate ──────────────────────────────────────────────────────────────
async function calculate() {
  const index_id     = document.getElementById('calc-index').value;
  const start_period = document.getElementById('calc-start').value;
  const end_period   = document.getElementById('calc-end').value;

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
    const res = await api.post('/calculate', { index_id, start_period, end_period });
    lastResult = res;
    renderResult(res);
  } catch (err) {
    showError(err.message);
    document.getElementById('result-card').classList.remove('visible');
    document.getElementById('download-wrap').style.display = 'none';
  } finally {
    btn.disabled = false;
    btn.textContent = 'Calculate';
  }
}

function renderResult(res) {
  const pctVal = res.pct_change;
  const cls    = pctClass(pctVal);
  const sign   = pctVal >= 0 ? '+' : '';

  document.getElementById('result-meta').textContent = res.index_name;
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
document.getElementById('calc-end').addEventListener('keydown', e => { if (e.key === 'Enter') calculate(); });

// ── Init ───────────────────────────────────────────────────────────────────
loadIndices();
