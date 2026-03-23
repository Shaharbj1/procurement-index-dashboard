import { api, showToast, fmtPct, fmtVal, pctClass, downloadUrl } from './api.js';

// ── State ──────────────────────────────────────────────────────────────────
let currentType    = 'ppi';
let currentPeriods = 24;
let regChart       = null;
let lastPayload    = null;

// ── Country colour palette (13 countries + EU avg) ─────────────────────────
const PALETTE = [
  '#1565C0','#2E7D32','#C62828','#E65100','#6A1B9A',
  '#00695C','#F9A825','#37474F','#AD1457','#558B2F',
  '#0277BD','#4E342E','#455A64',
  '#7B1FA2',  // EU Average — purple-ish dashed
];

const TYPE_LABELS = { ppi: 'PPI', cpi: 'CPI', lci: 'LCI', energy: 'Energy Prices' };
const PERIOD_TYPE_LABELS = { monthly: 'Monthly', quarterly: 'Quarterly', 'semi-annual': 'Semi-Annual' };

// ── Toggle buttons ─────────────────────────────────────────────────────────
document.querySelectorAll('#type-toggles .toggle-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('#type-toggles .toggle-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    currentType = btn.dataset.type;
    loadData();
  });
});

document.querySelectorAll('#range-toggles .toggle-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('#range-toggles .toggle-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    currentPeriods = parseInt(btn.dataset.periods);
    loadData();
  });
});

// ── Download buttons ────────────────────────────────────────────────────────
function buildExportUrl(fmt) {
  return `/api/regional/export?type=${currentType}&periods=${currentPeriods}&format=${fmt}`;
}

document.getElementById('reg-btn-dl-xlsx').addEventListener('click', () => downloadUrl(buildExportUrl('xlsx')));
document.getElementById('reg-dl-xlsx').addEventListener('click', () => {
  downloadUrl(buildExportUrl('xlsx'));
  document.getElementById('reg-dl-menu').classList.remove('open');
});
document.getElementById('reg-dl-csv').addEventListener('click', () => {
  downloadUrl(buildExportUrl('csv'));
  document.getElementById('reg-dl-menu').classList.remove('open');
});
document.getElementById('reg-btn-dl-toggle').addEventListener('click', e => {
  e.stopPropagation();
  document.getElementById('reg-dl-menu').classList.toggle('open');
});
document.addEventListener('click', () => document.getElementById('reg-dl-menu').classList.remove('open'));

// ── Load & render ──────────────────────────────────────────────────────────
async function loadData() {
  document.getElementById('reg-loading').style.display  = 'block';
  document.getElementById('reg-error').style.display    = 'none';
  document.getElementById('reg-chart-card').style.display = 'none';
  document.getElementById('reg-table-card').style.display  = 'none';

  try {
    const data = await api.get(`/regional/summary?type=${currentType}&periods=${currentPeriods}`);
    lastPayload = data;
    renderChart(data);
    renderTable(data);
    document.getElementById('reg-loading').style.display    = 'none';
    document.getElementById('reg-chart-card').style.display = '';
    document.getElementById('reg-table-card').style.display  = '';
  } catch (err) {
    document.getElementById('reg-loading').style.display = 'none';
    const errEl = document.getElementById('reg-error');
    errEl.style.display = 'block';
    errEl.textContent = '⚠ Failed to load regional data: ' + err.message;
  }
}

// ── Chart ──────────────────────────────────────────────────────────────────
function renderChart(data) {
  const typeLabel     = TYPE_LABELS[data.type] || data.type.toUpperCase();
  const ptLabel       = PERIOD_TYPE_LABELS[data.period_type] || data.period_type;
  document.getElementById('reg-chart-title').textContent      = `${typeLabel} by Country`;
  document.getElementById('reg-period-type-label').textContent = ptLabel;

  const periods = data.periods;

  const datasets = data.series.map((s, i) => {
    const isEuAvg = s.country === 'EU_AVG';
    const vm = {};
    s.data.forEach(d => { vm[d.period] = d.value; });
    return {
      label:           s.country_name,
      data:            periods.map(p => vm[p] ?? null),
      borderColor:     PALETTE[i % PALETTE.length],
      backgroundColor: 'transparent',
      borderWidth:     isEuAvg ? 2.5 : 1.5,
      borderDash:      isEuAvg ? [6, 3] : [],
      pointRadius:     isEuAvg ? 2 : 1,
      tension:         0.25,
      spanGaps:        true,
    };
  });

  const ctx = document.getElementById('reg-chart').getContext('2d');
  if (regChart) regChart.destroy();
  regChart = new Chart(ctx, {
    type: 'line',
    data: { labels: periods, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: {
          position: 'bottom',
          labels: { font: { size: 10 }, boxWidth: 12, padding: 8 },
        },
        tooltip: {
          callbacks: {
            label: ctx => `${ctx.dataset.label}: ${ctx.parsed.y != null ? ctx.parsed.y.toFixed(2) : '—'}`,
          },
        },
      },
      scales: {
        x: { ticks: { font: { size: 9 }, maxRotation: 45, autoSkip: true, maxTicksLimit: 20 }},
        y: { ticks: { font: { size: 9 }}},
      },
    },
  });
}

// ── Table ──────────────────────────────────────────────────────────────────
function renderTable(data) {
  const typeLabel = TYPE_LABELS[data.type] || data.type.toUpperCase();
  document.getElementById('reg-table-title').textContent = `${typeLabel} — Country Comparison`;

  // Find the EU Average series
  const euSeries = data.series.find(s => s.country === 'EU_AVG');

  // Build latest-period lookup for EU avg
  const euMap = {};
  if (euSeries) euSeries.data.forEach(d => { euMap[d.period] = d.value; });

  // For each non-EU-avg series, get latest period data
  const rows = data.series
    .filter(s => s.country !== 'EU_AVG')
    .map(s => {
      // Find the latest non-null data point
      const validData = [...s.data].reverse().find(d => d.value != null);
      const latest    = validData?.value  ?? null;
      const period    = validData?.period ?? '—';
      const mom       = validData?.mom_change ?? null;
      const yoy       = validData?.yoy_change ?? null;
      const euVal     = period !== '—' ? (euMap[period] ?? null) : null;
      const vsEu      = (latest != null && euVal != null) ? latest - euVal : null;
      return { country: s.country, name: s.country_name, latest, period, mom, yoy, vsEu };
    });

  // Sort: EU countries first (alphabetical by name), then CH, then IL
  const sortOrder = ['DE','FR','IT','ES','NL','BE','SE','PL','AT','PT','CZ','CH','IL'];
  rows.sort((a, b) => sortOrder.indexOf(a.country) - sortOrder.indexOf(b.country));

  const thStyle = 'padding:8px 12px;background:var(--navy);color:#fff;text-align:left;white-space:nowrap;cursor:pointer';
  const thead = document.getElementById('reg-thead');
  thead.innerHTML = `<tr>
    <th style="${thStyle}">Country</th>
    <th style="${thStyle};text-align:right">Latest Value</th>
    <th style="${thStyle}">Period</th>
    <th style="${thStyle};text-align:right">MoM %</th>
    <th style="${thStyle};text-align:right">YoY %</th>
    <th style="${thStyle};text-align:right">vs EU Avg</th>
  </tr>`;

  const tbody = document.getElementById('reg-tbody');
  tbody.innerHTML = rows.map((r, i) => {
    const bg = i % 2 === 0 ? '' : 'background:#F0F4F8';
    return `<tr style="border-bottom:1px solid var(--border);${bg}">
      <td style="padding:6px 12px;font-weight:600">${escHtml(r.name)}
        <span style="font-size:.75rem;color:var(--text-muted);margin-left:4px">${r.country}</span>
      </td>
      <td style="padding:6px 12px;text-align:right;font-family:monospace">${r.latest != null ? r.latest.toFixed(2) : '—'}</td>
      <td style="padding:6px 12px;font-size:.82rem;color:var(--text-muted)">${escHtml(r.period)}</td>
      <td style="padding:6px 12px;text-align:right" class="${r.mom != null ? pctClass(r.mom) : ''}">${r.mom != null ? fmtPct(r.mom) : '—'}</td>
      <td style="padding:6px 12px;text-align:right" class="${r.yoy != null ? pctClass(r.yoy) : ''}">${r.yoy != null ? fmtPct(r.yoy) : '—'}</td>
      <td style="padding:6px 12px;text-align:right" class="${r.vsEu != null ? (r.vsEu > 0 ? 'positive' : r.vsEu < 0 ? 'negative' : '') : ''}">${r.vsEu != null ? (r.vsEu >= 0 ? '+' : '') + r.vsEu.toFixed(2) : '—'}</td>
    </tr>`;
  }).join('');

  // EU average footer row
  if (euSeries) {
    const validEu = [...euSeries.data].reverse().find(d => d.value != null);
    const euLatest = validEu?.value ?? null;
    const euPeriod = validEu?.period ?? '—';
    tbody.innerHTML += `<tr style="border-top:2px solid var(--navy);background:#EBF3FB">
      <td style="padding:6px 12px;font-weight:700;color:var(--navy)">🇪🇺 EU Average</td>
      <td style="padding:6px 12px;text-align:right;font-family:monospace;font-weight:700">${euLatest != null ? euLatest.toFixed(2) : '—'}</td>
      <td style="padding:6px 12px;font-size:.82rem;color:var(--text-muted)">${escHtml(euPeriod)}</td>
      <td style="padding:6px 12px;text-align:right">—</td>
      <td style="padding:6px 12px;text-align:right">—</td>
      <td style="padding:6px 12px;text-align:right">—</td>
    </tr>`;
  }
}

function escHtml(str) {
  return String(str || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// ── Init ───────────────────────────────────────────────────────────────────
loadData();
