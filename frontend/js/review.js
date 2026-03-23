import { api, showToast, fmtPct, fmtVal, pctClass, sourceLink } from './api.js';

const COLORS = [
  '#4A7FAA','#2E7D32','#E65100','#6A1B9A','#00695C',
  '#1565C0','#C62828','#F57F17','#37474F','#558B2F',
];

const REGIONAL_COLOR = '#7B1FA2';

let reviewData   = null;
let regionalData = {};   // type → payload from /api/regional/summary
const panelCharts = {}; // chart id → Chart instance

// ── Load ───────────────────────────────────────────────────────────────────
async function load() {
  try {
    const [main, ppi, cpi, lci, energy] = await Promise.all([
      api.get('/review/summary'),
      api.get('/regional/summary?type=ppi&periods=24').catch(() => null),
      api.get('/regional/summary?type=cpi&periods=24').catch(() => null),
      api.get('/regional/summary?type=lci&periods=24').catch(() => null),
      api.get('/regional/summary?type=energy&periods=24').catch(() => null),
    ]);
    reviewData   = main;
    regionalData = { ppi, cpi, lci, energy };
    render(reviewData);
    renderRegionalPanel();
  } catch (err) {
    showToast('Failed to load review data: ' + err.message, 'error');
  }
}

function render(data) {
  document.getElementById('review-asof').textContent = `As of ${data.as_of || '—'}`;
  document.getElementById('footer-timestamp').textContent =
    'Report generated: ' + new Date().toLocaleDateString('en-GB', { year:'numeric', month:'long', day:'numeric' });

  renderSegCards(data.segments);
  renderPanels(data.segments);
}

// ── Zone 2 — Summary Cards ─────────────────────────────────────────────────
function renderSegCards(segments) {
  const grid = document.getElementById('seg-cards-grid');
  grid.innerHTML = segments.map(seg => {
    const trendArrow = seg.trend === 'up' ? '↑' : seg.trend === 'down' ? '↓' : '→';
    const trendCls   = seg.trend === 'up' ? 'pos' : seg.trend === 'down' ? 'neg' : 'neu';
    const sparkId    = `spark-${slugify(seg.segment)}`;
    return `
    <div class="seg-card" style="border-left-color:${seg.badge_color}">
      <div class="seg-card-info">
        <div class="seg-card-title" style="color:${seg.badge_color}">${escHtml(seg.segment)}</div>
        <div style="font-size:.8rem;color:var(--text-muted)">${seg.count} indices tracked</div>
        <div class="seg-card-kpis">
          <div class="seg-kpi">
            <span class="label">Avg MoM</span>
            <span class="val ${pctClass(seg.avg_mom)}">${fmtPct(seg.avg_mom)}</span>
          </div>
          <div class="seg-kpi">
            <span class="label">Avg YoY</span>
            <span class="val ${pctClass(seg.avg_yoy)}">${fmtPct(seg.avg_yoy)}</span>
          </div>
        </div>
      </div>
      <div class="seg-trend ${trendCls}">${trendArrow}</div>
      <div class="seg-sparkline-wrap">
        <canvas id="${sparkId}"></canvas>
      </div>
    </div>`;
  }).join('');

  // Render sparklines
  segments.forEach(seg => {
    const sparkId = `spark-${slugify(seg.segment)}`;
    const canvas  = document.getElementById(sparkId);
    if (!canvas) return;
    // Aggregate segment series (average across indices per period)
    const periodMap = {};
    seg.indices.forEach(idx => {
      idx.series.forEach(pt => {
        if (!periodMap[pt.period]) periodMap[pt.period] = [];
        if (pt.value != null) periodMap[pt.period].push(pt.value);
      });
    });
    const periods = Object.keys(periodMap).sort().slice(-12);
    const values  = periods.map(p => {
      const arr = periodMap[p];
      return arr.length ? arr.reduce((a,b) => a+b, 0)/arr.length : null;
    });

    new Chart(canvas.getContext('2d'), {
      type: 'line',
      data: {
        labels: periods,
        datasets: [{ data: values, borderColor: seg.badge_color, borderWidth: 2,
                     pointRadius: 0, fill: false, tension: 0.4 }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: { enabled: false } },
        scales: { x: { display: false }, y: { display: false } },
        animation: false,
      }
    });
  });
}

// ── Zone 3 — Trend Panels ─────────────────────────────────────────────────
function renderPanels(segments) {
  const container = document.getElementById('seg-panels');
  container.innerHTML = '';

  segments.forEach((seg, si) => {
    const panelId   = `panel-${si}`;
    const chartId   = `chart-${si}`;
    const btnId     = `toggle-${si}`;
    const panelTint = hexToRgba(seg.badge_color, 0.06);

    const panel = document.createElement('div');
    panel.className = 'review-panel';
    panel.style.background = panelTint;
    panel.innerHTML = `
      <div class="review-panel-header">
        <div class="review-panel-title">
          <span class="badge" style="background:${seg.badge_color}">${escHtml(seg.segment)}</span>
          <span style="font-size:.85rem;color:var(--text-muted)">${seg.count} indices</span>
        </div>
        <button class="btn btn-secondary btn-sm" id="${btnId}" data-mode="values">Show % change</button>
      </div>
      <div class="review-panel-body">
        <div class="review-panel-chart" style="background:${panelTint}">
          <canvas id="${chartId}"></canvas>
        </div>
        <div class="review-panel-table">
          <table style="width:100%;border-collapse:collapse;font-size:.82rem">
            <thead>
              <tr>
                <th style="padding:6px 10px;background:var(--bg-alt);text-align:left">Index</th>
                <th style="padding:6px 10px;background:var(--bg-alt);text-align:right">Latest</th>
                <th style="padding:6px 10px;background:var(--bg-alt);text-align:right">Period</th>
                <th style="padding:6px 10px;background:var(--bg-alt);text-align:right">MoM%</th>
                <th style="padding:6px 10px;background:var(--bg-alt);text-align:right">YoY%</th>
              </tr>
            </thead>
            <tbody>
              ${seg.indices.map(idx => `
                <tr style="border-bottom:1px solid var(--border)">
                  <td style="padding:5px 10px"><span class="index-name-cell">${escHtml(idx.name)}${sourceLink(idx.source_url)}</span></td>
                  <td style="padding:5px 10px;text-align:right">${idx.latest_value != null ? fmtVal(idx.latest_value,2) : '—'}</td>
                  <td style="padding:5px 10px;text-align:right">${idx.latest_period || '—'}</td>
                  <td style="padding:5px 10px;text-align:right" class="${pctClass(idx.mom)}">${fmtPct(idx.mom)}</td>
                  <td style="padding:5px 10px;text-align:right" class="${pctClass(idx.yoy)}">${fmtPct(idx.yoy)}</td>
                </tr>`).join('')}
            </tbody>
          </table>
        </div>
      </div>`;
    container.appendChild(panel);

    // Render line chart
    renderPanelChart(chartId, seg, false);

    // Toggle values ↔ % change
    document.getElementById(btnId).addEventListener('click', function() {
      const showPct = this.dataset.mode === 'values';
      this.dataset.mode = showPct ? 'pct' : 'values';
      this.textContent = showPct ? 'Show values' : 'Show % change';
      if (panelCharts[chartId]) panelCharts[chartId].destroy();
      renderPanelChart(chartId, seg, showPct);
    });
  });
}

function renderPanelChart(chartId, seg, showPct) {
  const canvas = document.getElementById(chartId);
  if (!canvas) return;

  const datasets = seg.indices
    .filter(idx => idx.series && idx.series.length)
    .map((idx, i) => {
      const series = idx.series.slice(-24);
      return {
        label: idx.name,
        data: series.map(pt => showPct ? pt.mom_change : pt.value),
        // Note: series from /review/summary doesn't have mom_change; use value only
        borderColor: COLORS[i % COLORS.length],
        backgroundColor: 'transparent',
        borderWidth: 1.5,
        pointRadius: 1.5,
        tension: 0.3,
      };
    });

  const labels = (() => {
    // Collect all periods
    const periodSet = new Set();
    seg.indices.forEach(idx => idx.series.slice(-24).forEach(pt => periodSet.add(pt.period)));
    return [...periodSet].sort().slice(-24);
  })();

  const chart = new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => {
              const val = ctx.parsed.y;
              return ` ${ctx.dataset.label}: ${val != null ? (showPct ? fmtPct(val) : fmtVal(val,2)) : '—'}`;
            }
          }
        }
      },
      scales: {
        x: { ticks: { font: { size: 9 }, maxRotation: 45, autoSkip: true, maxTicksLimit: 12 }},
        y: { ticks: { font: { size: 9 }}},
      }
    }
  });
  panelCharts[chartId] = chart;
}

// ── Copy to Clipboard ──────────────────────────────────────────────────────
document.getElementById('btn-copy-all').addEventListener('click', () => {
  if (!reviewData) return showToast('No data loaded yet.', 'error');

  let text = '';
  reviewData.segments.forEach(seg => {
    const trendLabel = seg.trend === 'up' ? '↑ Increasing' : seg.trend === 'down' ? '↓ Decreasing' : '→ Stable';
    text += `\n${seg.segment.toUpperCase()} — Review as of ${reviewData.as_of}\n`;
    text += `  Indices tracked : ${seg.count}\n`;
    text += `  Avg MoM change  : ${fmtPct(seg.avg_mom)}\n`;
    text += `  Avg YoY change  : ${fmtPct(seg.avg_yoy)}\n`;
    text += `  Trend           : ${trendLabel}\n\n`;
    text += `  ${'Index'.padEnd(30)}| Latest  | MoM%   | YoY%\n`;
    text += `  ${'-'.repeat(60)}\n`;
    seg.indices.forEach(idx => {
      const name = idx.name.slice(0,28).padEnd(30);
      const val  = idx.latest_value != null ? String(fmtVal(idx.latest_value,2)).padEnd(7) : '—'.padEnd(7);
      const mom  = fmtPct(idx.mom).padEnd(6);
      const yoy  = fmtPct(idx.yoy);
      text += `  ${name}| ${val} | ${mom} | ${yoy}\n`;
    });
    text += '\n';
  });

  navigator.clipboard.writeText(text.trim()).then(
    () => showToast('Copied to clipboard!', 'success'),
    () => showToast('Could not copy to clipboard', 'error')
  );
});

// ── Print ──────────────────────────────────────────────────────────────────
document.getElementById('btn-print').addEventListener('click', () => window.print());

// ── Helpers ────────────────────────────────────────────────────────────────
function hexToRgba(hex, alpha) {
  const r = parseInt(hex.slice(1,3),16);
  const g = parseInt(hex.slice(3,5),16);
  const b = parseInt(hex.slice(5,7),16);
  return `rgba(${r},${g},${b},${alpha})`;
}
function slugify(str) {
  return str.toLowerCase().replace(/[^a-z0-9]+/g,'_').replace(/^_|_$/g,'');
}
function escHtml(str) {
  return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// ── Regional Panel ─────────────────────────────────────────────────────────
function renderRegionalPanel() {
  const container = document.getElementById('seg-panels');
  const REGIONAL_BADGE = REGIONAL_COLOR;
  const panelTint = 'rgba(123,31,162,.05)';

  // ── Summary card ──────────────────────────────────────────────────────────
  const ppiData   = regionalData.ppi;
  const euAvgSeries = ppiData?.series?.find(s => s.country === 'EU_AVG');
  const euLatestPt  = euAvgSeries ? [...euAvgSeries.data].reverse().find(d => d.value != null) : null;

  // Build REGIONAL card in seg-cards-grid
  const grid = document.getElementById('seg-cards-grid');
  const regCardEl = document.createElement('div');
  regCardEl.className = 'seg-card';
  regCardEl.style.borderLeftColor = REGIONAL_BADGE;
  regCardEl.innerHTML = `
    <div class="seg-card-info">
      <div class="seg-card-title" style="color:${REGIONAL_BADGE}">Regional</div>
      <div style="font-size:.8rem;color:var(--text-muted)">52 indices tracked</div>
      <div class="seg-card-kpis">
        <div class="seg-kpi">
          <span class="label">EU PPI Latest</span>
          <span class="val">${euLatestPt ? euLatestPt.value.toFixed(2) : '—'}</span>
        </div>
        <div class="seg-kpi">
          <span class="label">Period</span>
          <span class="val" style="font-size:.8rem">${euLatestPt ? euLatestPt.period : '—'}</span>
        </div>
      </div>
    </div>
    <div class="seg-trend neu">→</div>
    <div class="seg-sparkline-wrap">
      <canvas id="spark-regional"></canvas>
    </div>`;
  grid.appendChild(regCardEl);

  // Sparkline — EU PPI avg
  if (euAvgSeries) {
    const sparkPts = euAvgSeries.data.filter(d => d.value != null).slice(-12);
    new Chart(document.getElementById('spark-regional').getContext('2d'), {
      type: 'line',
      data: {
        labels: sparkPts.map(d => d.period),
        datasets: [{ data: sparkPts.map(d => d.value), borderColor: REGIONAL_BADGE,
                     borderWidth: 2, pointRadius: 0, fill: false, tension: 0.4 }]
      },
      options: {
        responsive: true, maintainAspectRatio: false, animation: false,
        plugins: { legend: { display: false }, tooltip: { enabled: false } },
        scales: { x: { display: false }, y: { display: false } },
      },
    });
  }

  // ── Trend panel ───────────────────────────────────────────────────────────
  const panel = document.createElement('div');
  panel.className = 'review-panel';
  panel.style.background = panelTint;

  const TYPES = ['ppi','cpi','lci','energy'];
  const TYPE_LABELS = { ppi:'PPI', cpi:'CPI', lci:'LCI', energy:'Energy' };

  // Build 4 mini-chart canvases
  const miniChartsHtml = TYPES.map(t => `
    <div style="flex:1;min-width:200px">
      <div style="font-weight:600;font-size:.82rem;color:${REGIONAL_BADGE};margin-bottom:6px;text-align:center">${TYPE_LABELS[t]}</div>
      <div style="position:relative;height:140px"><canvas id="reg-mini-${t}"></canvas></div>
    </div>`).join('');

  // Build comparison table rows: EU Avg + CH + IL for each type
  const tableRows = TYPES.map(t => {
    const payload = regionalData[t];
    if (!payload) return '';
    const eu = payload.series.find(s => s.country === 'EU_AVG');
    const ch = payload.series.find(s => s.country === 'CH');
    const il = payload.series.find(s => s.country === 'IL');
    const latestVal = (s) => {
      if (!s) return null;
      const pt = [...s.data].reverse().find(d => d.value != null);
      return pt ? { value: pt.value, period: pt.period, mom: pt.mom_change, yoy: pt.yoy_change } : null;
    };
    const euPt = latestVal(eu);
    const chPt = latestVal(ch);
    const ilPt = latestVal(il);
    const row = (label, pt, extraStyle='') => `
      <tr style="border-bottom:1px solid var(--border);${extraStyle}">
        <td style="padding:5px 10px;font-weight:600">${label}</td>
        <td style="padding:5px 10px;font-size:.8rem;color:var(--text-muted)">${TYPE_LABELS[t]}</td>
        <td style="padding:5px 10px;text-align:right">${pt ? pt.value.toFixed(2) : '—'}</td>
        <td style="padding:5px 10px;text-align:right;font-size:.8rem;color:var(--text-muted)">${pt ? pt.period : '—'}</td>
        <td style="padding:5px 10px;text-align:right" class="${pt?.mom != null ? pctClass(pt.mom) : ''}">${pt?.mom != null ? fmtPct(pt.mom) : '—'}</td>
        <td style="padding:5px 10px;text-align:right" class="${pt?.yoy != null ? pctClass(pt.yoy) : ''}">${pt?.yoy != null ? fmtPct(pt.yoy) : '—'}</td>
      </tr>`;
    return row('🇪🇺 EU Avg', euPt, 'background:#F3E5F5') +
           row('🇨🇭 Switzerland', chPt) +
           row('🇮🇱 Israel', ilPt);
  }).join('');

  panel.innerHTML = `
    <div class="review-panel-header">
      <div class="review-panel-title">
        <span class="badge" style="background:${REGIONAL_BADGE}">Regional</span>
        <span style="font-size:.85rem;color:var(--text-muted)">52 indices — 13 countries</span>
      </div>
      <a href="/regional.html" class="btn btn-secondary btn-sm" style="display:inline-flex">Open Explorer →</a>
    </div>
    <div class="review-panel-body" style="flex-direction:column;gap:20px">
      <div style="display:flex;gap:16px;flex-wrap:wrap">${miniChartsHtml}</div>
      <div class="review-panel-table">
        <table style="width:100%;border-collapse:collapse;font-size:.82rem">
          <thead>
            <tr>
              <th style="padding:6px 10px;background:var(--bg-alt);text-align:left">Country</th>
              <th style="padding:6px 10px;background:var(--bg-alt);text-align:left">Type</th>
              <th style="padding:6px 10px;background:var(--bg-alt);text-align:right">Latest</th>
              <th style="padding:6px 10px;background:var(--bg-alt);text-align:right">Period</th>
              <th style="padding:6px 10px;background:var(--bg-alt);text-align:right">MoM%</th>
              <th style="padding:6px 10px;background:var(--bg-alt);text-align:right">YoY%</th>
            </tr>
          </thead>
          <tbody>${tableRows}</tbody>
        </table>
      </div>
    </div>`;

  container.appendChild(panel);

  // Render mini charts
  TYPES.forEach(t => {
    const payload = regionalData[t];
    if (!payload) return;
    const eu = payload.series.find(s => s.country === 'EU_AVG');
    const il = payload.series.find(s => s.country === 'IL');
    const ch = payload.series.find(s => s.country === 'CH');

    const toDataset = (s, color, dashed=false) => {
      if (!s) return null;
      const vm = {}; s.data.forEach(d => { vm[d.period] = d.value; });
      const pts = payload.periods.slice(-16);
      return {
        label: s.country_name || s.country,
        data: pts.map(p => vm[p] ?? null),
        borderColor: color,
        backgroundColor: 'transparent',
        borderWidth: dashed ? 2 : 1.5,
        borderDash: dashed ? [5,3] : [],
        pointRadius: 1,
        tension: 0.25,
        spanGaps: true,
      };
    };

    const labels   = payload.periods.slice(-16);
    const datasets = [
      toDataset(eu, REGIONAL_BADGE, true),
      toDataset(ch, '#0277BD'),
      toDataset(il, '#2E7D32'),
    ].filter(Boolean);

    const canvas = document.getElementById(`reg-mini-${t}`);
    if (!canvas) return;
    new Chart(canvas.getContext('2d'), {
      type: 'line',
      data: { labels, datasets },
      options: {
        responsive: true, maintainAspectRatio: false, animation: false,
        plugins: {
          legend: { display: true, position: 'bottom',
                    labels: { font: { size: 8 }, boxWidth: 8, padding: 4 } },
          tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${ctx.parsed.y != null ? ctx.parsed.y.toFixed(2) : '—'}` } },
        },
        scales: {
          x: { ticks: { font: { size: 8 }, maxRotation: 45, autoSkip: true, maxTicksLimit: 8 }},
          y: { ticks: { font: { size: 8 }}},
        },
      },
    });
  });
}

// ── Init ───────────────────────────────────────────────────────────────────
load();
