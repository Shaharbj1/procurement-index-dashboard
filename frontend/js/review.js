import { api, showToast, fmtPct, fmtVal, pctClass } from './api.js';

const COLORS = [
  '#4A7FAA','#2E7D32','#E65100','#6A1B9A','#00695C',
  '#1565C0','#C62828','#F57F17','#37474F','#558B2F',
];

let reviewData = null;
const panelCharts = {}; // segment key → Chart instance

// ── Load ───────────────────────────────────────────────────────────────────
async function load() {
  try {
    reviewData = await api.get('/review/summary');
    render(reviewData);
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
                  <td style="padding:5px 10px">${escHtml(idx.name)}</td>
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

// ── Init ───────────────────────────────────────────────────────────────────
load();
