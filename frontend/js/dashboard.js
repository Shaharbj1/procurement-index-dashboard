import { api, showToast, fmtPct, fmtVal, pctClass, segBadge, sourceBadge, downloadUrl, sourceLink } from './api.js';

let allIndices = [];
let panelChart = null;
let selectedId = null;

// Active filter state (read at download time)
const filters = { segment: '', source: '', category: '', q: '' };

async function loadIndices() {
  try {
    allIndices = await api.get('/indices');
    renderKPIs(allIndices);
    renderTable(allIndices);
  } catch (err) {
    showToast('Failed to load indices: ' + err.message, 'error');
    document.getElementById('indices-tbody').innerHTML =
      `<tr><td colspan="7" class="text-center" style="color:var(--negative);padding:24px">Error loading data</td></tr>`;
  }
}

function renderKPIs(data) {
  document.getElementById('kpi-total').textContent = data.length;

  // Last upload date
  const dates = data.map(d => d.last_updated).filter(Boolean).sort();
  const lastUpload = dates.length ? dates[dates.length - 1].slice(0, 10) : '—';
  document.getElementById('kpi-last-upload').textContent = lastUpload;

  // Highest / Lowest MoM
  const withMom = data.filter(d => d.mom_change != null);
  if (withMom.length) {
    const sorted = [...withMom].sort((a, b) => b.mom_change - a.mom_change);
    const high = sorted[0];
    const low  = sorted[sorted.length - 1];
    document.getElementById('kpi-highest').textContent = fmtPct(high.mom_change);
    document.getElementById('kpi-highest-name').textContent = high.name;
    document.getElementById('kpi-lowest').textContent  = fmtPct(low.mom_change);
    document.getElementById('kpi-lowest-name').textContent  = low.name;
  }
}

function renderTable(data) {
  const tbody = document.getElementById('indices-tbody');
  if (!data.length) {
    tbody.innerHTML = `<tr><td colspan="7" class="text-center" style="color:var(--text-muted);padding:32px">No indices match the current filters.</td></tr>`;
    return;
  }
  tbody.innerHTML = data.map(idx => {
    const lockIcon = idx.paid_source ? ' 🔒' : '';
    const momCls   = pctClass(idx.mom_change);
    const yoyCls   = pctClass(idx.yoy_change);
    return `<tr data-id="${idx.id}" class="${selectedId === idx.id ? 'selected' : ''}">
      <td><span class="index-name-cell"><strong>${escHtml(idx.name)}</strong>${lockIcon}${sourceLink(idx.source_url)}</span></td>
      <td>${segBadge(idx.segment)}</td>
      <td>${sourceBadge(idx.source)}</td>
      <td>${idx.latest_value != null ? fmtVal(idx.latest_value, 2) : '—'}</td>
      <td>${idx.latest_period || '—'}</td>
      <td class="${momCls}">${fmtPct(idx.mom_change)}</td>
      <td class="${yoyCls}">${fmtPct(idx.yoy_change)}</td>
    </tr>`;
  }).join('');

  tbody.querySelectorAll('tr[data-id]').forEach(tr => {
    tr.addEventListener('click', () => openPanel(tr.dataset.id));
  });
}

async function openPanel(indexId) {
  selectedId = indexId;
  // Highlight selected row
  document.querySelectorAll('#indices-tbody tr').forEach(tr => {
    tr.classList.toggle('selected', tr.dataset.id === indexId);
  });

  document.getElementById('panel-placeholder').style.display = 'none';
  document.getElementById('panel-content').style.display = 'block';
  document.getElementById('panel-title').textContent = 'Loading…';

  try {
    const detail = await api.get(`/indices/${indexId}`);
    document.getElementById('panel-title').innerHTML =
      `<span class="index-name-cell">${escHtml(detail.name)}${sourceLink(detail.source_url)}</span>`;
    document.getElementById('panel-period').textContent =
      `${detail.base_year} | ${detail.unit} | Last: ${detail.series.length ? detail.series[detail.series.length-1].period : '—'}`;

    // Set calculator link
    document.getElementById('open-in-calc').href = `/calculator.html?index_id=${indexId}`;

    // Chart
    const series36 = detail.series.slice(-36);
    const labels = series36.map(d => d.period);
    const values = series36.map(d => d.value);
    renderPanelChart(labels, values, detail.name);

    // Mini table — last 12 months
    const last12 = detail.series.slice(-12).reverse();
    const miniTbody = document.getElementById('mini-tbody');
    miniTbody.innerHTML = last12.map(r => `
      <tr>
        <td>${r.period}</td>
        <td>${fmtVal(r.value, 2)}</td>
        <td class="${pctClass(r.mom_change)}">${fmtPct(r.mom_change)}</td>
        <td class="${pctClass(r.yoy_change)}">${fmtPct(r.yoy_change)}</td>
      </tr>`).join('');
  } catch (err) {
    showToast('Failed to load index detail: ' + err.message, 'error');
  }
}

function renderPanelChart(labels, values, title) {
  const ctx = document.getElementById('panel-chart').getContext('2d');
  if (panelChart) panelChart.destroy();
  panelChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: title,
        data: values,
        borderColor: '#4A7FAA',
        backgroundColor: 'rgba(74,127,170,.08)',
        borderWidth: 2,
        pointRadius: 2,
        fill: true,
        tension: 0.3,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { font: { size: 10 }, maxRotation: 45, autoSkip: true, maxTicksLimit: 12 } },
        y: { ticks: { font: { size: 10 } } },
      }
    }
  });
}

// ── Filters ────────────────────────────────────────────────────────────────
function applyFilters() {
  filters.segment  = document.getElementById('f-segment').value;
  filters.source   = document.getElementById('f-source').value;
  filters.category = document.getElementById('f-category').value;
  filters.q        = document.getElementById('f-search').value.trim().toLowerCase();

  // Show/hide regional sub-filters
  const regSub = document.getElementById('regional-subfilters');
  if (regSub) regSub.style.display = filters.segment === 'regional' ? '' : 'none';

  const regType    = document.getElementById('f-reg-type')?.value    || '';
  const regCountry = document.getElementById('f-reg-country')?.value || '';

  const filtered = allIndices.filter(idx => {
    if (filters.segment  && idx.segment  !== filters.segment)  return false;
    if (filters.source   && idx.source   !== filters.source)   return false;
    if (filters.category && idx.category !== filters.category) return false;
    if (filters.q && !idx.name.toLowerCase().includes(filters.q)) return false;
    // Regional sub-filters (only active when segment === 'regional')
    if (filters.segment === 'regional') {
      if (regType) {
        // index id format: reg_{type}_{iso_lower}
        const typePart = idx.id.split('_')[1]; // ppi / cpi / lci / energy
        if (typePart !== regType) return false;
      }
      if (regCountry && idx.country_iso !== regCountry) return false;
    }
    return true;
  });
  renderTable(filtered);
}

['f-segment','f-source','f-category'].forEach(id => {
  document.getElementById(id).addEventListener('change', applyFilters);
});
document.getElementById('f-search').addEventListener('input', applyFilters);
// Regional sub-filter listeners
document.getElementById('f-reg-type')?.addEventListener('change', applyFilters);
document.getElementById('f-reg-country')?.addEventListener('change', applyFilters);

// ── Download ───────────────────────────────────────────────────────────────
window.downloadDashboard = function(fmt) {
  const params = new URLSearchParams({ format: fmt });
  if (filters.segment)  params.set('segment',  filters.segment);
  if (filters.source)   params.set('source',   filters.source);
  if (filters.category) params.set('category', filters.category);
  if (filters.q)        params.set('q',        filters.q);
  downloadUrl('/api/export/dashboard?' + params.toString());
  document.getElementById('download-menu').classList.remove('open');
};

document.getElementById('btn-download-xlsx').addEventListener('click', () => downloadDashboard('xlsx'));
document.getElementById('btn-download-toggle').addEventListener('click', (e) => {
  e.stopPropagation();
  document.getElementById('download-menu').classList.toggle('open');
});
document.addEventListener('click', () => document.getElementById('download-menu').classList.remove('open'));

function escHtml(str) {
  return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// ── Init ───────────────────────────────────────────────────────────────────
loadIndices();
