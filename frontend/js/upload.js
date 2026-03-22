import { api, showToast } from './api.js';

let pendingSessionId = null;
let uploadHistory = [];

const dropZone    = document.getElementById('drop-zone');
const fileInput   = document.getElementById('file-input');
const previewSec  = document.getElementById('preview-section');
const successMsg  = document.getElementById('success-msg');
const errorMsg    = document.getElementById('upload-error');

// ── Drag & Drop ────────────────────────────────────────────────────────────
dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('drag-over'); });
dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag-over'));
dropZone.addEventListener('drop', e => {
  e.preventDefault();
  dropZone.classList.remove('drag-over');
  const file = e.dataTransfer.files[0];
  if (file) handleFile(file);
});
document.getElementById('browse-trigger').addEventListener('click', () => fileInput.click());
dropZone.addEventListener('click', () => fileInput.click());
fileInput.addEventListener('change', e => {
  if (e.target.files[0]) handleFile(e.target.files[0]);
});

// ── Handle file ────────────────────────────────────────────────────────────
async function handleFile(file) {
  hideMessages();
  previewSec.classList.remove('visible');
  pendingSessionId = null;

  const allowed = ['.csv', '.xlsx', '.xls'];
  const ext = '.' + file.name.split('.').pop().toLowerCase();
  if (!allowed.includes(ext)) {
    showError('Unsupported file type. Please upload a .csv or .xlsx file.');
    return;
  }

  const formData = new FormData();
  formData.append('file', file);

  try {
    const res = await api.upload('/upload', formData);
    pendingSessionId = res.session_id;

    document.getElementById('meta-file').textContent   = file.name;
    document.getElementById('meta-format').textContent = res.detected_format;
    document.getElementById('meta-rows').textContent   = res.total_rows;

    const tbody = document.getElementById('preview-tbody');
    tbody.innerHTML = res.rows.map(r => `
      <tr>
        <td>${escHtml(r.index_id)}</td>
        <td>${escHtml(r.period)}</td>
        <td>${r.value}</td>
      </tr>`).join('');

    const note = res.total_rows > 10
      ? `Showing first 10 of ${res.total_rows} rows.`
      : `Showing all ${res.total_rows} rows.`;
    document.getElementById('preview-note').textContent = note;

    previewSec.classList.add('visible');
  } catch (err) {
    showError('Parse error: ' + err.message);
  }
}

// ── Confirm ────────────────────────────────────────────────────────────────
document.getElementById('btn-confirm').addEventListener('click', async () => {
  if (!pendingSessionId) return;
  const btn = document.getElementById('btn-confirm');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> Importing…';

  // Remember meta for history
  const fileName  = document.getElementById('meta-file').textContent;
  const fmt       = document.getElementById('meta-format').textContent;

  try {
    const res = await api.post('/upload/confirm', { session_id: pendingSessionId });
    previewSec.classList.remove('visible');
    successMsg.textContent = `✔ ${res.message}`;
    successMsg.classList.add('visible');
    pendingSessionId = null;
    fileInput.value = '';

    // Add to history
    uploadHistory.unshift({
      filename: fileName,
      date: new Date().toISOString().slice(0,16).replace('T',' '),
      format: fmt,
      added: res.rows_added,
      updated: res.rows_updated,
      status: 'Success',
    });
    renderHistory();
  } catch (err) {
    showError('Import failed: ' + err.message);
  } finally {
    btn.disabled = false;
    btn.innerHTML = '✔ Confirm Import';
  }
});

document.getElementById('btn-cancel').addEventListener('click', () => {
  previewSec.classList.remove('visible');
  pendingSessionId = null;
  fileInput.value = '';
});

// ── Template download ──────────────────────────────────────────────────────
document.getElementById('btn-template').addEventListener('click', () => {
  const csv = 'index_id,period,value\nsec_pkg_nbsk,2024-01,142.3\nsec_pkg_nbsk,2024-02,143.1\nemn_cpi_uk,2024-01,128.4\nemn_cpi_uk,2024-02,129.0\nprim_ppi_eu,2024-01,115.6\n';
  const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8;' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'sample_standard.csv';
  a.click();
});

// ── History ────────────────────────────────────────────────────────────────
function renderHistory() {
  const tbody = document.getElementById('history-tbody');
  if (!uploadHistory.length) {
    tbody.innerHTML = `<tr><td colspan="6" style="padding:20px;text-align:center;color:var(--text-muted)">No uploads yet in this session.</td></tr>`;
    return;
  }
  tbody.innerHTML = uploadHistory.slice(0,10).map(h => `
    <tr>
      <td style="padding:8px 12px">${escHtml(h.filename)}</td>
      <td style="padding:8px 12px">${h.date}</td>
      <td style="padding:8px 12px">${escHtml(h.format)}</td>
      <td style="padding:8px 12px;text-align:right">${h.added}</td>
      <td style="padding:8px 12px;text-align:right">${h.updated}</td>
      <td style="padding:8px 12px"><span class="badge" style="background:var(--positive)">${h.status}</span></td>
    </tr>`).join('');
}

// ── Helpers ────────────────────────────────────────────────────────────────
function showError(msg) {
  errorMsg.textContent = '⚠ ' + msg;
  errorMsg.classList.add('visible');
}
function hideMessages() {
  successMsg.classList.remove('visible');
  errorMsg.classList.remove('visible');
}
function escHtml(str) {
  return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
