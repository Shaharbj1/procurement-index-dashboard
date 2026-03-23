import { api, showToast } from './api.js';

async function loadLog() {
  try {
    const data = await api.get('/admin/fetch-log');
    renderStatus(data);
    renderEnvStatus(data.env_status);
    renderLog(data.fetch_log);

    const paidEl = document.getElementById('paid-list');
    if (paidEl && data.paid_indices) {
      paidEl.textContent = data.paid_indices.join(', ');
    }
  } catch (err) {
    showToast('Failed to load admin data: ' + err.message, 'error');
  }
}

function renderStatus(data) {
  const statusEl = document.getElementById('overall-status');
  const nextEl   = document.getElementById('next-run');
  const lastEl   = document.getElementById('last-run');

  const statusMap = {
    'OK':        ['OK', 'status-ok'],
    'ERROR':     ['ERROR', 'status-error'],
    'NEVER_RUN': ['NEVER RUN', 'status-never'],
  };
  const [label, cls] = statusMap[data.overall_status] || ['UNKNOWN', 'status-never'];
  statusEl.textContent = label;
  statusEl.className = `status-badge ${cls}`;

  nextEl.textContent = data.next_scheduled_run
    ? new Date(data.next_scheduled_run).toLocaleString()
    : '—';
  lastEl.textContent = data.last_run
    ? new Date(data.last_run).toLocaleString()
    : '—';
}

function renderEnvStatus(envStatus) {
  const el = document.getElementById('env-status');
  if (!el || !envStatus) return;
  el.innerHTML = Object.entries(envStatus).map(([key, isSet]) => `
    <div class="env-key-status">
      <span class="${isSet ? 'env-set' : 'env-unset'}">${isSet ? '✔' : '✗'}</span>
      <span style="font-family:monospace;font-size:.85rem">${key}</span>
      <span style="color:var(--text-muted);font-size:.8rem">${isSet ? 'Set' : 'Not set'}</span>
    </div>
  `).join('');
}

function renderLog(entries) {
  const tbody = document.getElementById('log-tbody');
  if (!entries || !entries.length) {
    tbody.innerHTML = `<tr><td colspan="7" style="padding:24px;text-align:center;color:var(--text-muted)">No fetch log entries yet. Click "Trigger Now" to run the first fetch.</td></tr>`;
    return;
  }
  tbody.innerHTML = entries.map(e => {
    const statusCls = e.status === 'ok' ? 'status-ok'
                    : e.status === 'error' ? 'status-error'
                    : 'status-skipped';
    const errorText = e.error_msg
      ? e.error_msg.slice(0, 80) + (e.error_msg.length > 80 ? '…' : '')
      : '—';
    return `
      <tr style="border-bottom:1px solid var(--border)">
        <td style="padding:6px 12px;white-space:nowrap">${e.run_at || '—'}</td>
        <td style="padding:6px 12px;font-family:monospace;font-size:.82rem">${escHtml(e.index_id)}</td>
        <td style="padding:6px 12px;text-align:center">
          <span class="status-badge ${statusCls}">${e.status.toUpperCase()}</span>
        </td>
        <td style="padding:6px 12px;text-align:right">${e.rows_added ?? 0}</td>
        <td style="padding:6px 12px;text-align:right">${e.rows_updated ?? 0}</td>
        <td style="padding:6px 12px;text-align:right">${e.duration_ms ?? '—'}</td>
        <td style="padding:6px 12px;color:var(--negative);font-size:.8rem" title="${escHtml(e.error_msg || '')}">${escHtml(errorText)}</td>
      </tr>`;
  }).join('');
}

window.triggerRefresh = async function() {
  const btn = document.getElementById('btn-trigger');
  const msg = document.getElementById('trigger-msg');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> Starting…';

  try {
    const res = await api.post('/admin/refresh', {});
    msg.textContent = '✔ Fetch job started in background. Reload this page in ~60 seconds to see results.';
    msg.style.display = 'block';
    showToast('Fetch started successfully', 'success');
  } catch (err) {
    showToast('Failed to trigger fetch: ' + err.message, 'error');
  } finally {
    btn.disabled = false;
    btn.textContent = '▶ Trigger Now';
  }
};

function escHtml(str) {
  return String(str || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

loadLog();
