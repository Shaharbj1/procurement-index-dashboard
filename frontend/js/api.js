/**
 * api.js — Shared fetch wrapper + error toast display
 */

// ── Toast notifications ───────────────────────────────────────────────────
let _toastContainer = null;

function _getToastContainer() {
  if (!_toastContainer) {
    _toastContainer = document.createElement('div');
    _toastContainer.className = 'toast-container';
    document.body.appendChild(_toastContainer);
  }
  return _toastContainer;
}

export function showToast(message, type = 'info', duration = 4000) {
  const container = _getToastContainer();
  const toast = document.createElement('div');
  toast.className = `toast${type !== 'info' ? ' ' + type : ''}`;
  toast.textContent = message;
  container.appendChild(toast);
  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transition = 'opacity .3s';
    setTimeout(() => toast.remove(), 300);
  }, duration);
}

// ── Core fetch wrapper ────────────────────────────────────────────────────
export async function apiFetch(path, options = {}) {
  try {
    const res = await fetch('/api' + path, {
      headers: { 'Content-Type': 'application/json', ...options.headers },
      ...options,
    });
    if (!res.ok) {
      let detail = `HTTP ${res.status}`;
      try {
        const err = await res.json();
        detail = err.detail || err.message || detail;
      } catch (_) {}
      throw new ApiError(detail, res.status);
    }
    // Handle non-JSON (file downloads)
    const ct = res.headers.get('content-type') || '';
    if (ct.includes('application/json')) return res.json();
    return res;
  } catch (err) {
    if (err instanceof ApiError) throw err;
    throw new ApiError(err.message || 'Network error', 0);
  }
}

export class ApiError extends Error {
  constructor(message, status) {
    super(message);
    this.status = status;
  }
}

// ── Convenience methods ───────────────────────────────────────────────────
export const api = {
  get:  (path, params) => {
    const url = params ? path + '?' + new URLSearchParams(params) : path;
    return apiFetch(url);
  },
  post: (path, body) => apiFetch(path, {
    method: 'POST',
    body: JSON.stringify(body),
  }),
  upload: (path, formData) => fetch('/api' + path, {
    method: 'POST',
    body: formData,
  }).then(async res => {
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new ApiError(err.detail || `HTTP ${res.status}`, res.status);
    }
    return res.json();
  }),
};

// ── Helpers ───────────────────────────────────────────────────────────────
export function fmtPct(val, decimals = 2) {
  if (val == null) return '—';
  const sign = val >= 0 ? '+' : '';
  return `${sign}${val.toFixed(decimals)}%`;
}

export function fmtVal(val, decimals = 1) {
  if (val == null) return '—';
  return val.toFixed(decimals);
}

export function pctClass(val) {
  if (val == null) return 'neu';
  return val > 0 ? 'pos' : val < 0 ? 'neg' : 'neu';
}

export const SEGMENT_BADGES = {
  secondary_packaging: { label: 'Secondary pkg', cls: 'badge-secondary' },
  emn:                 { label: 'EMN',            cls: 'badge-emn' },
  logistics:           { label: 'Logistics',      cls: 'badge-logistics' },
  primary_pkg_md:      { label: 'Primary pkg & MD', cls: 'badge-primary' },
  api_chemicals:       { label: 'API & chemicals', cls: 'badge-api' },
};

export function segBadge(segment) {
  const m = SEGMENT_BADGES[segment] || { label: segment, cls: 'badge-source' };
  return `<span class="badge ${m.cls}">${m.label}</span>`;
}

export function sourceBadge(source) {
  return `<span class="badge badge-source">${source}</span>`;
}

export function navActive(page) {
  document.querySelectorAll('.nav-links a').forEach(a => {
    a.classList.toggle('active', a.dataset.page === page);
  });
}

/** Trigger file download via URL */
export function downloadUrl(url) {
  window.location.href = url;
}

/** Render a small external-link icon for a source_url (returns '' if null) */
export function sourceLink(url) {
  if (!url) return '';
  const escaped = String(url).replace(/"/g, '&quot;');
  return `<a href="${escaped}" target="_blank" rel="noopener noreferrer" class="source-link" title="Open data source">
    <svg width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.5">
      <path d="M5 2H2v8h8V7M7 1h4v4M11 1L6 6"/>
    </svg>
  </a>`;
}
