'use strict';

/* NVMe Monitor dashboard — every value comes from the backend API; no mock data. */

/* ---------- tiny helpers ---------- */
const $ = (s, r = document) => r.querySelector(s);
const enc = encodeURIComponent;
const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

const SEV_COLORS = {
  excellent: '#22c55e',
  acceptable: '#a3e635',
  warning: '#f59e0b',
  critical: '#ef4444',
  none: '#2b3542',
};
const STATE_COLORS = {
  idle: '#22c55e', light: '#a3e635', healthy: '#22d3ee',
  saturated: '#f59e0b', sick: '#f97316', critical: '#ef4444',
};

const FLEET_METRICS = [
  ['combined_latency_ms', 'Combined latency'],
  ['read_latency_ms', 'Read latency'],
  ['write_latency_ms', 'Write latency'],
  ['concurrency_util', 'Concurrency util'],
  ['iops_util', 'IOPS util'],
  ['bandwidth_util', 'Bandwidth util'],
  ['busy_ratio', 'Busy'],
  ['flush_time_share', 'Flush share'],
  ['discard_time_share', 'Discard share'],
  ['temperature_celsius', 'Temperature'],
];
const HEAT_UNITS = {
  write_latency_ms: 'ms', read_latency_ms: 'ms', combined_latency_ms: 'ms',
  concurrency_util: 'percent', iops_util: 'percent', bandwidth_util: 'percent',
  busy_ratio: 'percent', flush_time_share: 'percent', discard_time_share: 'percent',
  temperature_celsius: '°C',
};
const CHART_ORDER = [
  'total_iops', 'read_iops', 'write_iops',
  'read_bandwidth', 'write_bandwidth',
  'combined_latency', 'read_latency', 'write_latency',
  'avg_queue_depth', 'busy_ratio',
  'concurrency_util', 'iops_util', 'bandwidth_util',
  'saturation_state', 'ios_in_progress',
];

function esc(s) {
  return String(s).replace(/[&<>"]/g, c => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]
  ));
}

function fmtVal(v, unit) {
  if (v === null || v === undefined || Number.isNaN(v)) return '—';
  switch (unit) {
    case 'percent': return v.toFixed(1) + '%';
    case 'ms': return v >= 100 ? Math.round(v) + ' ms' : v.toFixed(2) + ' ms';
    case 'iops': return v >= 1000 ? (v / 1000).toFixed(1) + 'k' : String(Math.round(v));
    case 'MB/s': return v >= 1000 ? (v / 1000).toFixed(1) + ' GB/s' : v.toFixed(1) + ' MB/s';
    case '°C': return v.toFixed(1) + '°C';
    case 'count': return String(Math.round(v));
    default: return String(Math.round(v * 100) / 100);
  }
}

function fmtTick(v, unit) {
  if (unit === 'percent') return Math.round(v) + '%';
  if (unit === 'ms') return v >= 1000 ? (v / 1000).toFixed(1) + 's' : Math.round(v) + 'ms';
  if (unit === 'iops') return v >= 1000 ? (v / 1000).toFixed(1) + 'k' : String(Math.round(v));
  if (unit === 'MB/s') return v >= 1000 ? (v / 1000).toFixed(1) + 'G' : Math.round(v) + 'M';
  return String(Math.round(v * 100) / 100);
}

function fmtTime(ts) {
  const d = new Date(ts * 1000);
  const hm = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false });
  return d.toDateString() === new Date().toDateString()
    ? hm
    : `${d.getMonth() + 1}/${d.getDate()} ${hm}`;
}

async function api(path) {
  const r = await fetch(path);
  if (!r.ok) {
    let detail = r.statusText;
    try {
      const j = await r.json();
      if (j.detail !== undefined) {
        detail = typeof j.detail === 'string' ? j.detail : JSON.stringify(j.detail);
      }
    } catch (_) { /* keep statusText */ }
    const e = new Error(detail);
    e.status = r.status;
    throw e;
  }
  return r.json();
}

/* ---------- state ---------- */
const state = {
  node: 'all',
  disk: null,
  disks: [],
  nodeNames: [],
  windowSec: 86400,
  fleetMetric: 'combined_latency_ms',
  heatMetric: 'combined_latency_ms',
  heatN: 1024,
  chartMetrics: new Set(['total_iops', 'combined_latency', 'busy_ratio']),
  pollMs: 5000,
  catalog: {},
  fleetCells: [],   // [{node, disk, status, severity, value}]
  histRaw: [],      // raw history cells [{ts, value, severity}]
  lastSeries: null, // { d, from, to } for chart redraws on resize
};

/* ---------- shared tooltip ---------- */
const tip = $('#tooltip');
document.addEventListener('mousemove', e => {
  const t = e.target.closest('.hm-cell');
  if (t && t._tip) {
    tip.hidden = false;
    tip.textContent = t._tip;
    tip.style.left = Math.min(window.innerWidth - tip.offsetWidth - 8, e.clientX + 14) + 'px';
    tip.style.top = (e.clientY + 16) + 'px';
  } else {
    tip.hidden = true;
  }
});

/* ---------- fleet heatmap (HeatmapGrid, N = number of disks) ---------- */
const fleetGrid = NVHeatmap.HeatmapGrid($('#fleet-grid'), {
  n: 0,
  gap: 4,
  render(i, el, s) {
    const d = state.fleetCells[i];
    el._tip = d
      ? `${d.node} / ${d.disk}\n${state.fleetMetric}: ${fmtVal(d.value, HEAT_UNITS[state.fleetMetric])}\nseverity: ${d.severity}${d.status && d.status !== 'active' ? '\nstatus: ' + d.status : ''}`
      : '';
    el.style.background = d ? (SEV_COLORS[d.severity] || SEV_COLORS.none) : SEV_COLORS.none;
    el.classList.toggle('clickable', !!d);
    el.classList.toggle('cell-selected',
      !!d && d.node === state.node && d.disk === state.disk);
    if (d && s > 56) {
      el.style.fontSize = clamp(s * 0.13, 10, 15) + 'px';
      el.innerHTML =
        `<span class="cell-disk">${esc(d.disk)}</span>` +
        `<span class="cell-sub">${esc(fmtVal(d.value, HEAT_UNITS[state.fleetMetric]))}</span>`;
    } else {
      el.innerHTML = '';
    }
  },
});

$('#fleet-grid').addEventListener('click', e => {
  const cell = e.target.closest('.hm-cell');
  if (!cell) return;
  const d = state.fleetCells[Number(cell.dataset.i)];
  if (!d) return;
  setNode(d.node, d.disk);
});

/* ---------- history heatmap (HeatmapGrid, N = user-set, default 1024) ---------- */
const histGrid = NVHeatmap.HeatmapGrid($('#history-heat'), {
  n: state.heatN,
  gap: 2,
  render(i, el) {
    let p = null;
    if (state.histRaw.length) {
      p = state.histRaw[Math.floor(i * state.histRaw.length / state.heatN)]
        || state.histRaw[state.histRaw.length - 1];
    }
    el._tip = p
      ? `${fmtTime(p.ts)}\n${state.heatMetric}: ${fmtVal(p.value, HEAT_UNITS[state.heatMetric])}\nseverity: ${p.severity}`
      : '';
    el.style.background = p ? (SEV_COLORS[p.severity] || SEV_COLORS.none) : SEV_COLORS.none;
  },
});

/* ---------- node list + header status ---------- */
async function loadNodes() {
  const data = await api('/nodes');
  state.nodeNames = data.nodes.map(n => n.name);

  const sel = $('#node-select');
  const prev = state.node;
  sel.innerHTML = '';
  sel.add(new Option('All nodes', 'all'));
  for (const n of data.nodes) {
    const disks = `${n.device_count} disk${n.device_count === 1 ? '' : 's'}`;
    sel.add(new Option(`${n.name} — ${n.status}, ${disks}`, n.name));
  }
  if ([...sel.options].some(o => o.value === prev)) {
    sel.value = prev;
  } else {
    sel.value = 'all';
    state.node = 'all';
  }

  const dot = $('#hdr-dot');
  const current = data.nodes.find(n => n.name === state.node);
  const status = current ? current.status : (state.node === 'all' ? 'active' : null);
  dot.className = 'dot ' + (status === 'active' || status === 'live' ? 'ok'
    : status === 'stale' ? 'warn' : status === 'off' ? 'bad' : '');
}

/* ---------- fleet data ---------- */
async function refreshFleet() {
  const nodes = state.node === 'all' ? state.nodeNames : [state.node];
  const cells = [];
  for (const node of nodes) {
    try {
      const d = await api(`/nodes/${enc(node)}/heatmap/current`);
      for (const dev of d.devices) {
        const cell = dev.cells ? dev.cells[state.fleetMetric] : null;
        cells.push({
          node, disk: dev.name, status: dev.status,
          severity: cell ? cell.severity : 'none',
          value: cell ? cell.value : null,
        });
      }
    } catch (_) {
      try {
        const d = await api(`/nodes/${enc(node)}/disks`);
        for (const dev of d.disks) {
          cells.push({ node, disk: dev.name, status: 'unreachable', severity: 'none', value: null });
        }
      } catch (_) { /* node gone */ }
    }
  }
  state.fleetCells = cells;
  $('#fleet-empty').hidden = cells.length > 0;
  fleetGrid.setN(cells.length);
  fleetGrid.refresh();
}

/* ---------- per-disk detail ---------- */
async function loadDisks() {
  const sel = $('#disk-select');
  sel.innerHTML = '';
  const all = state.node === 'all';
  $('#detail-disabled').hidden = !all;
  $('#detail-panel').classList.toggle('disabled', all);
  if (all) {
    state.disks = [];
    state.disk = null;
    return;
  }
  try {
    const d = await api(`/nodes/${enc(state.node)}/disks`);
    state.disks = d.disks;
    for (const disk of d.disks) {
      sel.add(new Option(disk.model ? `${disk.name} — ${disk.model}` : disk.name, disk.name));
    }
    if (!d.disks.some(x => x.name === state.disk)) {
      state.disk = d.disks.length ? d.disks[0].name : null;
    }
    if (state.disk) sel.value = state.disk;
  } catch (e) {
    state.disks = [];
    state.disk = null;
    $('#live-strip').innerHTML = `<div class="error">disks: ${esc(e.message)}</div>`;
  }
}

async function refreshLive() {
  if (state.node === 'all' || !state.disk) return;
  try {
    const d = await api(`/nodes/${enc(state.node)}/disks/${enc(state.disk)}/stat`);
    const der = d.derived || {};
    const cards = [
      ['state', der.saturation_state_name || '—', 'st-' + (der.saturation_state_name || '')],
      ['IOPS', fmtVal(der.total_iops, 'iops')],
      ['read', fmtVal(der.read_megabytes_per_second, 'MB/s')],
      ['write', fmtVal(der.write_megabytes_per_second, 'MB/s')],
      ['latency', fmtVal(der.combined_latency_ms, 'ms')],
      ['queue depth', fmtVal(der.avg_queue_depth, 'count')],
      ['busy', fmtVal(der.busy_percent, 'percent')],
      ['iops util', fmtVal(der.iops_util_percent, 'percent')],
      ['bw util', fmtVal(der.bandwidth_util_percent, 'percent')],
      ['temp', fmtVal(der.temperature_celsius, '°C')],
    ];
    $('#live-strip').innerHTML = cards.map(([k, v, cls]) =>
      `<div class="stat"><span class="k">${esc(k)}</span>` +
      `<span class="v ${cls || ''}">${esc(v)}</span></div>`
    ).join('') + (d.warning ? `<div class="warn">${esc(d.warning)}</div>` : '');
  } catch (e) {
    $('#live-strip').innerHTML = `<div class="error">live stat: ${esc(e.message)}</div>`;
  }
}

/* ---------- charts (small multiples, hand-rolled SVG) ---------- */
function drawChart(mount, points, opt) {
  mount.innerHTML = '';
  const W = mount.clientWidth, H = mount.clientHeight;
  if (W < 40 || H < 40) return;
  const NS = 'http://www.w3.org/2000/svg';
  const svg = document.createElementNS(NS, 'svg');
  svg.setAttribute('width', W);
  svg.setAttribute('height', H);
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
  const el = (name, attrs) => {
    const n = document.createElementNS(NS, name);
    for (const k in attrs) n.setAttribute(k, attrs[k]);
    return n;
  };

  const m = { l: 56, r: 12, t: 10, b: 22 };
  const iw = W - m.l - m.r, ih = H - m.t - m.b;
  const from = opt.from, to = opt.to;
  const enumMode = !!opt.valueLabels;

  let yMax = 0;
  for (const [, v] of points) if (v > yMax) yMax = v;
  if (enumMode) {
    yMax = Math.max(...Object.keys(opt.valueLabels).map(Number), 0);
  } else {
    yMax = yMax > 0 ? yMax * 1.08 : 1;
  }
  const X = ts => m.l + iw * ((ts - from) / Math.max(1e-9, to - from));
  const Y = v => m.t + ih * (1 - v / yMax);

  const yTicks = enumMode
    ? Object.keys(opt.valueLabels).map(Number)
    : [0, 0.25, 0.5, 0.75, 1].map(f => f * yMax);
  for (const tv of yTicks) {
    const y = Y(tv);
    svg.appendChild(el('line', { x1: m.l, x2: W - m.r, y1: y, y2: y, class: 'grid' }));
    const label = enumMode ? opt.valueLabels[String(tv)] : fmtTick(tv, opt.unit);
    svg.appendChild(el('text', { x: m.l - 6, y: y + 3, 'text-anchor': 'end' })).textContent = label;
  }
  for (let k = 0; k <= 4; k++) {
    const ts = from + (to - from) * (k / 4);
    svg.appendChild(el('line', {
      x1: X(ts), x2: X(ts), y1: m.t, y2: m.t + ih, class: 'axis',
    }));
    svg.appendChild(el('text', { x: X(ts), y: H - 6, 'text-anchor': k === 0 ? 'start' : k === 4 ? 'end' : 'middle' }))
      .textContent = fmtTime(ts);
  }

  if (!points.length) {
    svg.appendChild(el('text', { x: W / 2, y: H / 2, 'text-anchor': 'middle' }))
      .textContent = 'no data for this window';
    mount.appendChild(svg);
    return;
  }

  const path = points.map(([ts, v], i) =>
    `${i ? 'L' : 'M'}${X(ts).toFixed(1)},${Y(v).toFixed(1)}`).join(' ');
  const last = points[points.length - 1];
  svg.appendChild(el('path', {
    d: `${path}L${X(last[0]).toFixed(1)},${Y(0).toFixed(1)}L${X(points[0][0]).toFixed(1)},${Y(0).toFixed(1)}Z`,
    fill: 'rgba(56,189,248,.12)', stroke: 'none',
  }));
  svg.appendChild(el('path', {
    d: path, fill: 'none', stroke: '#38bdf8', 'stroke-width': 1.8,
    'stroke-linejoin': 'round', 'stroke-linecap': 'round',
  }));
  mount.appendChild(svg);
}

function renderCharts() {
  const wrap = $('#charts');
  wrap.innerHTML = '';
  if (!state.lastSeries) return;
  const { d, from, to } = state.lastSeries;
  for (const name of CHART_ORDER) {
    if (!state.chartMetrics.has(name)) continue;
    const s = d.series[name] || { points: [] };
    const card = document.createElement('div');
    card.className = 'chart-card';
    const head = document.createElement('div');
    head.className = 'chart-head';
    const last = s.points.length ? s.points[s.points.length - 1][1] : null;
    const lastLabel = last === null ? 'no data'
      : fmtVal(last, s.unit) + (s.value_labels
        ? ` (${s.value_labels[String(Math.round(last))] || ''})` : '');
    head.innerHTML = `<b>${esc(name)}</b><span class="muted">${esc(lastLabel)}</span>`;
    const body = document.createElement('div');
    body.className = 'chart-body';
    card.append(head, body);
    wrap.append(card);
    drawChart(body, s.points, {
      unit: s.unit, valueLabels: s.value_labels, from, to,
    });
  }
}

async function refreshCharts() {
  if (state.node === 'all' || !state.disk) return;
  if (!state.chartMetrics.size) {
    $('#charts').innerHTML = '<p class="hint">select at least one metric above</p>';
    return;
  }
  const to = Date.now() / 1000;
  const from = to - state.windowSec;
  const metrics = [...state.chartMetrics].join(',');
  try {
    const d = await api(
      `/nodes/${enc(state.node)}/disks/${enc(state.disk)}/timeseries` +
      `?metrics=${enc(metrics)}&from=${from}&to=${to}`
    );
    state.lastSeries = { d, from, to };
    renderCharts();
  } catch (e) {
    $('#charts').innerHTML = `<div class="error">timeseries: ${esc(e.message)}</div>`;
  }
}

/* ---------- history heatmap data ---------- */
async function refreshHistory() {
  if (state.node === 'all' || !state.disk) return;
  const to = Date.now() / 1000;
  const from = to - state.windowSec;
  const step = Math.max(15, Math.round(state.windowSec / state.heatN));
  try {
    const d = await api(
      `/nodes/${enc(state.node)}/disks/${enc(state.disk)}/heatmap` +
      `?metrics=${enc(state.heatMetric)}&from=${from}&to=${to}&step=${step}s`
    );
    const row = d.rows[state.heatMetric];
    state.histRaw = row ? row.cells : [];
    $('#heat-empty').hidden = state.histRaw.length > 0;
    $('#heat-hint').textContent =
      `${state.histRaw.length} points over the window, mapped onto ${state.heatN} cells` +
      ` · step ${step}s`;
    histGrid.setN(state.heatN);
    histGrid.refresh();
  } catch (e) {
    state.histRaw = [];
    $('#heat-empty').hidden = false;
    $('#heat-hint').textContent = 'history heatmap: ' + e.message;
  }
}

/* ---------- metric catalog + chips ---------- */
async function loadCatalog() {
  try {
    const d = await api('/timeseries/metrics');
    state.catalog = d.metrics || {};
  } catch (_) {
    state.catalog = {};
  }
  const chips = $('#chart-chips');
  chips.innerHTML = '';
  for (const name of CHART_ORDER) {
    if (!(name in state.catalog)) continue;
    const b = document.createElement('button');
    b.type = 'button';
    b.className = 'chip' + (state.chartMetrics.has(name) ? ' active' : '');
    b.textContent = name;
    b.addEventListener('click', () => {
      if (state.chartMetrics.has(name)) state.chartMetrics.delete(name);
      else state.chartMetrics.add(name);
      b.classList.toggle('active', state.chartMetrics.has(name));
      refreshCharts();
    });
    chips.appendChild(b);
  }
}

/* ---------- wiring ---------- */
function setNode(node, disk) {
  state.node = node;
  $('#node-select').value = node;
  if (disk) state.disk = disk;
  loadDisks().then(() => {
    tick();
    slowTick();
  });
}

function tick() {
  refreshFleet();
  if (state.node !== 'all' && state.disk) refreshLive();
}

function slowTick() {
  const prev = state.node;
  loadNodes().then(() => {
    if (state.node !== prev) return loadDisks();
  }).then(() => {
    if (state.node !== 'all' && state.disk) {
      refreshCharts();
      refreshHistory();
    }
  });
}

let pollTimer = 0, slowTimer = 0;
function startPolling() {
  clearInterval(pollTimer);
  pollTimer = setInterval(tick, state.pollMs);
}

function buildStaticControls() {
  const fleetSel = $('#fleet-metric');
  const heatSel = $('#heat-metric');
  for (const [value, label] of FLEET_METRICS) {
    fleetSel.add(new Option(label, value));
    heatSel.add(new Option(label, value));
  }
  fleetSel.value = state.fleetMetric;
  heatSel.value = state.heatMetric;

  fleetSel.addEventListener('change', () => {
    state.fleetMetric = fleetSel.value;
    refreshFleet();
  });
  heatSel.addEventListener('change', () => {
    state.heatMetric = heatSel.value;
    refreshHistory();
  });
  $('#node-select').addEventListener('change', e => setNode(e.target.value));
  $('#disk-select').addEventListener('change', e => {
    state.disk = e.target.value;
    tick();
    slowTick();
  });
  $('#poll-select').addEventListener('change', e => {
    state.pollMs = Number(e.target.value);
    startPolling();
  });
  $('#heat-n').addEventListener('change', e => {
    state.heatN = clamp(parseInt(e.target.value, 10) || 1024, 16, 4096);
    e.target.value = state.heatN;
    histGrid.setN(state.heatN);
    histGrid.refresh();
  });
  $('#window-seg').addEventListener('click', e => {
    const b = e.target.closest('button');
    if (!b) return;
    state.windowSec = Number(b.dataset.w);
    for (const x of $('#window-seg').querySelectorAll('button')) {
      x.classList.toggle('active', x === b);
    }
    slowTick();
  });

  const ro = new ResizeObserver(() => renderCharts());
  ro.observe($('#charts'));
}

async function init() {
  buildStaticControls();

  const legend = $('#sev-legend');
  for (const sev of ['excellent', 'acceptable', 'warning', 'critical']) {
    legend.insertAdjacentHTML('beforeend',
      `<span class="swatch"><i style="background:${SEV_COLORS[sev]}"></i>${sev}</span>`);
  }

  await loadCatalog();
  await loadNodes();
  await loadDisks();
  tick();
  slowTick();
  startPolling();
  setInterval(slowTick, 60000);
}

init().catch(err => {
  console.error('init failed', err);
  $('#fleet-empty').textContent = 'backend unreachable: ' + err.message;
  $('#fleet-empty').hidden = false;
});
