/*
 * HeatmapGrid — responsive layout of exactly N equal-sized square cells
 * inside a variable-sized rectangular container.
 *
 * Contract:
 *  - exactly N cells, all equal size, strict 1:1 aspect ratio
 *  - cells fill row by row, left to right, top to bottom, starting from the width
 *  - columns = max whole cells that fit the width for the chosen cell size;
 *    leftover width stays empty on the right (cells are never stretched)
 *  - the last row may be full, partial, or a single cell — all N cells are placed
 *  - no scrolling, clipping or overflow: the layout always fits the container
 *  - cell size is maximized: maximize width usage (totalWidth / W) first,
 *    tie-break by larger cell size — while fitting all rows inside the height
 *  - optional gap, accounted for in every calculation
 *  - fully responsive: recomputes and re-renders whenever the container resizes
 */
(function (global) {
  'use strict';

  /**
   * Compute the layout for n cells in a W×H container with gap g.
   * Returns { columns, rows, cell, score } or null when nothing fits (n<=0 or zero-sized box).
   */
  function computeHeatmapLayout(n, W, H, g) {
    if (!(n > 0) || !(W > 0) || !(H > 0)) return null;
    var best = null;
    for (var c = 1; c <= n; c++) {
      var sw = (W - (c - 1) * g) / c;      // max side that fits c columns across the width
      if (sw <= 0) break;                  // wider only shrinks sw further
      var r = Math.ceil(n / c);
      var sh = (H - (r - 1) * g) / r;      // max side that fits r rows down the height
      if (sh <= 0) continue;               // too tall at this column count; more columns may fit
      var s = Math.min(sw, sh);
      var totalWidth = c * s + (c - 1) * g;
      var score = totalWidth / W;
      if (!best || score > best.score + 1e-9 ||
          (Math.abs(score - best.score) <= 1e-9 && s > best.s)) {
        best = { columns: c, rows: r, cell: s, score: score };
      }
    }
    return best;
  }

  /**
   * Create a grid inside `container` (gets position:relative + overflow:hidden).
   * opts: { n = 1024, gap = 0, render(i, el, side, col, row) }
   * Returns { setN, getN, getLayout, refresh, destroy }.
   */
  function HeatmapGrid(container, opts) {
    opts = opts || {};
    var n = Math.max(0, opts.n === undefined ? 1024 : (opts.n | 0));
    var gap = Math.max(0, opts.gap || 0);
    var renderCell = opts.render || function () {};

    container.classList.add('hm-container');
    var inner = document.createElement('div');
    inner.className = 'hm-inner';
    container.appendChild(inner);

    var cells = [];
    var layout = null;

    function ensureCells(count) {
      while (cells.length < count) {
        var el = document.createElement('div');
        el.className = 'hm-cell';
        inner.appendChild(el);
        cells.push(el);
      }
      for (var k = count; k < cells.length; k++) cells[k].remove();
      cells.length = count;
      for (var i = 0; i < count; i++) cells[i].dataset.i = i;
    }

    function layoutNow() {
      var W = inner.clientWidth, H = inner.clientHeight;
      layout = computeHeatmapLayout(n, W, H, gap);
      if (!layout) { ensureCells(0); return; }
      ensureCells(n);
      var c = layout.columns, s = layout.cell;
      for (var i = 0; i < n; i++) {
        var col = i % c, row = (i / c) | 0;
        var el = cells[i];
        el.style.left = (col * (s + gap)) + 'px';
        el.style.top = (row * (s + gap)) + 'px';
        el.style.width = s + 'px';
        el.style.height = s + 'px';
        renderCell(i, el, s, col, row);
      }
    }

    var raf = 0;
    function schedule() {
      if (!raf) {
        raf = requestAnimationFrame(function () { raf = 0; layoutNow(); });
      }
    }

    var ro = null;
    if (typeof ResizeObserver !== 'undefined') {
      ro = new ResizeObserver(schedule);
      ro.observe(container);
    }
    window.addEventListener('resize', schedule);
    layoutNow();

    return {
      setN: function (v) { n = Math.max(0, v | 0); schedule(); },
      getN: function () { return n; },
      getLayout: function () { return layout ? Object.assign({}, layout) : null; },
      refresh: schedule,
      destroy: function () {
        if (ro) ro.disconnect();
        window.removeEventListener('resize', schedule);
        inner.remove();
      }
    };
  }

  var api = { HeatmapGrid: HeatmapGrid, computeHeatmapLayout: computeHeatmapLayout };
  global.NVHeatmap = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
