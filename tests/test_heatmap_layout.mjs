/* Tests for the HeatmapGrid layout algorithm (runs in plain node). */
import { createRequire } from 'module';
import assert from 'node:assert/strict';
const { computeHeatmapLayout } = createRequire(import.meta.url)('../static/heatmap.js');

function invariants(n, W, H, g, L) {
  assert(L, 'layout must exist');
  const { columns: c, rows: r, cell: s } = L;
  assert(c >= 1, `columns >= 1 (got ${c})`);
  assert(s > 0, `cell size > 0 (got ${s})`);
  assert(r === Math.ceil(n / c), `rows = ceil(N/c): ${r} vs ${Math.ceil(n / c)}`);
  assert(c * s + (c - 1) * g <= W + 1e-6, `width fits: ${c * s + (c - 1) * g} <= ${W}`);
  assert(r * s + (r - 1) * g <= H + 1e-6, `height fits: ${r * s + (r - 1) * g} <= ${H}`);
  assert((r - 1) * c < n && r * c >= n,
    `last row has ${n - (r - 1) * c} cells (must be >= 1)`);
  return L;
}

/* 1. spec worked example: N=7, W=300, H=100, g=10 -> 6 columns, 2 rows (last row 1 cell) */
{
  const L = invariants(7, 300, 100, 10, computeHeatmapLayout(7, 300, 100, 10));
  assert.equal(L.columns, 6);
  assert.equal(L.rows, 2);
  assert.ok(Math.abs(L.cell - (300 - 50) / 6) < 1e-9, `cell = 250/6, got ${L.cell}`);
}

/* 2. hand-computed regression: N=1024, W=800, H=600, g=2 -> 38 cols, 27 rows */
{
  const L = invariants(1024, 800, 600, 2, computeHeatmapLayout(1024, 800, 600, 2));
  assert.equal(L.columns, 38);
  assert.equal(L.rows, 27);
  assert.ok(Math.abs(L.cell - 726 / 38) < 1e-9, `cell = 726/38, got ${L.cell}`);
  assert.ok(Math.abs(L.score - 1) < 1e-9, 'width fully used');
}

/* 3. very wide, short container: several rows beat one dense row on cell size
      while still filling the width: 7 rows of 2000/147 px fit in H=100 */
{
  const L = invariants(1024, 2000, 100, 0, computeHeatmapLayout(1024, 2000, 100, 0));
  assert.equal(L.columns, 147);
  assert.equal(L.rows, 7);
  assert.ok(Math.abs(L.cell - 2000 / 147) < 1e-9);
  assert.ok(Math.abs(L.score - 1) < 1e-9, 'width fully used');
}

/* 4. single cell: takes the largest square that fits */
{
  const L = computeHeatmapLayout(1, 300, 200, 5);
  assert.equal(L.columns, 1);
  assert.equal(L.rows, 1);
  assert.ok(Math.abs(L.cell - 200) < 1e-9);
}

/* 5. N=2 in a wide, short box: two side-by-side squares capped by the height,
      leftover width left empty on the right */
{
  const L = invariants(2, 400, 100, 0, computeHeatmapLayout(2, 400, 100, 0));
  assert.equal(L.columns, 2);
  assert.equal(L.rows, 1);
  assert.ok(Math.abs(L.cell - 100) < 1e-9);
  assert.ok(Math.abs(L.score - 0.5) < 1e-9);
}

/* 6. tiny container: all N cells still placed (sub-pixel sizes allowed) */
{
  const L = invariants(1024, 50, 50, 1, computeHeatmapLayout(1024, 50, 50, 1));
  assert.equal(L.columns * L.rows >= 1024, true);
}

/* 7. tall narrow container: cells capped by height, width still filled best */
{
  const L = invariants(20, 60, 1000, 3, computeHeatmapLayout(20, 60, 1000, 3));
  assert.equal(L.columns, 2);
  assert.equal(L.rows, 10);
  assert.ok(Math.abs(L.cell - 28.5) < 1e-9);
}

/* 8. degenerate inputs */
assert.equal(computeHeatmapLayout(0, 800, 600, 2), null);
assert.equal(computeHeatmapLayout(10, 0, 600, 2), null);
assert.equal(computeHeatmapLayout(10, 800, 0, 2), null);

/* 9. fuzz: random boxes must satisfy every invariant, and no smaller-cell
      solution may fill the width better */
{
  let seed = 42;
  const rnd = () => (seed = (seed * 1103515245 + 12345) % 2147483648) / 2147483648;
  for (let t = 0; t < 500; t++) {
    const N = 1 + Math.floor(rnd() * 1500);
    const W = 10 + Math.floor(rnd() * 2000);
    const H = 10 + Math.floor(rnd() * 1200);
    const g = Math.floor(rnd() * 8);
    const L = invariants(N, W, H, g, computeHeatmapLayout(N, W, H, g));

    /* width-fill optimality: brute force over all column counts */
    let bestScore = 0, bestS = 0;
    for (let c = 1; c <= N; c++) {
      const sw = (W - (c - 1) * g) / c;
      if (sw <= 0) break;
      const r = Math.ceil(N / c);
      const sh = (H - (r - 1) * g) / r;
      if (sh <= 0) continue;
      const s = Math.min(sw, sh);
      const score = (c * s + (c - 1) * g) / W;
      if (score > bestScore + 1e-12 || (Math.abs(score - bestScore) <= 1e-12 && s > bestS)) {
        bestScore = score; bestS = s;
      }
    }
    assert.ok(Math.abs(L.score - bestScore) <= 1e-9,
      `score optimal: ${L.score} vs ${bestScore} (N=${N} W=${W} H=${H} g=${g})`);
  }
}

console.log('heatmap layout tests: all passed');
