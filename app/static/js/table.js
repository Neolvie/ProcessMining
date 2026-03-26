'use strict';

function buildTable(containerId, columns, rows, opts = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;
  if (!rows || rows.length === 0) { el.innerHTML = empty(); return; }

  // User click state takes priority over default sortKey
  const sortKey = _sortState[containerId]?.key ?? opts.sortKey ?? columns[0]?.key;
  const sortDir = _sortState[containerId]?.dir ?? opts.sortDir ?? 'desc';

  const sorted = [...rows].sort((a, b) => {
    const va = a[sortKey] ?? -Infinity;
    const vb = b[sortKey] ?? -Infinity;
    const cmp = typeof va === 'string' ? va.localeCompare(vb, 'ru') : (va - vb);
    return sortDir === 'asc' ? cmp : -cmp;
  });

  const thCells = columns.map(c => {
    const isSorted = c.key === sortKey;
    const cls = isSorted ? (sortDir === 'asc' ? 'sort-asc' : 'sort-desc') : '';
    return `<th class="${cls}" data-key="${c.key}">${c.label}</th>`;
  }).join('');

  const tbRows = sorted.map(row => {
    const tds = columns.map(c => {
      const val = row[c.key];
      const rendered = c.render ? c.render(val, row) : (val ?? '—');
      return `<td>${rendered}</td>`;
    }).join('');
    const rowCls = opts.clickable ? 'clickable-row' : '';
    const rowAttr = opts.onRowClick ? `onclick="${opts.onRowClick}(this)"` : '';
    const dataAttrs = opts.rowData
      ? opts.rowData.map(k => `data-${k}="${row[k] ?? ''}"`).join(' ')
      : '';
    return `<tr class="${rowCls}" ${rowAttr} ${dataAttrs}>${tds}</tr>`;
  }).join('');

  el.innerHTML = `<table>
    <thead><tr>${thCells}</tr></thead>
    <tbody>${tbRows}</tbody>
  </table>`;

  el.querySelectorAll('thead th[data-key]').forEach(th => {
    th.addEventListener('click', () => {
      const key = th.dataset.key;
      const cur = _sortState[containerId] || {};
      const dir = cur.key === key && cur.dir === 'desc' ? 'asc' : 'desc';
      _sortState[containerId] = { key, dir };
      buildTable(containerId, columns, rows, opts);
    });
  });
}
