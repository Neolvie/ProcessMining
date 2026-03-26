'use strict';

async function api(path, params = {}) {
  const qs = new URLSearchParams(
    Object.entries(params).filter(([, v]) => v != null && v !== '')
  ).toString();
  const url = '/api' + path + (qs ? '?' + qs : '');
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error ${res.status}: ${path}`);
  return res.json();
}
