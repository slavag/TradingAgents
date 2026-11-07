const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const root = path.join(__dirname, '..', '..', 'tradingagents', 'web', 'static');

function runtime(fetcher, persisted = {}) {
  const nodes = new Map();
  const events = [];
  const saved = new Map(Object.entries(persisted));
  const window = { TradingAgentsModelPicker: require(path.join(root, 'model_picker.js')),
    dispatchEvent(event) { events.push(event.type); },
    localStorage: { getItem(key) { return saved.get(key) ?? null; },
      setItem(key, value) { saved.set(key, value); } } };
  const context = vm.createContext({ window, document: {
    getElementById(id) {
      if (!nodes.has(id)) nodes.set(id, { value: id === 'discovery-limit' ? '20' : '',
        checked: false, options: [], classList: { add() {}, remove() {}, toggle() {} },
        style: { setProperty() {} }, innerHTML: '', textContent: '', dataset: {},
        attributes: {}, setAttribute(name, value) { this.attributes[name] = value; } });
      return nodes.get(id);
    },
    querySelectorAll() { return []; },
  }, fetch: fetcher, URL, Date, console, Set, Map, Event: class { constructor(type) { this.type = type; } } });
  const source = fs.readFileSync(path.join(root, 'app.js'), 'utf8').replace(/\nboot\(\);\s*$/, '');
  vm.runInContext(source, context);
  return { context, nodes, events, saved };
}

function payload(ticker) {
  return { status: 'fresh', retrieved_at: '2026-10-16T12:00:00Z', quote_as_of: '2026-10-15',
    warnings: [], items: [{ ticker, company_name: ticker, close: 10, currency: 'USD',
      quote_date: '2026-10-15', ret_1d_pct: 1, ret_5d_pct: 2, ret_20d_pct: 3,
      avg_dollar_volume: 11000000, relative_volume: 1.1, trend_checks: 1,
      score: 4, sources: ['apewisdom'], reason: 'Measured move', risk_flags: [],
      already_analyzed: false }] };
}

test('newer Discover response wins over a late older response, with 15 and 20 requests', async () => {
  const pending = [];
  const { context, nodes } = runtime((url) => new Promise((resolve) => pending.push({ url, resolve })));
  const first = vm.runInContext('loadMomentumDiscovery({force:false})', context);
  nodes.get('discovery-limit').value = '15';
  const second = vm.runInContext('loadMomentumDiscovery({force:true})', context);
  assert.match(pending[0].url, /limit=20&refresh=false/);
  assert.match(pending[1].url, /limit=15&refresh=true/);
  pending[1].resolve({ ok: true, json: async () => payload('PENG') });
  await second;
  pending[0].resolve({ ok: true, json: async () => payload('AMD') });
  await first;
  assert.match(nodes.get('ticker-track').innerHTML, /PENG/);
  assert.doesNotMatch(nodes.get('ticker-track').innerHTML, /AMD/);
});

test('card Add moves ticker to analysis without submitting the form', () => {
  const { context, nodes, events } = runtime(() => { throw Error('No request expected'); });
  vm.runInContext("handleTickerTrackClick({target:{closest(selector){return selector==='[data-discover-add]'?{getAttribute(){return 'PENG';}}:null;}}})", context);
  assert.equal(nodes.get('tickers').value, 'PENG');
  assert.deepEqual(events, ['tradingagents:ticker-added']);
});

test('rows and tiles toggle preserves recommendations without fetching', () => {
  const { context, nodes, saved } = runtime(() => { throw Error('View switch must not fetch'); });
  nodes.get('ticker-track').innerHTML = '<article>AMD</article>';
  vm.runInContext("state.speakingItems = [{ticker:'AMD'}]; setDiscoveryView('rows')", context);
  assert.equal(nodes.get('ticker-track').dataset.view, 'rows');
  assert.equal(nodes.get('ticker-track').innerHTML, '<article>AMD</article>');
  assert.equal(nodes.get('discovery-view-rows').attributes['aria-pressed'], 'true');
  assert.equal(nodes.get('discovery-view-tiles').attributes['aria-pressed'], 'false');
  assert.equal(saved.get('tradingagents-discover-view'), undefined);
  vm.runInContext("setDiscoveryView('tiles')", context);
  assert.equal(nodes.get('ticker-track').dataset.view, 'tiles');
  assert.equal(nodes.get('discovery-view-tiles').attributes['aria-pressed'], 'true');
  assert.equal(nodes.get('ticker-track').innerHTML, '<article>AMD</article>');
});

test('new page defaults to tiles without reading a stored view', () => {
  const rows = runtime(() => { throw Error('Restoring view must not fetch'); },
    { 'tradingagents-discover-view': 'rows' });
  vm.runInContext("setDiscoveryView('tiles')", rows.context);
  assert.equal(rows.nodes.get('ticker-track').dataset.view, 'tiles');
  assert.equal(rows.saved.get('tradingagents-discover-view'), 'rows');
});
