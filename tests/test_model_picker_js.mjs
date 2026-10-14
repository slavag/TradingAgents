import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';

const require = createRequire(import.meta.url);
const {
  mergeModelChoices,
  isCurrentRefresh,
  preferredModelForProvider,
  canRefreshProvider,
} = require('../tradingagents/web/static/model_picker.js');

const roleOptions = {
  quick: { openai: [['Quick', 'gpt-6-luna'], ['Custom model ID', 'custom']] },
  deep: { openai: [['Deep', 'gpt-6.1-sol'], ['Custom model ID', 'custom']] },
};

test('a configured legacy model survives an empty account list', () => {
  const result = mergeModelChoices('quick', 'openai', 'gpt-5.4-mini', [], roleOptions);
  assert.equal(result.at(-1)[1], 'gpt-5.4-mini');
  assert.match(result.at(-1)[0], /Configured model \(unverified\)/);
  assert.equal(result[0][1], 'gpt-6-luna');
});

test('newly available models follow recommendations without changing selected ID', () => {
  const result = mergeModelChoices('quick', 'openai', 'gpt-6-luna',
    ['gpt-6-luna', 'account/special'], roleOptions);
  assert.deepEqual(result.map(([, id]) => id), ['gpt-6-luna', 'custom', 'account/special']);
  assert.match(result.at(-1)[0], /Available in this account/);
});

test('merging one role never mutates another role catalog', () => {
  const before = structuredClone(roleOptions.deep);
  mergeModelChoices('quick', 'openai', 'gpt-6-luna', ['account/special'], roleOptions);
  assert.deepEqual(roleOptions.deep, before);
});

test('late responses for a switched provider or newer request are discarded', () => {
  assert.equal(isCurrentRefresh('quick', 'openai', 1, 'quick', 'openai', 1), true);
  assert.equal(isCurrentRefresh('quick', 'openai', 1, 'quick', 'anthropic', 1), false);
  assert.equal(isCurrentRefresh('quick', 'openai', 1, 'quick', 'openai', 2), false);
  assert.equal(isCurrentRefresh('quick', 'openai', 1, 'deep', 'openai', 1), false);
});

test('classic helper script does not collide with app global lexical names', () => {
  const source = readFileSync(fileURLToPath(new URL('../tradingagents/web/static/model_picker.js', import.meta.url)), 'utf8');
  const context = vm.createContext({ window: {} });
  vm.runInContext(source, context);
  assert.doesNotThrow(() => vm.runInContext(
    'const { mergeModelChoices, isCurrentRefresh } = window.TradingAgentsModelPicker;', context,
  ));
});

test('switching back to configured provider restores its saved model', () => {
  const remembered = { quick: { anthropic: 'claude-haiku-5-5' } };
  const defaults = { quick: { provider: 'openai', model: 'gpt-5.4-mini' } };
  assert.equal(preferredModelForProvider('quick', 'openai', remembered, defaults), 'gpt-5.4-mini');
  assert.equal(preferredModelForProvider('quick', 'anthropic', remembered, defaults), 'claude-haiku-5-5');
  assert.equal(preferredModelForProvider('quick', 'google', remembered, defaults), null);
});

test('only providers with a verified listing API offer refresh', () => {
  const supported = ['openai', 'anthropic', 'ollama'];
  assert.equal(canRefreshProvider('openai', supported), true);
  assert.equal(canRefreshProvider('azure', supported), false);
  assert.equal(canRefreshProvider('openai_compatible', supported), false);
});
