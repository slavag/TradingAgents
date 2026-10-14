(function () {
"use strict";

function mergeModelChoices(role, provider, selectedValue, discoveredIds, roleOptions) {
  const curated = roleOptions[role]?.[provider] || [];
  const seen = new Set(curated.map(([, id]) => id));
  const merged = curated.map(([label, id]) => [label, id]);
  for (const id of discoveredIds) {
    if (typeof id === "string" && id && !seen.has(id)) {
      merged.push([`${id} · Available in this account`, id]);
      seen.add(id);
    }
  }
  if (selectedValue && !seen.has(selectedValue)) {
    merged.push([`${selectedValue} · Configured model (unverified)`, selectedValue]);
  }
  return merged;
}

function isCurrentRefresh(expectedRole, expectedProvider, expectedToken,
                          currentRole, currentProvider, currentToken) {
  return expectedRole === currentRole && expectedProvider === currentProvider
    && expectedToken === currentToken;
}

function preferredModelForProvider(role, provider, remembered, roleDefaults) {
  if (remembered[role]?.[provider]) return remembered[role][provider];
  const configured = roleDefaults[role];
  return configured?.provider === provider ? configured.model : null;
}

function canRefreshProvider(provider, supportedProviders) {
  return supportedProviders.includes(provider);
}

if (typeof module !== "undefined" && module.exports) {
  module.exports = { mergeModelChoices, isCurrentRefresh, preferredModelForProvider, canRefreshProvider };
}
if (typeof window !== "undefined") {
  window.TradingAgentsModelPicker = { mergeModelChoices, isCurrentRefresh, preferredModelForProvider, canRefreshProvider };
}
})();
