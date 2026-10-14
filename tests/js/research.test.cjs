const assert = require("node:assert/strict");
const { test } = require("node:test");
const {
  researchIndex,
  tickerSelection,
  reportContext,
} = require("../../tradingagents/web/static/research.js");

const completed = {
  id: "a".repeat(32),
  analysis_date: "2026-10-12",
  created_at: "2026-10-12T18:00:00Z",
  status: "completed",
  tickers: ["PENG", "NVDA"],
  consolidated_reports: { html: "/batch-a" },
  results: [
    { ticker: "PENG", status: "completed", reports: { investor: "/peng-a" } },
    { ticker: "NVDA", status: "completed", reports: { investor: "/nvda-a" } },
  ],
};
const failed = {
  id: "b".repeat(32),
  analysis_date: "2026-10-13",
  created_at: "2026-10-13T18:00:00Z",
  status: "failed",
  tickers: ["PENG"],
  results: [{ ticker: "PENG", status: "failed", error: "Unavailable" }],
};

test("a failed newest attempt retains the previous successful report and its exact batch", () => {
  const rows = researchIndex([completed, failed]);
  const peng = rows.find((r) => r.ticker === "PENG");
  assert.equal(peng.latest.job.id, "b".repeat(32));
  assert.equal(peng.successful.job.id, "a".repeat(32));
  assert.deepEqual(reportContext(peng.successful).tickers, ["PENG", "NVDA"]);
  assert.equal(reportContext(peng.successful).consolidated, "/batch-a");
  assert.equal(reportContext(peng.latest).consolidated, null);
});
test("queued tickers appear before results exist and filtered history stays per ticker", () => {
  const queued = {
    id: "c".repeat(32),
    created_at: "2026-10-14",
    status: "queued",
    tickers: ["SPY"],
    results: [],
  };
  const rows = researchIndex([completed, failed, queued]);
  assert.equal(rows[0].ticker, "SPY");
  assert.equal(rows[0].latest.result.status, "queued");
  assert.equal(rows.find((r) => r.ticker === "NVDA").history.length, 1);
});
test("ticker selection preserves suffixes, deduplicates, and rejects path-shaped symbols", () => {
  assert.deepEqual(
    tickerSelection(" peng, PENG; SHOP.TO\nBRK-B ^GSPC BTC-USD "),
    { tickers: ["PENG", "SHOP.TO", "BRK-B", "^GSPC", "BTC-USD"], invalid: [] },
  );
  assert.deepEqual(tickerSelection("../secret, <img>, AAPL"), {
    tickers: ["AAPL"],
    invalid: ["../SECRET", "<IMG>"],
  });
});
