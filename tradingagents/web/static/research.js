(function () {
  "use strict";

  function tickerSelection(value) {
    const tickers = [],
      invalid = [];
    for (const ticker of String(value)
      .toUpperCase()
      .split(/[\s,;]+/)
      .filter(Boolean)) {
      if (!/^[A-Z0-9^][A-Z0-9._^=+\-]{0,63}$/.test(ticker))
        invalid.push(ticker);
      else if (!tickers.includes(ticker)) tickers.push(ticker);
    }
    return { tickers, invalid };
  }

  function researchIndex(jobs) {
    const rows = new Map();
    const ordered = [...jobs].sort((a, b) =>
      String(b.created_at || b.analysis_date || "").localeCompare(
        String(a.created_at || a.analysis_date || ""),
      ),
    );
    for (const job of ordered) {
      for (const ticker of job.tickers || []) {
        const result = (job.results || []).find((r) => r.ticker === ticker) || {
          ticker,
          status:
            job.status === "running" && job.current_ticker !== ticker
              ? "queued"
              : job.status,
        };
        if (!rows.has(ticker))
          rows.set(ticker, {
            ticker,
            history: [],
            latest: null,
            successful: null,
          });
        const row = rows.get(ticker);
        const entry = { job, result };
        row.history.push(entry);
        row.latest ||= entry;
        if (
          !row.successful &&
          result.status === "completed" &&
          Object.keys(result.reports || {}).length
        ) {
          row.successful = entry;
        }
      }
    }
    return [...rows.values()];
  }

  function reportContext(entry) {
    return {
      tickers: entry.job.tickers || [],
      date: entry.job.analysis_date,
      investor: entry.result.reports?.investor || null,
      full: entry.result.reports?.full || null,
      consolidated: entry.job.consolidated_reports?.html || null,
      markdown: entry.job.consolidated_reports?.md || null,
    };
  }

  const api = { tickerSelection, researchIndex, reportContext };
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  if (typeof window !== "undefined") window.TradingAgentsResearch = api;
})();
