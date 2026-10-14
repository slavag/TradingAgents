(function () {
  "use strict";
  const { researchIndex, tickerSelection, reportContext } =
    window.TradingAgentsResearch;
  const runtime = window.TradingAgentsRuntime;
  const el = (id) => document.getElementById(id);
  const escape = (value) =>
    String(value ?? "").replace(
      /[&<>"']/g,
      (c) =>
        ({
          "&": "&amp;",
          "<": "&lt;",
          ">": "&gt;",
          '"': "&quot;",
          "'": "&#39;",
        })[c],
    );
  const jobs = new Map();
  let rows = [],
    current = {},
    loading = false,
    loaded = false,
    discoverLoaded = false;
  let restoredDefaults = false;
  const titles = {
    research: "My Research",
    new: "New Analysis",
    runs: "Runs",
    discover: "Discover",
    settings: "Settings",
  };
  const dateLabel = (value) =>
    value ? String(value).slice(0, 10) : "Unknown date";
  const statusLabel = (value) =>
    ({
      completed: "Complete",
      failed: "Failed",
      running: "Running",
      queued: "Queued",
      interrupted: "Interrupted",
      partial: "Partial",
    })[value] || "Unavailable";
  const chip = (status) =>
    `<span class="chip ${escape(status)}">${escape(statusLabel(status))}</span>`;
  const tickerURL = (ticker, run, tab) =>
    `#research/${encodeURIComponent(ticker)}${run || tab ? `?${new URLSearchParams({ ...(run && { run }), ...(tab && { tab }) })}` : ""}`;
  const runURL = (id) => `#runs/${encodeURIComponent(id)}`;
  function link(url, text, style = "secondary-button") {
    // Report URLs are constructed by our local API; never render arbitrary schemes.
    return typeof url === "string" &&
      /^\/api\/jobs\/[a-f0-9]{32}\/reports\//.test(url)
      ? `<a class="${style}" href="${escape(url)}">${escape(text)}</a>`
      : "";
  }
  function reportRow(title, description, href, label, download) {
    return `<div class="report-row"><div><strong>${escape(title)}</strong><p>${escape(description)}</p></div><div class="report-actions">${href ? link(href, label || "Open") : '<span class="report-unavailable">Not available</span>'}${download ? link(download, "Markdown", "text-button") : ""}</div></div>`;
  }
  function reports(entry) {
    const context = reportContext(entry);
    return `<section class="reports-panel" id="selected-reports"><h2>Saved reports</h2>
      ${reportRow("Investor report", "Company research and final recommendation.", context.investor)}
      ${reportRow("Full research", "Complete analysis and supporting evidence.", context.full)}
      ${reportRow("Consolidated report", `Batch: ${context.tickers.join(", ")} · ${dateLabel(context.date)}`, context.consolidated, "Open consolidated report", context.markdown)}
      </section>`;
  }
  function showNotice(message, href, label) {
    el("workspace-notice").innerHTML =
      `${escape(message)}${href ? `<a href="${escape(href)}">${escape(label)}</a>` : ""}`;
    el("workspace-notice").classList.toggle("hidden", !message);
  }
  async function loadHistory() {
    if (loading) return;
    loading = true;
    el("reload-history").disabled = true;
    try {
      const collected = new Map();
      let offset = 0,
        more = true;
      while (more) {
        const response = await fetch(`/api/jobs?limit=100&offset=${offset}`, {
          headers: { Accept: "application/json" },
        });
        if (!response.ok)
          throw new Error(
            response.status === 404 || response.status === 405
              ? "The server needs a restart to load the new research API."
              : "Could not load saved research. Please retry.",
          );
        const data = await response.json();
        if (!Array.isArray(data.jobs))
          throw new Error("The research response was invalid. Please retry.");
        data.jobs.forEach((job) => collected.set(job.id, job));
        more = data.has_more && data.jobs.length > 0;
        offset += data.jobs.length;
      }
      jobs.clear();
      collected.forEach((job, id) => jobs.set(id, job));
      rows = researchIndex([...jobs.values()]);
      loaded = true;
      showNotice("");
      render();
    } catch (error) {
      showNotice(error.message);
      if (!jobs.size)
        el("research-list").innerHTML =
          '<p class="empty-state">Research could not be loaded. Use Refresh history to try again.</p>';
    } finally {
      loading = false;
      el("reload-history").disabled = false;
    }
  }
  function readRoute() {
    const [path, query = ""] = location.hash.slice(1).split("?");
    const [view = "research", key = ""] = path.split("/");
    let decoded = "";
    try {
      decoded = decodeURIComponent(key);
    } catch (_) {
      /* malformed links remain unselected */
    }
    const params = new URLSearchParams(query);
    return {
      view: titles[view] ? view : "research",
      key: decoded,
      run: params.get("run"),
      tab: params.get("tab") || "overview",
    };
  }
  function renderLibrary() {
    if (!loaded && !rows.length) {
      el("research-list").innerHTML =
        '<p class="empty-state">Loading saved research…</p>';
      return;
    }
    const query = el("research-search").value.trim().toLowerCase();
    const filter = el("research-filter").value;
    const list = rows.filter((row) => {
      const status = row.latest.result.status;
      return (
        (!query ||
          `${row.ticker} ${row.successful?.result.company_name || ""}`
            .toLowerCase()
            .includes(query)) &&
        (filter === "all" ||
          (filter === "active" && ["running", "queued"].includes(status)) ||
          (filter === "failed" && ["failed", "interrupted"].includes(status)) ||
          (filter === "completed" && status === "completed"))
      );
    });
    if (el("research-sort").value === "symbol")
      list.sort((a, b) => a.ticker.localeCompare(b.ticker));
    el("research-list").innerHTML =
      list
        .map((row) => {
          const selected = row.ticker === current.key;
          const report =
            row.successful?.result.reports?.investor ||
            row.successful?.result.reports?.full;
          const previous =
            row.successful && row.latest.job.id !== row.successful.job.id;
          return `<article class="research-row ${selected ? "selected" : ""}"><a class="ticker-select" href="${tickerURL(row.ticker)}" aria-current="${selected}"><strong>${escape(row.ticker)}</strong><time>${escape(dateLabel(row.latest.job.analysis_date))}</time></a><p class="row-company">${escape(row.latest.result.company_name || "Saved ticker analysis")}</p><div class="row-actions">${chip(row.latest.result.status)}${report ? link(report, previous ? "Previous report" : "Open report", "row-report-link") : `<a href="${runURL(row.latest.job.id)}">View run</a>`}</div></article>`;
        })
        .join("") ||
      `<div class="empty-state"><p>${rows.length ? "No tickers match these filters." : "No saved research yet."}</p><a href="#new">Start an analysis</a></div>`;
    el("research-count").textContent =
      `${list.length} of ${rows.length} analyzed tickers`;
  }
  function overview(entry) {
    const r = entry.result;
    const text =
      r.executive_summary ||
      r.target_summary ||
      (r.status === "completed"
        ? "Open the saved report to read the full recommendation and evidence."
        : r.error ||
          entry.job.progress_message ||
          "This ticker has no completed report yet.");
    const conditions = [
      ["Confirmation", r.conditional_confirmation],
      ["Alternative", r.conditional_alternative],
      ["Invalidation", r.conditional_invalidation],
    ].filter(([, value]) => value);
    return `<section class="overview-panel"><p class="caption">Recommendation</p><h2 class="recommendation">${escape(r.decision || statusLabel(r.status))}</h2><div class="position-grid"><div><span class="subtle">Existing position</span><h3>${escape(r.existing_position_action || "See full research")}</h3></div><div><span class="subtle">New position</span><h3>${escape(r.new_position_action || "See full research")}</h3></div></div><p class="overview-summary">${escape(text)}</p>
      ${
        r.existing_position_summary ||
        r.new_position_summary ||
        conditions.length
          ? `<details><summary>Position guidance and conditions</summary><div class="condition-list">${[
              ["Existing position", r.existing_position_summary],
              ["New position", r.new_position_summary],
              ...conditions,
            ]
              .filter(([, v]) => v)
              .map(
                ([label, value]) =>
                  `<div><strong>${escape(label)}</strong><p>${escape(value)}</p></div>`,
              )
              .join("")}</div></details>`
          : ""
      }
      ${r.price_target_label && !["—", "-"].includes(r.price_target_label) ? `<details><summary>Target and confidence</summary><div class="target-grid"><div><span class="caption">Target / horizon</span><strong>${escape(r.price_target_label)}${r.target_horizon ? ` · ${escape(r.target_horizon)}` : ""}</strong></div><div><span class="caption">Recommendation confidence</span><strong>${escape(r.recommendation_confidence_label || (r.recommendation_confidence_score != null ? `${r.recommendation_confidence_score}/100 (uncalibrated)` : "Unavailable"))}</strong></div></div><p>${escape(r.target_summary || "")}</p></details>` : ""}</section>`;
  }
  function renderDetail() {
    if (!loaded && !rows.length) {
      el("research-detail").innerHTML =
        '<p class="empty-state">Loading saved research…</p>';
      return;
    }
    const row = rows.find((r) => r.ticker === current.key);
    if (!row) {
      el("research-detail").innerHTML =
        `<div class="empty-detail"><h2>${current.key ? "Ticker not found" : "Your research starts here"}</h2><p>${rows.length ? "Select a ticker to see its recommendations, reports and history." : "Run your first analysis to build your research library."}</p><a class="primary-button" href="#new">New analysis</a></div>`;
      return;
    }
    const entry = current.run
      ? row.history.find((item) => item.job.id === current.run)
      : row.successful || row.latest;
    if (!entry) {
      el("research-detail").innerHTML =
        '<p class="empty-state">This report version was not found. <a href="#research">Back to research</a></p>';
      return;
    }
    const context = reportContext(entry);
    const tab = ["overview", "reports", "history"].includes(current.tab)
      ? current.tab
      : "overview";
    const previous = entry.job.id !== row.latest.job.id;
    el("research-detail").innerHTML =
      `<a class="mobile-back text-button" href="#research">Back to all tickers</a><header class="detail-header"><div><h1>${escape(row.ticker)}</h1><p class="company">${escape(entry.result.company_name || "Saved ticker analysis")}</p></div><div class="analysis-date">Analysis date<strong>${escape(dateLabel(entry.job.analysis_date))}</strong></div></header>
      <div class="detail-actions">${link(context.investor, "Open investor report", "primary-button")}${link(context.full, "Full research")}${link(context.consolidated, "Consolidated report")}<a class="secondary-button" href="${tickerURL(row.ticker, entry.job.id, "history")}">History</a></div>
      <div class="detail-tabs" role="tablist" aria-label="Ticker details">${["overview", "reports", "history"].map((name) => `<button id="tab-${name}" role="tab" aria-selected="${tab === name}" aria-controls="ticker-tab-panel" tabindex="${tab === name ? 0 : -1}" data-detail-tab="${name}">${name[0].toUpperCase() + name.slice(1)}</button>`).join("")}</div>
      ${previous ? `<div class="notice">Latest attempt: ${escape(statusLabel(row.latest.result.status))} · ${escape(dateLabel(row.latest.job.analysis_date))}. Showing saved research from ${escape(dateLabel(entry.job.analysis_date))}. <a href="${runURL(row.latest.job.id)}">View latest run</a></div>` : ""}
      <div id="ticker-tab-panel" role="tabpanel" aria-labelledby="tab-${tab}">${tab === "overview" ? overview(entry) + reports(entry) : tab === "reports" ? reports(entry) : row.history.map((item) => `<article class="history-entry"><header><a href="${tickerURL(row.ticker, item.job.id)}"><strong>${escape(dateLabel(item.job.analysis_date))}</strong></a>${chip(item.result.status)}</header><p class="subtle">Created ${escape(item.job.report_timestamp || item.job.created_at || "Unknown time")} · Batch: ${escape(item.job.tickers.join(", "))}</p><div class="report-actions">${link(item.result.reports?.investor, "Investor report")}${link(item.result.reports?.full, "Full research")}${link(item.job.consolidated_reports?.html, "Consolidated report")}</div></article>`).join("")}</div>
      <footer class="detail-footer"><span>Report version ${escape(entry.job.report_timestamp || entry.job.created_at || "Unknown")}</span><button class="text-button" type="button" data-analyze-ticker="${escape(row.ticker)}">Analyze again</button></footer>`;
  }
  function renderRuns() {
    const ordered = [...jobs.values()].sort((a, b) =>
      String(b.created_at || "").localeCompare(String(a.created_at || "")),
    );
    const job = jobs.get(current.key) || (!current.key ? ordered[0] : null);
    el("runs-list").innerHTML =
      ordered
        .map(
          (item) =>
            `<article class="run-entry ${job?.id === item.id ? "selected" : ""}"><a class="run-select" href="${runURL(item.id)}" aria-current="${job?.id === item.id}"><strong>${escape(item.tickers.join(", "))}</strong><time>${escape(dateLabel(item.analysis_date))}</time></a>${chip(item.status)}${link(item.consolidated_reports?.html, "Open consolidated report", "text-button")}</article>`,
        )
        .join("") || '<p class="empty-state">No runs yet.</p>';
    el("run-runtime").classList.toggle("hidden", !job);
    if (!job) {
      el("run-heading").innerHTML =
        '<p class="empty-state">Select a batch or start a new analysis.</p>';
      return;
    }
    const failed = (job.results || []).filter(
      (r) => r.status === "failed",
    ).length;
    const succeeded = (job.results || []).filter(
      (r) => r.status === "completed",
    ).length;
    el("run-heading").innerHTML =
      `<header class="run-heading"><h2>${escape(job.tickers.join(", "))}</h2><p>Analysis ${escape(dateLabel(job.analysis_date))} · ${succeeded} succeeded · ${failed} failed</p><div class="report-actions">${link(job.consolidated_reports?.html, "Open consolidated report", "primary-button")}${link(job.consolidated_reports?.md, "Download Markdown")}${!job.consolidated_reports?.html ? '<span class="subtle">Consolidated report is not available yet.</span>' : ""}</div></header>`;
    runtime.renderJob(job);
    el("run-ticker-statuses").innerHTML = job.tickers
      .map((ticker) => {
        const result = (job.results || []).find((r) => r.ticker === ticker);
        const status =
          result?.status ||
          (["running", "queued"].includes(job.status)
            ? job.current_ticker === ticker
              ? "running"
              : "queued"
            : job.status);
        return `<div class="run-ticker-status"><a href="${tickerURL(ticker, job.id)}"><strong>${escape(ticker)}</strong></a>${chip(status)}${link(result?.reports?.investor, "Open report", "text-button")}${result?.error ? `<span class="subtle">${escape(result.error)}</span>` : ""}</div>`;
      })
      .join("");
  }
  function render() {
    current = readRoute();
    if (
      current.view === "research" &&
      !current.key &&
      rows.length &&
      !window.matchMedia("(max-width:650px)").matches
    ) {
      history.replaceState(null, "", tickerURL(rows[0].ticker));
      current = readRoute();
    }
    document
      .querySelectorAll(".workspace-view")
      .forEach((view) =>
        view.classList.toggle("hidden", view.id !== `view-${current.view}`),
      );
    el("view-research").classList.toggle(
      "mobile-detail-selected",
      Boolean(current.key),
    );
    document.querySelectorAll("[data-view]").forEach((item) => {
      if (item.dataset.view === current.view)
        item.setAttribute("aria-current", "page");
      else item.removeAttribute("aria-current");
    });
    document.title = `TradingAgents · ${titles[current.view]}${current.key ? ` · ${current.key}` : ""}`;
    const active = [...jobs.values()].filter((j) =>
      ["running", "queued"].includes(j.status),
    );
    el("active-run-count").textContent = active.length || "";
    if (current.view === "research") {
      renderLibrary();
      renderDetail();
    }
    if (current.view === "runs") renderRuns();
    if (current.view === "discover" && !discoverLoaded) {
      discoverLoaded = true;
      runtime.loadDiscovery();
    }
    updateSelection();
    updateModelSummary();
  }
  function updateSelection() {
    const selection = tickerSelection(el("tickers").value);
    el("ticker-chips").innerHTML = selection.tickers
      .map(
        (ticker) =>
          `<span class="ticker-chip">${escape(ticker)}<button type="button" data-remove-ticker="${escape(ticker)}" aria-label="Remove ${escape(ticker)}">Remove</button></span>`,
      )
      .join("");
    el("ticker-selection-status").textContent = selection.invalid.length
      ? `Invalid ticker syntax: ${selection.invalid.join(", ")}`
      : selection.tickers.length
        ? `${selection.tickers.length} unique tickers selected. Instrument identity is verified during analysis.`
        : "Enter or paste tickers separated by commas or spaces.";
    el("tickers").setCustomValidity(
      selection.invalid.length
        ? "Remove invalid ticker symbols before starting."
        : "",
    );
    if (!runtime.isSubmitting())
      el("start-analysis").textContent = selection.tickers.length
        ? `Analyze ${selection.tickers.length} ${selection.tickers.length === 1 ? "ticker" : "tickers"}`
        : "Analyze tickers";
  }
  function updateModelSummary() {
    const summary = runtime.modelSummary();
    el("analysis-model-summary").textContent = summary;
    el("settings-model-summary").textContent = summary;
  }
  function configureModels() {
    location.hash = "#new";
    el("analysis-advanced").open = true;
    el("analysis-advanced").scrollIntoView({
      block: "start",
      behavior: "smooth",
    });
  }
  function saveDefaults() {
    try {
      const defaults = runtime.defaults();
      localStorage.setItem(
        "tradingagents.analysis-defaults.v1",
        JSON.stringify(defaults),
      );
      el("defaults-status").textContent =
        "Analysis defaults saved in this browser.";
    } catch (_) {
      el("defaults-status").textContent = "Could not save browser defaults.";
    }
  }
  function restoreDefaults() {
    if (restoredDefaults) return;
    restoredDefaults = true;
    try {
      const value = JSON.parse(
        localStorage.getItem("tradingagents.analysis-defaults.v1") || "null",
      );
      if (value && typeof value === "object") runtime.restoreDefaults(value);
    } catch (_) {
      /* Browser storage can be disabled or contain an old format. */
    }
  }
  document.addEventListener("click", (event) => {
    const trigger = event.target.closest(
      "[data-detail-tab],[data-analyze-ticker],[data-remove-ticker]",
    );
    if (!trigger) return;
    if (trigger.dataset.detailTab)
      location.hash = tickerURL(
        current.key,
        current.run,
        trigger.dataset.detailTab,
      );
    if (trigger.dataset.analyzeTicker) {
      el("tickers").value = trigger.dataset.analyzeTicker;
      location.hash = "#new";
      updateSelection();
    }
    if (trigger.dataset.removeTicker) {
      el("tickers").value = tickerSelection(el("tickers").value)
        .tickers.filter((t) => t !== trigger.dataset.removeTicker)
        .join(", ");
      updateSelection();
    }
  });
  document.addEventListener("keydown", (event) => {
    const tab = event.target.closest("[data-detail-tab]");
    if (tab && ["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) {
      event.preventDefault();
      const tabs = ["overview", "reports", "history"],
        index = tabs.indexOf(tab.dataset.detailTab);
      const next =
        event.key === "Home"
          ? 0
          : event.key === "End"
            ? 2
            : (index + (event.key === "ArrowRight" ? 1 : 2)) % 3;
      location.hash = tickerURL(current.key, current.run, tabs[next]);
      setTimeout(() => el(`tab-${tabs[next]}`)?.focus(), 0);
    }
  });
  ["research-search", "research-sort", "research-filter"].forEach((id) =>
    el(id).addEventListener(
      id === "research-search" ? "input" : "change",
      renderLibrary,
    ),
  );
  el("reload-history").addEventListener("click", loadHistory);
  el("tickers").addEventListener("input", updateSelection);
  el("analysis-form").addEventListener("change", updateModelSummary);
  el("reset-form").addEventListener("click", () => {
    updateSelection();
    updateModelSummary();
  });
  el("configure-models").addEventListener("click", configureModels);
  el("edit-model-defaults").addEventListener("click", configureModels);
  el("save-analysis-defaults").addEventListener("click", saveDefaults);
  window.addEventListener("hashchange", () => {
    render();
    window.scrollTo({ top: 0 });
  });
  window.addEventListener("tradingagents:job", (event) => {
    const job = event.detail;
    jobs.set(job.id, job);
    rows = researchIndex([...jobs.values()]);
    render();
  });
  window.addEventListener("tradingagents:started", (event) => {
    location.hash = runURL(event.detail.id);
  });
  window.addEventListener("tradingagents:ticker-added", () => {
    location.hash = "#new";
    updateSelection();
  });
  window.addEventListener("resize", render);
  restoreDefaults();
  render();
  loadHistory();
  const refresh = setInterval(() => {
    if (
      !document.hidden &&
      [...jobs.values()].some((j) => ["running", "queued"].includes(j.status))
    )
      loadHistory();
  }, 5000);
  window.addEventListener("beforeunload", () => clearInterval(refresh));
})();
