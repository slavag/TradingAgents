const state = {
  jobId: null,
  pollHandle: null,
  htmlBlobUrl: null,
  markdownBlobUrl: null,
};

const elements = {
  form: document.getElementById("analysis-form"),
  tickers: document.getElementById("tickers"),
  analysisDate: document.getElementById("analysis-date"),
  provider: document.getElementById("provider"),
  backendUrl: document.getElementById("backend-url"),
  quickThinker: document.getElementById("quick-thinker"),
  deepThinker: document.getElementById("deep-thinker"),
  researchDepth: document.getElementById("research-depth"),
  openaiEffortWrap: document.getElementById("openai-effort-wrap"),
  openaiEffort: document.getElementById("openai-effort"),
  googleThinkingWrap: document.getElementById("google-thinking-wrap"),
  googleThinking: document.getElementById("google-thinking"),
  saveReports: document.getElementById("save-reports"),
  exportPathWrap: document.getElementById("export-path-wrap"),
  exportPath: document.getElementById("export-path"),
  healthIndicator: document.getElementById("health-indicator"),
  jobIndicator: document.getElementById("job-indicator"),
  statusBadge: document.getElementById("status-badge"),
  currentTicker: document.getElementById("current-ticker"),
  completedCount: document.getElementById("completed-count"),
  progressMessage: document.getElementById("progress-message"),
  progressBody: document.getElementById("progress-body"),
  recentEvents: document.getElementById("recent-events"),
  currentReport: document.getElementById("current-report"),
  resultsList: document.getElementById("results-list"),
  speakingList: document.getElementById("speaking-list"),
  refreshSpeaking: document.getElementById("refresh-speaking"),
  speakingTopn: document.getElementById("speaking-topn"),
  speakingLookback: document.getElementById("speaking-lookback"),
  reportState: document.getElementById("report-state"),
  openHtmlReport: document.getElementById("open-html-report"),
  downloadMarkdownReport: document.getElementById("download-markdown-report"),
  defaultReportPath: document.getElementById("default-report-path"),
  customReportPath: document.getElementById("custom-report-path"),
  resetForm: document.getElementById("reset-form"),
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function setToday() {
  elements.analysisDate.value = new Date().toISOString().slice(0, 10);
}

function updateProviderFields() {
  const provider = elements.provider.value;
  const isOpenAI = provider === "openai";
  const isGoogle = provider === "google";

  elements.openaiEffortWrap.classList.toggle("hidden", !isOpenAI);
  elements.googleThinkingWrap.classList.toggle("hidden", !isGoogle);

  if (provider === "openai" && !elements.backendUrl.value.trim()) {
    elements.backendUrl.value = "https://api.openai.com/v1";
  }
}

function updateExportToggle() {
  elements.exportPathWrap.classList.toggle("hidden", !elements.saveReports.checked);
}

function setHealth(ok, message = null) {
  elements.healthIndicator.textContent = ok ? "Ready" : (message || "Offline");
}

async function fetchHealth() {
  try {
    const response = await fetch("/api/health");
    setHealth(response.ok);
  } catch {
    setHealth(false);
  }
}

function revokeReportUrls() {
  if (state.htmlBlobUrl) {
    URL.revokeObjectURL(state.htmlBlobUrl);
    state.htmlBlobUrl = null;
  }
  if (state.markdownBlobUrl) {
    URL.revokeObjectURL(state.markdownBlobUrl);
    state.markdownBlobUrl = null;
  }
}

function setReportButtons(job) {
  revokeReportUrls();

  const html = job.consolidated_html;
  const markdown = job.consolidated_markdown;
  const paths = job.consolidated_paths || {};
  const defaultPaths = paths.default || null;
  const customPaths = paths.custom || null;

  if (html) {
    state.htmlBlobUrl = URL.createObjectURL(new Blob([html], { type: "text/html" }));
  }
  if (markdown) {
    state.markdownBlobUrl = URL.createObjectURL(new Blob([markdown], { type: "text/markdown" }));
  }

  elements.openHtmlReport.disabled = !state.htmlBlobUrl;
  elements.downloadMarkdownReport.disabled = !state.markdownBlobUrl;

  if (job.status === "completed" && (state.htmlBlobUrl || state.markdownBlobUrl)) {
    elements.reportState.textContent = "Ready";
    elements.reportState.className = "chip done";
  } else if (job.status === "failed") {
    elements.reportState.textContent = "Failed";
    elements.reportState.className = "chip failed";
  } else {
    elements.reportState.textContent = "Waiting";
    elements.reportState.className = "chip neutral";
  }

  elements.defaultReportPath.textContent = defaultPaths
    ? (defaultPaths.html || defaultPaths.markdown || "Saved")
    : "No report yet.";
  elements.customReportPath.textContent = customPaths
    ? (customPaths.html || customPaths.markdown || "Saved")
    : "Not written yet.";
}

function speakingCard(item, rank) {
  const price = item.price != null ? `$${Number(item.price).toFixed(2)}` : "—";
  const ret5 = item.ret_5d_pct != null ? `${item.ret_5d_pct.toFixed(1)}%` : "—";
  return `
    <article class="speaking-card">
      <div class="speaking-topline">
        <div>
          <div class="ticker-mark">${escapeHtml(item.ticker)}</div>
          <p class="speaking-text">Rank #${rank}</p>
        </div>
        <div class="decision-pill hold">Score ${escapeHtml(item.score)}</div>
      </div>
      <div class="speaking-metrics">
        <div class="metric-box"><span>Price</span><strong>${price}</strong></div>
        <div class="metric-box"><span>5D</span><strong>${ret5}</strong></div>
        <div class="metric-box"><span>Trend</span><strong>${escapeHtml(item.trend_score)}/2</strong></div>
        <div class="metric-box"><span>Sector</span><strong>${escapeHtml(item.sector || "—")}</strong></div>
      </div>
      <p class="speaking-text">P/E: ${escapeHtml(item.pe_ratio || "—")} · Market Cap: ${escapeHtml(item.market_cap || "—")}</p>
    </article>
  `;
}

async function fetchSpeakingStocks() {
  elements.speakingList.className = "speaking-list empty-state";
  elements.speakingList.textContent = "Refreshing chatter feed…";

  const topN = Number(elements.speakingTopn.value || 10);
  const lookback = Number(elements.speakingLookback.value || 30);

  try {
    const response = await fetch(`/api/speaking-stocks?top_n=${topN}&lookback_days=${lookback}`);
    const payload = await response.json();
    const items = payload.items || [];

    if (!items.length) {
      elements.speakingList.className = "speaking-list empty-state";
      elements.speakingList.textContent = "No speaking stocks available right now.";
      return;
    }

    elements.speakingList.className = "speaking-list";
    elements.speakingList.innerHTML = items.map((item, index) => speakingCard(item, index + 1)).join("");
  } catch (error) {
    elements.speakingList.className = "speaking-list empty-state";
    elements.speakingList.textContent = `Unable to load speaking stocks: ${error.message}`;
  }
}

function decisionClass(decision) {
  if (!decision) return "failed";
  const normalized = String(decision).toLowerCase();
  if (normalized.includes("buy")) return "buy";
  if (normalized.includes("sell")) return "sell";
  return "hold";
}

function resultCard(result) {
  if (result.status === "failed") {
    return `
      <article class="result-card">
        <div class="result-topline">
          <div class="ticker-mark">${escapeHtml(result.ticker)}</div>
          <div class="decision-pill failed">Failed</div>
        </div>
        <p class="result-text">${escapeHtml(result.error || "Unknown error.")}</p>
      </article>
    `;
  }

  const highlights = result.highlights || {};
  return `
    <article class="result-card">
      <div class="result-topline">
        <div class="ticker-mark">${escapeHtml(result.ticker)}</div>
        <div class="decision-pill ${decisionClass(result.decision)}">${escapeHtml(result.decision || "Pending")}</div>
      </div>
      <div class="result-metrics">
        <div class="metric-box"><span>Target</span><strong>${escapeHtml(result.price_target_label || "—")}</strong></div>
        <div class="metric-box"><span>Confidence</span><strong>${escapeHtml(result.confidence_label || "—")}</strong></div>
      </div>
      <p class="result-text"><strong>Outlook:</strong> ${escapeHtml(result.target_summary || "—")}</p>
      <p class="result-text">${escapeHtml(result.executive_summary || "")}</p>
      <p class="result-text">
        <strong>Market:</strong> ${escapeHtml(highlights.market || "—")}<br />
        <strong>Social:</strong> ${escapeHtml(highlights.social || "—")}<br />
        <strong>News:</strong> ${escapeHtml(highlights.news || "—")}<br />
        <strong>Fundamentals:</strong> ${escapeHtml(highlights.fundamentals || "—")}
      </p>
    </article>
  `;
}

function renderResults(job) {
  const results = job.results || [];
  if (!results.length) {
    elements.resultsList.className = "results-list empty-state";
    elements.resultsList.textContent = "Ticker results will appear here once the run starts.";
    return;
  }

  elements.resultsList.className = "results-list";
  elements.resultsList.innerHTML = results.map(resultCard).join("");
}

function renderProgressRows(job) {
  const rows = job.progress_rows || [];
  if (!rows.length) {
    elements.progressBody.innerHTML = `
      <tr>
        <td colspan="3" class="placeholder-row">Progress table will update when the run starts.</td>
      </tr>
    `;
    return;
  }

  elements.progressBody.innerHTML = rows.map((row) => `
    <tr>
      <td>${escapeHtml(row.team)}</td>
      <td>${escapeHtml(row.agent)}</td>
      <td>
        <span class="status-cell">
          <span class="status-dot ${row.status}"></span>
          ${escapeHtml(row.status.replace("_", " "))}
        </span>
      </td>
    </tr>
  `).join("");
}

function renderEvents(job) {
  const events = job.recent_events || [];
  if (!events.length) {
    elements.recentEvents.className = "event-feed empty-state";
    elements.recentEvents.textContent = "Activity will appear here once agents start working.";
    return;
  }

  elements.recentEvents.className = "event-feed";
  elements.recentEvents.innerHTML = events.map((event) => `
    <article class="event-card">
      <div class="event-meta">
        <span>${escapeHtml(event.time)}</span>
        <span>${escapeHtml(event.type)}</span>
      </div>
      <p>${escapeHtml(event.content)}</p>
    </article>
  `).join("");
}

function statusChip(job) {
  if (job.status === "completed") return ["Completed", "chip done"];
  if (job.status === "failed") return ["Failed", "chip failed"];
  if (job.status === "running") return ["Running", "chip live"];
  if (job.status === "queued") return ["Queued", "chip live"];
  return ["Idle", "chip neutral"];
}

function updateJobStatus(job) {
  const [label, className] = statusChip(job);
  elements.jobIndicator.textContent = label;
  elements.statusBadge.textContent = label;
  elements.statusBadge.className = className;
  elements.currentTicker.textContent = job.current_ticker || "—";
  elements.completedCount.textContent = `${job.completed || 0} / ${job.total || 0}`;
  elements.progressMessage.textContent = job.progress_message || "Waiting for a run.";
  renderProgressRows(job);
  renderEvents(job);
  elements.currentReport.textContent = job.current_report || "Waiting for report output.";
  renderResults(job);
  setReportButtons(job);
}

async function pollJob(jobId) {
  try {
    const response = await fetch(`/api/jobs/${jobId}`);
    if (!response.ok) {
      throw new Error(`Status ${response.status}`);
    }

    const job = await response.json();
    updateJobStatus(job);

    if (job.status === "completed" || job.status === "failed") {
      clearInterval(state.pollHandle);
      state.pollHandle = null;
    }
  } catch (error) {
    elements.progressMessage.textContent = `Polling error: ${error.message}`;
  }
}

function collectPayload() {
  const analysts = [...document.querySelectorAll('input[name="analyst"]:checked')].map((input) => input.value);

  return {
    tickers: elements.tickers.value,
    analysis_date: elements.analysisDate.value,
    analysts,
    research_depth: Number(elements.researchDepth.value),
    llm_provider: elements.provider.value,
    backend_url: elements.backendUrl.value.trim() || null,
    quick_thinker: elements.quickThinker.value.trim(),
    deep_thinker: elements.deepThinker.value.trim(),
    openai_reasoning_effort: elements.provider.value === "openai"
      ? elements.openaiEffort.value
      : null,
    google_thinking_level: elements.provider.value === "google"
      ? (elements.googleThinking.value || null)
      : null,
    save_reports: elements.saveReports.checked,
    export_path: elements.saveReports.checked ? (elements.exportPath.value.trim() || null) : null,
  };
}

async function submitAnalysis(event) {
  event.preventDefault();
  const payload = collectPayload();
  revokeReportUrls();
  elements.defaultReportPath.textContent = "Preparing reports…";
  elements.customReportPath.textContent = payload.save_reports ? "Will write after completion." : "Custom export disabled.";

  try {
    const response = await fetch("/api/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const job = await response.json();
    if (!response.ok) {
      throw new Error(job.detail || "Unable to create job.");
    }

    state.jobId = job.id;
    updateJobStatus(job);

    if (state.pollHandle) {
      clearInterval(state.pollHandle);
    }
    state.pollHandle = setInterval(() => pollJob(state.jobId), 5000);
    pollJob(state.jobId);
  } catch (error) {
    elements.progressMessage.textContent = error.message;
    elements.statusBadge.textContent = "Failed";
    elements.statusBadge.className = "chip failed";
  }
}

function resetForm() {
  elements.form.reset();
  setToday();
  elements.provider.value = "openai";
  elements.quickThinker.value = "gpt-5.4";
  elements.deepThinker.value = "gpt-5.4";
  elements.researchDepth.value = "3";
  elements.saveReports.checked = true;
  updateProviderFields();
  updateExportToggle();
}

function openHtmlReport() {
  if (!state.htmlBlobUrl) return;
  window.open(state.htmlBlobUrl, "_blank", "noopener,noreferrer");
}

function downloadMarkdownReport() {
  if (!state.markdownBlobUrl) return;
  const anchor = document.createElement("a");
  anchor.href = state.markdownBlobUrl;
  anchor.download = `tradingagents-report-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}.md`;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
}

function boot() {
  setToday();
  updateProviderFields();
  updateExportToggle();
  fetchHealth();
  fetchSpeakingStocks();

  elements.provider.addEventListener("change", updateProviderFields);
  elements.saveReports.addEventListener("change", updateExportToggle);
  elements.refreshSpeaking.addEventListener("click", fetchSpeakingStocks);
  elements.form.addEventListener("submit", submitAnalysis);
  elements.resetForm.addEventListener("click", resetForm);
  elements.openHtmlReport.addEventListener("click", openHtmlReport);
  elements.downloadMarkdownReport.addEventListener("click", downloadMarkdownReport);
}

boot();
