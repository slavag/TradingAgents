const state = {
  jobId: null,
  pollHandle: null,
  marketRefreshHandle: null,
  speakingRefreshHandle: null,
  htmlBlobUrl: null,
  markdownBlobUrl: null,
  marketItems: [],
  speakingItems: [],
  activeTickerItem: null,
};

const MARKET_TAPE_REFRESH_MS = 10 * 60 * 1000;
const SPEAKING_TAPE_REFRESH_MS = 30 * 60 * 1000;

const MODEL_OPTIONS = window.TRADINGAGENTS_MODEL_OPTIONS || {};

const elements = {
  form: document.getElementById("analysis-form"),
  tickers: document.getElementById("tickers"),
  analysisDate: document.getElementById("analysis-date"),
  quickProvider: document.getElementById("quick-provider"),
  quickThinker: document.getElementById("quick-thinker"),
  deepProvider: document.getElementById("deep-provider"),
  deepThinker: document.getElementById("deep-thinker"),
  finalReportProvider: document.getElementById("final-report-provider"),
  finalReportModel: document.getElementById("final-report-model"),
  researchDepth: document.getElementById("research-depth"),
  openaiEffortWrap: document.getElementById("openai-effort-wrap"),
  openaiEffort: document.getElementById("openai-effort"),
  googleThinkingWrap: document.getElementById("google-thinking-wrap"),
  googleThinking: document.getElementById("google-thinking"),
  saveReports: document.getElementById("save-reports"),
  exportPathWrap: document.getElementById("export-path-wrap"),
  exportPath: document.getElementById("export-path"),
  headerMarketTrack: document.getElementById("header-market-track"),
  headerMarketStatus: document.getElementById("header-market-status"),
  statusBadge: document.getElementById("status-badge"),
  currentTicker: document.getElementById("current-ticker"),
  completedCount: document.getElementById("completed-count"),
  progressMessage: document.getElementById("progress-message"),
  progressBody: document.getElementById("progress-body"),
  recentEvents: document.getElementById("recent-events"),
  currentReport: document.getElementById("current-report"),
  resultsList: document.getElementById("results-list"),
  tickerTrack: document.getElementById("ticker-track"),
  tickerStatus: document.getElementById("ticker-status"),
  tickerModal: document.getElementById("ticker-modal"),
  closeTickerModal: document.getElementById("close-ticker-modal"),
  addTickerToAnalysis: document.getElementById("add-ticker-to-analysis"),
  modalTitle: document.getElementById("ticker-modal-title"),
  modalScore: document.getElementById("modal-score"),
  modalPrice: document.getElementById("modal-price"),
  modalRet5: document.getElementById("modal-ret5"),
  modalTrend: document.getElementById("modal-trend"),
  modalDescription: document.getElementById("modal-description"),
  modalLookback: document.getElementById("modal-lookback"),
  modalZret5: document.getElementById("modal-zret5"),
  modalCompanyName: document.getElementById("modal-company-name"),
  modalSector: document.getElementById("modal-sector"),
  modalIndustry: document.getElementById("modal-industry"),
  modalMarketCap: document.getElementById("modal-market-cap"),
  modalCurrentPrice: document.getElementById("modal-current-price"),
  modal52wHigh: document.getElementById("modal-52w-high"),
  modal52wLow: document.getElementById("modal-52w-low"),
  modalVolume: document.getElementById("modal-volume"),
  modalEmployees: document.getElementById("modal-employees"),
  modalWebsite: document.getElementById("modal-website"),
  modalPe: document.getElementById("modal-pe"),
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

function populateModelSelect(selectElement, options, preferredValue = null) {
  const previousValue = preferredValue || selectElement.value;
  if (!options.length) {
    selectElement.innerHTML = "";
    return;
  }
  selectElement.innerHTML = options
    .map(([label, value]) => `<option value="${escapeHtml(value)}">${escapeHtml(label)}</option>`)
    .join("");

  const hasPreferred = options.some(([, value]) => value === previousValue);
  selectElement.value = hasPreferred ? previousValue : options[0][1];
}

function updateModelOptions(provider, selectElement) {
  const options = MODEL_OPTIONS[provider] || MODEL_OPTIONS.openai || [];
  populateModelSelect(selectElement, options);
}

function updateProviderFields() {
  const providers = [
    elements.quickProvider.value,
    elements.deepProvider.value,
    elements.finalReportProvider.value,
  ];
  const isOpenAI = providers.includes("openai");
  const isGoogle = providers.includes("google");

  elements.openaiEffortWrap.classList.toggle("hidden", !isOpenAI);
  elements.googleThinkingWrap.classList.toggle("hidden", !isGoogle);

  updateModelOptions(elements.quickProvider.value, elements.quickThinker);
  updateModelOptions(elements.deepProvider.value, elements.deepThinker);
  updateModelOptions(elements.finalReportProvider.value, elements.finalReportModel);
}

function updateExportToggle() {
  elements.exportPathWrap.classList.toggle("hidden", !elements.saveReports.checked);
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

function tickerItem(item) {
  const score = item.score != null ? item.score.toFixed(2) : "—";
  const ret5 = item.ret_5d_pct != null ? `${item.ret_5d_pct.toFixed(1)}%` : "—";
  const trend = item.trend_score != null ? `trend ${item.trend_score}/2` : "trend —";
  return `
    <article class="ticker-item clickable" data-ticker-symbol="${escapeHtml(item.ticker)}" role="button" tabindex="0">
      <div class="ticker-row">
        <span class="ticker-symbol">${escapeHtml(item.ticker)}</span>
        <span class="ticker-metric daily">score ${escapeHtml(score)}</span>
      </div>
      <div class="ticker-row ticker-row-sub">
        <span class="ticker-metric">5d ${ret5}</span>
        <span class="ticker-metric">${escapeHtml(trend)}</span>
      </div>
    </article>
  `;
}

function marketTickerItem(item) {
  const change = item.ret_1d_pct != null
    ? `${item.ret_1d_pct > 0 ? "+" : ""}${item.ret_1d_pct.toFixed(1)}%`
    : "—";
  const changeClass = item.ret_1d_pct > 0 ? "positive" : (item.ret_1d_pct < 0 ? "negative" : "flat");

  return `
    <article class="ticker-item market-ticker-item">
      <span class="ticker-symbol">${escapeHtml(item.ticker)}</span>
      <span class="market-ticker-change ${changeClass}">${escapeHtml(change)}</span>
    </article>
  `;
}

function tickerGroup(items, renderer = tickerItem) {
  return `<div class="ticker-group">${items.map(renderer).join("")}</div>`;
}

function fillTickerSlots(items, targetCount) {
  if (!items.length || targetCount <= 0) return [];
  const filled = [];
  for (let index = 0; index < targetCount; index += 1) {
    filled.push(items[index % items.length]);
  }
  return filled;
}

function renderTape(trackElement, items, renderer, targetCount, emptyMessage) {
  if (!items.length) {
    trackElement.innerHTML = `<div class="ticker-item placeholder">${escapeHtml(emptyMessage)}</div>`;
    return [];
  }

  const visibleItems = fillTickerSlots(items, targetCount);
  trackElement.style.setProperty("--visible-tickers", String(targetCount));
  trackElement.innerHTML = tickerGroup(visibleItems, renderer) + tickerGroup(visibleItems, renderer);
  return visibleItems;
}

async function fetchMarketTickers() {
  elements.headerMarketStatus.textContent = "Refreshing index tape…";
  elements.headerMarketTrack.innerHTML = '<div class="ticker-item market-ticker-item placeholder">Refreshing index tape…</div>';
  state.marketItems = [];

  try {
    const response = await fetch("/api/market-tickers?limit=12");
    const payload = await response.json();
    const items = payload.items || [];

    if (!items.length) {
      elements.headerMarketStatus.textContent = "No market indexes available right now.";
      elements.headerMarketTrack.innerHTML = '<div class="ticker-item market-ticker-item placeholder">No market indexes available right now.</div>';
      return;
    }

    state.marketItems = items;
    const visibleItems = renderTape(
      elements.headerMarketTrack,
      items,
      marketTickerItem,
      Math.max(8, Math.min(12, items.length)),
      "No market indexes available right now.",
    );
    elements.headerMarketStatus.textContent = `Tracking ${items.length} benchmark indexes`;
  } catch (error) {
    elements.headerMarketStatus.textContent = `Unable to load index tape: ${error.message}`;
    elements.headerMarketTrack.innerHTML = `<div class="ticker-item market-ticker-item placeholder">${escapeHtml(error.message)}</div>`;
  }
}

async function fetchSpeakingStocks() {
  elements.tickerStatus.textContent = "Refreshing chatter tape…";
  elements.tickerTrack.innerHTML = '<div class="ticker-item placeholder">Refreshing chatter feed…</div>';
  state.speakingItems = [];

  const topN = Number(elements.speakingTopn.value || 10);
  const lookback = Number(elements.speakingLookback.value || 30);

  try {
    const response = await fetch(`/api/speaking-stocks?top_n=${topN}&lookback_days=${lookback}`);
    const payload = await response.json();
    const items = payload.items || [];

    if (!items.length) {
      elements.tickerStatus.textContent = "No speaking stocks available right now.";
      elements.tickerTrack.innerHTML = '<div class="ticker-item placeholder">No speaking stocks available right now.</div>';
      return;
    }

    state.speakingItems = items;
    const visibleItems = renderTape(
      elements.tickerTrack,
      items,
      tickerItem,
      topN,
      "No speaking stocks available right now.",
    );
    elements.tickerStatus.textContent = `Showing ${visibleItems.length} slots from ${items.length} unique speaking stocks`;
  } catch (error) {
    elements.tickerStatus.textContent = `Unable to load chatter feed: ${error.message}`;
    elements.tickerTrack.innerHTML = `<div class="ticker-item placeholder">${escapeHtml(error.message)}</div>`;
  }
}

function appendTickerToTextarea(ticker) {
  const current = elements.tickers.value
    .split(/[\s,]+/)
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);
  if (!current.includes(ticker)) {
    current.push(ticker);
    elements.tickers.value = current.join(", ");
  }
}

function openTickerModal(item) {
  if (!item) return;
  state.activeTickerItem = item;
  elements.modalTitle.textContent = item.ticker;
  elements.modalCompanyName.textContent = item.ticker;
  elements.modalScore.textContent = String(item.score ?? "—");
  elements.modalPrice.textContent = item.price != null ? `$${Number(item.price).toFixed(2)}` : "—";
  elements.modalRet5.textContent = item.ret_5d_pct != null ? `${item.ret_5d_pct.toFixed(1)}%` : "—";
  elements.modalTrend.textContent = item.trend_score != null ? `${item.trend_score}/2` : "—";
  elements.modalLookback.textContent = item.lookback_days != null ? `${item.lookback_days} trading days` : "—";
  elements.modalZret5.textContent = item.z_ret5 != null ? String(item.z_ret5) : "—";
  elements.modalSector.textContent = "Loading…";
  elements.modalIndustry.textContent = "Loading…";
  elements.modalMarketCap.textContent = "Loading…";
  elements.modalCurrentPrice.textContent = "Loading…";
  elements.modal52wHigh.textContent = "Loading…";
  elements.modal52wLow.textContent = "Loading…";
  elements.modalVolume.textContent = "Loading…";
  elements.modalEmployees.textContent = "Loading…";
  elements.modalWebsite.textContent = "Loading…";
  elements.modalPe.textContent = "Loading…";
  const sources = Array.isArray(item.sources) && item.sources.length
    ? item.sources.join(", ")
    : "the chatter feed";
  elements.modalDescription.textContent =
    `${item.ticker} is currently on the speaking-stocks tape because it ranks inside the ` +
    `ApeWisdom ∩ StockTwits universe and is rescored using the MyAgent momentum/trend model from ${sources}.`;

  elements.tickerModal.classList.remove("hidden");
  elements.tickerModal.setAttribute("aria-hidden", "false");
  hydrateTickerModal(item.ticker);
}

function formatLargeNumber(value) {
  if (value == null || value === "") return "—";
  const num = Number(value);
  if (Number.isNaN(num)) return String(value);
  if (Math.abs(num) >= 1_000_000_000_000) return `${(num / 1_000_000_000_000).toFixed(2)}T`;
  if (Math.abs(num) >= 1_000_000_000) return `${(num / 1_000_000_000).toFixed(2)}B`;
  if (Math.abs(num) >= 1_000_000) return `${(num / 1_000_000).toFixed(2)}M`;
  if (Math.abs(num) >= 1_000) return `${(num / 1_000).toFixed(1)}K`;
  return num.toLocaleString();
}

async function hydrateTickerModal(symbol) {
  try {
    const response = await fetch(`/api/speaking-stocks/${encodeURIComponent(symbol)}`);
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail || "Unable to load company snapshot.");
    }

    if (!state.activeTickerItem || state.activeTickerItem.ticker !== symbol) {
      return;
    }

    elements.modalCompanyName.textContent = payload.company_name || symbol;
    elements.modalSector.textContent = payload.sector || "—";
    elements.modalIndustry.textContent = payload.industry || "—";
    elements.modalMarketCap.textContent = formatLargeNumber(payload.market_cap);
    elements.modalCurrentPrice.textContent =
      payload.current_price != null ? `$${Number(payload.current_price).toFixed(2)}` : "—";
    elements.modal52wHigh.textContent =
      payload.fifty_two_week_high != null ? `$${Number(payload.fifty_two_week_high).toFixed(2)}` : "—";
    elements.modal52wLow.textContent =
      payload.fifty_two_week_low != null ? `$${Number(payload.fifty_two_week_low).toFixed(2)}` : "—";
    elements.modalVolume.textContent = formatLargeNumber(payload.average_volume);
    elements.modalEmployees.textContent = formatLargeNumber(payload.employees);
    elements.modalWebsite.textContent = payload.website || "—";
    elements.modalPe.textContent = payload.pe_ratio != null ? String(payload.pe_ratio) : "—";
    elements.modalDescription.textContent = payload.summary || elements.modalDescription.textContent;
  } catch (error) {
    if (!state.activeTickerItem || state.activeTickerItem.ticker !== symbol) {
      return;
    }
    elements.modalDescription.textContent = `Unable to load company snapshot: ${error.message}`;
    elements.modalSector.textContent = "—";
    elements.modalIndustry.textContent = "—";
    elements.modalMarketCap.textContent = "—";
    elements.modalCurrentPrice.textContent = "—";
    elements.modal52wHigh.textContent = "—";
    elements.modal52wLow.textContent = "—";
    elements.modalVolume.textContent = "—";
    elements.modalEmployees.textContent = "—";
    elements.modalWebsite.textContent = "—";
    elements.modalPe.textContent = "—";
  }
}

function closeTickerModal() {
  state.activeTickerItem = null;
  elements.tickerModal.classList.add("hidden");
  elements.tickerModal.setAttribute("aria-hidden", "true");
}

function handleTickerTrackClick(event) {
  const trigger = event.target.closest("[data-ticker-symbol]");
  if (!trigger) return;
  const symbol = trigger.getAttribute("data-ticker-symbol");
  const item = state.speakingItems.find((entry) => entry.ticker === symbol);
  openTickerModal(item);
}

function addActiveTickerToAnalysis() {
  if (!state.activeTickerItem) return;
  appendTickerToTextarea(state.activeTickerItem.ticker);
  closeTickerModal();
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
      <p class="result-text">${escapeHtml(result.executiveSummary || result.executive_summary || "")}</p>
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
    llm_provider: elements.quickProvider.value,
    quick_provider: elements.quickProvider.value,
    quick_thinker: elements.quickThinker.value.trim(),
    deep_provider: elements.deepProvider.value,
    deep_thinker: elements.deepThinker.value.trim(),
    final_report_provider: elements.finalReportProvider.value,
    final_report_model: elements.finalReportModel.value.trim(),
    openai_reasoning_effort: [elements.quickProvider.value, elements.deepProvider.value, elements.finalReportProvider.value].includes("openai")
      ? elements.openaiEffort.value
      : null,
    google_thinking_level: [elements.quickProvider.value, elements.deepProvider.value, elements.finalReportProvider.value].includes("google")
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
  elements.quickProvider.value = "openai";
  elements.deepProvider.value = "openai";
  elements.finalReportProvider.value = "openai";
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
  fetchMarketTickers();
  fetchSpeakingStocks();
  state.marketRefreshHandle = setInterval(fetchMarketTickers, MARKET_TAPE_REFRESH_MS);
  state.speakingRefreshHandle = setInterval(fetchSpeakingStocks, SPEAKING_TAPE_REFRESH_MS);

  document.querySelectorAll(".sidebar-toggle").forEach((toggle) => {
    const section = toggle.parentElement;
    const isExpanded = section.classList.contains("expanded");
    section.classList.toggle("collapsed", !isExpanded);
    toggle.setAttribute("aria-expanded", String(isExpanded));
    toggle.addEventListener("click", (event) => {
      event.preventDefault();
      const nextExpanded = section.classList.contains("collapsed");
      section.classList.toggle("collapsed", !nextExpanded);
      section.classList.toggle("expanded", nextExpanded);
      toggle.setAttribute("aria-expanded", String(nextExpanded));
    });
  });

  elements.quickProvider.addEventListener("change", updateProviderFields);
  elements.deepProvider.addEventListener("change", updateProviderFields);
  elements.finalReportProvider.addEventListener("change", updateProviderFields);
  elements.saveReports.addEventListener("change", updateExportToggle);
  elements.refreshSpeaking.addEventListener("click", fetchSpeakingStocks);
  elements.tickerTrack.addEventListener("click", handleTickerTrackClick);
  elements.form.addEventListener("submit", submitAnalysis);
  elements.resetForm.addEventListener("click", resetForm);
  elements.openHtmlReport.addEventListener("click", openHtmlReport);
  elements.downloadMarkdownReport.addEventListener("click", downloadMarkdownReport);
  elements.closeTickerModal.addEventListener("click", closeTickerModal);
  elements.addTickerToAnalysis.addEventListener("click", addActiveTickerToAnalysis);
  elements.tickerModal.addEventListener("click", (event) => {
    if (event.target instanceof HTMLElement && event.target.dataset.closeModal === "true") {
      closeTickerModal();
    }
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !elements.tickerModal.classList.contains("hidden")) {
      closeTickerModal();
    }
  });
  window.addEventListener("beforeunload", () => {
    if (state.pollHandle) {
      clearInterval(state.pollHandle);
    }
    if (state.marketRefreshHandle) {
      clearInterval(state.marketRefreshHandle);
    }
    if (state.speakingRefreshHandle) {
      clearInterval(state.speakingRefreshHandle);
    }
  });
}

boot();
