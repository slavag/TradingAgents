# TUI Result Summary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Print one compact evidence-honest result table after single-ticker and multi-ticker TUI analyses.

**Architecture:** Add a pure Rich table builder in `cli/main.py` that consumes the result dictionaries already returned by `run_single_analysis`. Call it once from `run_analysis` after all ticker runs finish and before the existing save and full-report flow; target extraction, graph execution, and LLM usage remain untouched.

**Tech Stack:** Python 3.13, Rich `Table`/`Text`, Typer, pytest, Ruff

## Global Constraints

- Support a single ticker and a sequential multi-ticker batch with one row per result.
- Columns are `Ticker`, `Decision`, `Target`, `Confidence (uncalibrated)`, `Horizon`, and `Outlook`.
- Failed results remain visible with `Failed` and a concise error message.
- Missing target metrics render as an em dash; never invent a target, confidence, horizon, or outlook.
- Do not redesign the live progress layout, change graph execution, alter target extraction, or add LLM calls.
- Preserve existing completion messages, default batch-report generation, save behavior, and optional full-report display.

## File Structure

- Modify `cli/main.py`: define `build_tui_result_summary(analysis_results: list[dict]) -> Table` next to the existing compact-report formatting helpers, then print its return value from `run_analysis`.
- Create `tests/test_tui_result_summary.py`: cover successful, unavailable, failed, multi-ticker, and `run_analysis` integration behavior.

---

### Task 1: Build the compact result table

**Files:**
- Create: `tests/test_tui_result_summary.py`
- Modify: `cli/main.py:1243-1270`

**Interfaces:**
- Consumes: `analysis_results: list[dict]`, using `ticker`, `decision`, `price_target`, `confidence_score`, `target_horizon`, `target_summary`, and optional `error`.
- Produces: `build_tui_result_summary(analysis_results: list[dict]) -> Table`.

- [ ] **Step 1: Write failing rendering tests**

Create `tests/test_tui_result_summary.py` with a terminal renderer and three focused tests:

```python
from io import StringIO

from rich.console import Console

import cli.main as cli_main


def render_table(results: list[dict]) -> str:
    output = StringIO()
    console = Console(
        file=output,
        width=220,
        color_system=None,
        force_terminal=False,
    )
    console.print(cli_main.build_tui_result_summary(results))
    return output.getvalue()


def test_tui_result_summary_renders_successful_metrics():
    output = render_table(
        [
            {
                "ticker": "NVDA",
                "decision": "Buy",
                "price_target": 185.25,
                "confidence_score": 78,
                "target_horizon": "3 months",
                "target_summary": "Earnings growth supports the target.",
            }
        ]
    )

    assert "Ticker" in output
    assert "Decision" in output
    assert "Confidence (uncalibrated)" in output
    assert "NVDA" in output
    assert "Buy" in output
    assert "185.25" in output
    assert "78/100" in output
    assert "3 months" in output
    assert "Earnings growth supports the target." in output


def test_tui_result_summary_keeps_missing_metrics_unavailable():
    output = render_table(
        [
            {
                "ticker": "PENG",
                "decision": "Hold",
                "price_target": None,
                "confidence_score": None,
                "target_horizon": None,
                "target_summary": None,
            }
        ]
    )

    assert "PENG" in output
    assert "Hold" in output
    assert output.count("—") >= 4
    assert "50/100" not in output


def test_tui_result_summary_renders_every_ticker_and_failure():
    output = render_table(
        [
            {
                "ticker": "NVDA",
                "decision": "Buy",
                "price_target": 185.25,
                "confidence_score": 78,
                "target_horizon": "3 months",
                "target_summary": "Supported outlook.",
            },
            {
                "ticker": "BROKEN",
                "decision": None,
                "price_target": None,
                "confidence_score": None,
                "target_horizon": None,
                "target_summary": None,
                "error": RuntimeError("provider unavailable"),
            },
        ]
    )

    assert output.count("NVDA") == 1
    assert output.count("BROKEN") == 1
    assert "Failed" in output
    assert "provider unavailable" in output
```

- [ ] **Step 2: Run the rendering tests and verify RED**

Run:

```bash
/Users/slava/.pyenv/shims/python -m pytest tests/test_tui_result_summary.py -q
```

Expected: collection succeeds and all three tests fail with
`AttributeError: module 'cli.main' has no attribute 'build_tui_result_summary'`.

- [ ] **Step 3: Implement the minimal pure Rich table builder**

Add the following immediately after `compact_report_text` in `cli/main.py`:

```python
def build_tui_result_summary(analysis_results: list[dict]) -> Table:
    """Build a compact post-run summary for one ticker or a batch."""
    table = Table(
        title="Analysis Results",
        box=box.SIMPLE_HEAD,
        header_style="bold magenta",
        show_lines=True,
        expand=True,
    )
    table.add_column("Ticker", style="cyan", no_wrap=True)
    table.add_column("Decision", no_wrap=True)
    table.add_column("Target", justify="right", no_wrap=True)
    table.add_column("Confidence (uncalibrated)", justify="right", no_wrap=True)
    table.add_column("Horizon", no_wrap=True)
    table.add_column("Outlook", ratio=2)

    for result in analysis_results:
        error = result.get("error")
        failed = error is not None
        target = result.get("price_target")
        confidence = result.get("confidence_score")
        outlook_source = error if failed else result.get("target_summary")
        outlook = compact_report_text(str(outlook_source or ""), max_chars=160) or "—"

        table.add_row(
            Text(str(result.get("ticker") or "—")),
            Text(
                "Failed" if failed else str(result.get("decision") or "—"),
                style="red" if failed else None,
            ),
            Text(format_price_target(target) if target is not None else "—"),
            Text(f"{confidence}/100" if confidence is not None else "—"),
            Text(str(result.get("target_horizon") or "—")),
            Text(outlook),
        )

    return table
```

Use `Text` cells so report text and exception messages are rendered literally
rather than being interpreted as Rich markup.

- [ ] **Step 4: Run the rendering tests and verify GREEN**

Run:

```bash
/Users/slava/.pyenv/shims/python -m pytest tests/test_tui_result_summary.py -q
```

Expected: `3 passed`.

- [ ] **Step 5: Run nearby formatting regressions**

Run:

```bash
/Users/slava/.pyenv/shims/python -m pytest \
  tests/test_tui_result_summary.py \
  tests/test_consolidated_report_formatting.py \
  tests/test_target_profile.py -q
```

Expected: all selected tests pass. The existing target-profile tests continue
to prove that unavailable metrics stay `None` before presentation.

- [ ] **Step 6: Commit the table builder**

```bash
git add cli/main.py tests/test_tui_result_summary.py
git commit -m "feat: add TUI result summary table"
```

---

### Task 2: Print the summary before existing post-analysis actions

**Files:**
- Modify: `tests/test_tui_result_summary.py`
- Modify: `cli/main.py:2643-2649`

**Interfaces:**
- Consumes: `build_tui_result_summary(analysis_results: list[dict]) -> Table` from Task 1.
- Produces: `run_analysis(checkpoint: bool | None = None)` prints that table exactly once after all requested ticker results have been collected.

- [ ] **Step 1: Write the failing workflow-order test**

Append this test to `tests/test_tui_result_summary.py`:

```python
def test_run_analysis_prints_summary_before_full_report_prompt(monkeypatch):
    result = {
        "ticker": "NVDA",
        "analysis_date": "2026-07-27",
        "decision": "Buy",
        "final_state": {"final_trade_decision": "**Rating**: Buy"},
        "results_dir": "/tmp/nvda",
        "price_target": 185.25,
        "confidence_score": 78,
        "target_horizon": "3 months",
        "target_summary": "Supported outlook.",
        "reference_price": 170.0,
    }
    events = []
    summary_table = object()

    monkeypatch.setattr(
        cli_main,
        "get_user_selections",
        lambda: {"tickers": ["NVDA"], "analysis_date": "2026-07-27"},
    )
    monkeypatch.setattr(
        cli_main,
        "get_save_preferences",
        lambda _selections: {"save_enabled": False, "save_path": None},
    )
    monkeypatch.setattr(
        cli_main,
        "run_single_analysis",
        lambda *_args, **_kwargs: result,
    )

    def build_summary(results):
        assert results == [result]
        events.append("summary-built")
        return summary_table

    def print_output(*args, **_kwargs):
        if args and args[0] is summary_table:
            events.append("summary-printed")

    def prompt(*_args, **_kwargs):
        events.append("prompt")
        return "N"

    monkeypatch.setattr(cli_main, "build_tui_result_summary", build_summary)
    monkeypatch.setattr(cli_main.console, "print", print_output)
    monkeypatch.setattr(cli_main.typer, "prompt", prompt)

    cli_main.run_analysis()

    assert events == ["summary-built", "summary-printed", "prompt"]
```

The patched `run_single_analysis` supplies already-computed target fields. If
the TUI layer attempted a new target-estimation or LLM step, this test would
have no such dependency available and would fail.

- [ ] **Step 2: Run the workflow test and verify RED**

Run:

```bash
/Users/slava/.pyenv/shims/python -m pytest \
  tests/test_tui_result_summary.py::test_run_analysis_prints_summary_before_full_report_prompt -q
```

Expected: FAIL because `events` contains only `["prompt"]`; `run_analysis` does
not yet build or print the compact summary.

- [ ] **Step 3: Print the table once after the completion message**

In `run_analysis`, immediately after the existing `Analysis Complete!` or
`Batch Analysis Complete!` message and before `successful_results` is
calculated, add:

```python
    console.print(build_tui_result_summary(analysis_results))
```

Do not move or modify the default batch-report generation, save block,
`successful_results` filtering, or full-report prompt.

- [ ] **Step 4: Run the workflow test and verify GREEN**

Run:

```bash
/Users/slava/.pyenv/shims/python -m pytest \
  tests/test_tui_result_summary.py::test_run_analysis_prints_summary_before_full_report_prompt -q
```

Expected: `1 passed`.

- [ ] **Step 5: Run all focused TUI and lifecycle tests**

Run:

```bash
/Users/slava/.pyenv/shims/python -m pytest \
  tests/test_tui_result_summary.py \
  tests/test_consolidated_report_formatting.py \
  tests/test_target_profile.py \
  tests/test_graph_execution_lifecycle.py \
  tests/test_cli_no_console.py -q
```

Expected: all selected tests pass. No existing completion, failure, lifecycle,
or no-console behavior regresses.

- [ ] **Step 6: Commit the workflow integration**

```bash
git add cli/main.py tests/test_tui_result_summary.py
git commit -m "feat: show TUI summary after analysis"
```

---

### Task 3: Final verification

**Files:**
- Verify only; no source changes expected.

**Interfaces:**
- Consumes: the committed implementation from Tasks 1 and 2.
- Produces: fresh evidence that the entire branch is test-clean, lint-clean, and free of whitespace errors.

- [ ] **Step 1: Run the complete test suite**

Run:

```bash
/Users/slava/.pyenv/shims/python -m pytest -q
```

Expected: all project tests pass; optional dependency skips and warnings may
remain only if they match the repository's existing baseline.

- [ ] **Step 2: Run Ruff on the changed Python files**

Run:

```bash
/Users/slava/.pyenv/shims/ruff check cli/main.py tests/test_tui_result_summary.py
```

Expected: `All checks passed!`

- [ ] **Step 3: Verify formatting and repository state**

Run:

```bash
git diff --check
git status --short --branch
```

Expected: `git diff --check` prints nothing, and status shows the feature branch
ahead only by the intended commits with no uncommitted files.
