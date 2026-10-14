"""Exercise ticker provenance and analysis rendering, independent of CSS layout."""

import json
import shutil
import subprocess
from html.parser import HTMLParser
from pathlib import Path

import pytest

WEB_STATIC_ROOT = Path(__file__).resolve().parents[1] / "tradingagents" / "web" / "static"


def evaluate_runtime(expression):
    if not shutil.which("node"):
        pytest.skip("Node.js is required to exercise the browser runtime")
    harness = r"""
const fs = require('node:fs'), vm = require('node:vm');
const picker = require(process.argv[1] + '/model_picker.js');
const nodes = new Map();
const context = vm.createContext({
  window: {TradingAgentsModelPicker:picker},
  document: {getElementById(id) {if (!nodes.has(id)) nodes.set(id,{value:'',checked:false,options:[],classList:{toggle(){},add(){},remove(){}},style:{setProperty(){}}});return nodes.get(id);}, querySelectorAll(){return[];}},
  URL, Date, console, Set, Map,
});
const source = fs.readFileSync(process.argv[1] + '/app.js','utf8').replace(/\nboot\(\);\s*$/, '');
vm.runInContext(source,context);
console.log(JSON.stringify(vm.runInContext(process.argv[2],context)));
"""
    result = subprocess.run(
        ["node", "-e", harness, str(WEB_STATIC_ROOT), expression],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


class Buttons(HTMLParser):
    def __init__(self):
        super().__init__()
        self.buttons = []

    def handle_starttag(self, tag, attrs):
        if tag == "button":
            self.buttons.append(dict(attrs))


def test_chatter_candidates_preserve_provenance_and_use_native_buttons():
    html = evaluate_runtime(
        "tickerItem({ticker:'PENG',price:79.24,market_open:false,score:1.2,ret_5d_pct:5,trend_score:2,sources:['ApeWisdom','StockTwits'],source_count:2})"
    )
    parser = Buttons()
    parser.feed(html)
    assert len(parser.buttons) == 1
    assert parser.buttons[0]["type"] == "button"
    assert parser.buttons[0]["data-ticker-symbol"] == "PENG"
    assert "not a buy/sell signal" in parser.buttons[0]["aria-label"]
    assert "ApeWisdom" in html and "StockTwits" in html
    assert "79.24" in html and "5D +5.0%" in html


def test_chatter_list_does_not_duplicate_tickers_or_inject_markup():
    html = evaluate_runtime(
        "(() => { const track={style:{setProperty(){}}}; renderTape(track,[{ticker:'<img src=x>',price:1},{ticker:'NVDA',price:2}],tickerItem,20,'Empty'); return track.innerHTML; })()"
    )
    parser = Buttons()
    parser.feed(html)
    assert len(parser.buttons) == 2
    assert "<img src=x>" not in html
    assert "&lt;img src=x&gt;" in html


def test_result_guidance_conditions_and_confidence_survive_rendering():
    html = evaluate_runtime(
        "resultCard({ticker:'PENG',decision:'Overweight',status:'completed',existing_position_action:'Hold',existing_position_summary:'Keep the existing core.',new_position_action:'Conditional Buy',new_position_summary:'Enter only after confirmation.',conditional_confirmation:'Close above the prior high.',conditional_alternative:'Wait for stabilization.',conditional_invalidation:'Cancel on a closing loss.',recommendation_confidence_label:'72/100 (uncalibrated)',price_target_label:'80.10',target_status_label:'Validated',executive_summary:'Evidence <requires> care.'})"
    )
    for text in (
        "Keep the existing core.",
        "Enter only after confirmation.",
        "Close above the prior high.",
        "Wait for stabilization.",
        "Cancel on a closing loss.",
        "72/100 (uncalibrated)",
        "80.10",
    ):
        assert text in html
    assert "Evidence &lt;requires&gt; care." in html


def test_evaluation_uses_selected_tickers_and_exact_completed_batch_paths():
    result = evaluate_runtime(
        "(() => { elements.evaluationScope.value='selected'; elements.tickers.value='PENG, SHOP.TO'; const selected=buildEvaluationRequest(); elements.evaluationScope.value='current_batch'; state.currentResults=[{results_dir:'/saved/PENG/run-a'}]; return {selected,batch:buildEvaluationRequest()}; })()"
    )
    assert result["selected"]["tickers"] == ["PENG", "SHOP.TO"]
    assert result["batch"]["report_paths"] == ["/saved/PENG/run-a"]


def test_empty_current_batch_evaluation_requires_a_completed_selection():
    result = evaluate_runtime(
        "(() => { elements.evaluationScope.value='current_batch'; state.currentResults=[]; try {buildEvaluationRequest(); return false;} catch (_) {return true;} })()"
    )
    assert result is True


def test_recovered_and_partial_runs_have_explicit_statuses():
    assert evaluate_runtime("statusChip({status:'interrupted'})") == [
        "Interrupted",
        "chip interrupted",
    ]
    assert evaluate_runtime("statusChip({status:'partial'})") == ["Partial", "chip partial"]


def test_saved_google_reasoning_and_export_preferences_are_restored():
    result = evaluate_runtime(
        "(() => { elements.googleThinking.value='low'; elements.saveReports.checked=true; window.TradingAgentsRuntime.restoreDefaults({google_thinking_level:'high',save_reports:false}); return {thinking:elements.googleThinking.value,export:elements.saveReports.checked}; })()"
    )
    assert result == {"thinking": "high", "export": False}
