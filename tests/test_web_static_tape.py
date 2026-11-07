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


def test_discover_markup_has_bounded_count_refresh_and_watchlist_region():
    page = (WEB_STATIC_ROOT / "index.html").read_text(encoding="utf-8")
    assert 'id="discovery-limit"' in page
    assert '<option value="15">15</option>' in page
    assert '<option value="20" selected>20</option>' in page
    assert 'id="refresh-discovery"' in page
    assert 'id="discovery-view-tiles"' in page
    assert 'id="discovery-view-rows"' in page
    assert 'aria-label="Watchlist layout"' in page
    assert 'id="ticker-track"' in page
    assert 'id="discovery-coverage"' in page
    assert 'aria-live="polite"' in page


def test_discover_candidate_card_is_escaped_informative_and_not_a_submit_button():
    html = evaluate_runtime(
        "renderMomentumCandidate({ticker:'AMD',company_name:'<img src=x>',close:202.5,currency:'USD',quote_date:'2026-10-15',price_basis:'unadjusted close; split-adjusted returns',ret_1d_pct:2.2,ret_5d_pct:8.4,ret_20d_pct:17.8,avg_dollar_volume:123000000,relative_volume:1.8,trend_checks:2,score:9.2,sources:['apewisdom','alpha_vantage_gainers'],reason:'Five-session move <script>x</script>',risk_flags:['high_volatility_spike'],already_analyzed:true,research_url:'/api/jobs/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/reports/AMD/investor'})"
    )
    assert "<img src=x>" not in html and "&lt;img src=x&gt;" in html
    assert "<script>" not in html and "&lt;script&gt;" in html
    for value in ("1D +2.2%", "5D +8.4%", "20D +17.8%", "1.8×", "2/2", "123.0M"):
        assert value in html
    assert "Already analyzed" in html and "Open saved research" in html
    assert "unadjusted close; split-adjusted returns" in html
    assert '/api/jobs/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/reports/AMD/investor' in html
    assert 'type="button"' in html and 'data-discover-add="AMD"' in html
    assert 'type="submit"' not in html


def test_duplicate_signal_types_keep_distinct_provider_provenance():
    html = evaluate_runtime(
        "renderMomentumCandidate({ticker:'CCC',company_name:'CCC',close:7.61,currency:'USD',quote_date:'2026-10-16',ret_1d_pct:-0.3,ret_5d_pct:8.4,ret_20d_pct:16.2,avg_dollar_volume:242000000,relative_volume:3.7,trend_checks:1,score:8.12,sources:['alpha_vantage_active','fmp_active'],reason:'Measured move',risk_flags:[],already_analyzed:false})"
    )
    assert "Alpha Vantage most active" in html
    assert "FMP most active" in html


def test_discover_groups_new_first_without_padding_and_explains_states():
    rendered = evaluate_runtime(
        "(() => { const make=(ticker,already_analyzed)=>({ticker,company_name:ticker,close:10,currency:'USD',quote_date:'2026-10-15',ret_1d_pct:1,ret_5d_pct:2,ret_20d_pct:3,avg_dollar_volume:11000000,relative_volume:1.1,trend_checks:1,score:4,sources:['apewisdom'],reason:'Measured move',risk_flags:[],already_analyzed}); return {html:renderMomentumWatchlist([make('AMD',true),make('PENG',false)]),partial:momentumStatusCopy({status:'partial',items:[make('PENG',false)],retrieved_at:'2026-10-16T12:00:00Z',quote_as_of:'2026-10-15',warnings:['fmp_unavailable']},20),stale:momentumStatusCopy({status:'stale',items:[make('PENG',false)],retrieved_at:'2026-10-16T12:00:00Z',quote_as_of:'2026-10-15',warnings:['refresh_failed']},20),empty:momentumStatusCopy({status:'fresh',items:[],retrieved_at:'2026-10-16T12:00:00Z'},20)}; })()"
    )
    assert rendered["html"].index("PENG") < rendered["html"].index("AMD")
    assert rendered["html"].count('data-discover-add=') == 2
    assert "Partial" in rendered["partial"] and "2026-10-15" in rendered["partial"]
    assert "Stale" in rendered["stale"] and "2026" in rendered["stale"]
    assert "0 of up to 20 stocks" in rendered["empty"]


def test_unavailable_identity_data_does_not_claim_no_stocks_qualify():
    html = evaluate_runtime("renderMomentumWatchlist([], 'unavailable')")
    assert "could not be verified" in html
    assert "No stocks met" not in html


def test_empty_fresh_screen_explains_which_stage_removed_candidates():
    price_gap = evaluate_runtime(
        "renderMomentumWatchlist([], 'fresh', {nominated:100,with_history:100,price_eligible:0,identity_checked:0,verified_equities:0,qualified:0})"
    )
    identity_gap = evaluate_runtime(
        "renderMomentumWatchlist([], 'fresh', {nominated:20,with_history:18,verified_equities:0,qualified:0})"
    )
    threshold_gap = evaluate_runtime(
        "renderMomentumWatchlist([], 'fresh', {nominated:20,with_history:18,verified_equities:7,qualified:0})"
    )
    assert "identity verification" in identity_gap
    assert "price, liquidity, or five-day momentum" in price_gap
    assert "identity verification" not in price_gap
    assert "7 verified equities" in threshold_gap
    assert "momentum or liquidity" in threshold_gap


def test_zero_items_status_does_not_claim_the_screen_rejected_verified_stocks():
    status = evaluate_runtime(
        "momentumStatusCopy({status:'fresh',items:[],retrieved_at:'2026-10-17T07:39:00Z',screening:{nominated:20,with_history:18,verified_equities:0,qualified:0}},20)"
    )
    assert "0 of up to 20 stocks" in status
    assert "No stocks met" not in status


def test_short_watchlist_explains_bounded_screening_funnel():
    copy = evaluate_runtime(
        "momentumScreeningCopy({status:'fresh',items:[{ticker:'CCC'},{ticker:'T'}],screening:{nominated:214,selected_for_prices:100,with_history:100,price_eligible:12,identity_checked:12,verified_equities:5,qualified:2}})"
    )
    assert "214 nominated" in copy
    assert "100 selected for prices" in copy
    assert "100 with history" in copy
    assert "12 passed price, liquidity, and momentum" in copy
    assert "5 identity-verified" in copy
    assert "2 qualified" in copy


def test_discover_retrieval_time_is_formatted_in_the_users_timezone():
    formatted = evaluate_runtime(
        "formatDiscoveryTimestamp('2026-10-16T12:00:00Z',{locale:'en-GB',timeZone:'Asia/Jerusalem'})"
    )
    assert "15:00" in formatted
    assert "UTC" not in formatted


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
