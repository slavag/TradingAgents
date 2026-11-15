"""Consolidated reports synthesize complete statements instead of slicing text."""
from tests.test_investor_outlooks import outlook, state
from tradingagents.agents.utils.outlooks import validate_outlooks
from tradingagents.consolidated_brief import (
    build_stock_brief,
    render_brief_html,
    render_brief_markdown,
)


def result():
    report_state = state()
    report_state.update({
        'portfolio_decision': {
            'executive_summary': 'Hold existing exposure while demand remains uncertain. Avoid a new position before confirmation.',
            'investment_thesis': ('The company serves enterprise buyers with hardware and cloud services. ' * 30
                                 + 'Margins improved, but cash conversion remains weak. Reconsider the view if the customer renewal fails.'),
        },
        'market_report': 'The daily trend remains weak. The short-term recovery has not confirmed a reversal.',
        'fundamentals_report': 'Revenue increased while operating cash remained negative. Financial resilience depends on cash conversion.',
        'news_report': 'No verified company-specific news was returned in the requested window.',
        'trader_investment_plan': 'Do not add before confirmation. This is a conditional plan.',
        'portfolio_outlooks': [item.model_dump(mode='json') for item in validate_outlooks([outlook(), outlook(12)], report_state)],
    })
    return {'ticker': 'TEST', 'analysis_date': '2026-11-13', 'decision': 'Hold',
            'existing_position_action': 'Hold', 'existing_position_summary': 'Maintain the current exposure. Exit only if the verified customer renewal fails.',
            'new_position_action': 'Wait', 'new_position_summary': 'Wait for confirmation. Do not initiate while cash conversion is weak.',
            'conditional_confirmation': 'Require sustained demand and improving cash conversion.',
            'conditional_invalidation': 'A failed renewal invalidates the setup.',
            'price_target': 105, 'reference_price': 100, 'recommendation_confidence_score': 61, 'confidence_score': 28,
            'target_validation_status': 'Accepted', 'target_horizon': '1–3 months',
            'target_summary': 'A conditional technical scenario.', 'target_risks': 'A failed renewal would weaken the case.',
            'results_dir': '/tmp', 'report_path': '/tmp/TEST.md', 'run_id': 'a'*32, 'final_state': report_state}


def test_complete_brief_condenses_repetition_and_preserves_horizons_and_conditions():
    item = result()
    brief = build_stock_brief(item)
    for text in (render_brief_markdown(brief), render_brief_html(brief)):
        assert '...' not in text and '…' not in text
        assert 'Exit only if the verified customer renewal fails.' in text
        assert 'A failed renewal invalidates the setup.' in text
        assert '6-month' in text and '12-month' in text
        assert '2027-05-13' in text and '2027-11-13' in text
        assert '65/100' in text and '25/100' in text
        assert '110' in text and '125' in text
        assert 'enterprise buyers' not in text or text.count('enterprise buyers') == 1
        assert len(text.split()) < 500


def test_long_material_single_sentence_is_not_cut_to_meet_an_arbitrary_limit():
    item = result()
    statement = 'Exit only if ' + 'the fully verified customer renewal condition and ' * 70 + 'the disclosed financing condition both fail.'
    item['conditional_invalidation'] = statement
    assert statement in render_brief_markdown(build_stock_brief(item))


def test_html_escapes_source_text_and_links_to_full_detail():
    item = result()
    item['final_state']['portfolio_decision']['executive_summary'] = 'Hold. <script>unsafe()</script> is untrusted text.'
    html = render_brief_html(build_stock_brief(item))
    assert '<script>' not in html
    assert '/api/jobs/' + 'a'*32 + '/reports/TEST/full' in html
    assert '/api/jobs/' + 'a'*32 + '/reports/TEST/investor' in html


def test_unavailable_extended_hours_input_does_not_create_a_summary_notice():
    item = result()
    item['final_state']['extended_hours_reaction'] = {'status':'unavailable', 'reason':'outside_supported_session'}
    for text in (render_brief_markdown(build_stock_brief(item)), render_brief_html(build_stock_brief(item))):
        assert 'Extended-hours market reaction' not in text
        assert 'outside_supported_session' not in text


def test_both_consolidated_outputs_use_the_same_compact_complete_brief():
    from cli.main import build_consolidated_report, build_consolidated_report_html
    item = result()
    for rendered in (build_consolidated_report([item], item['analysis_date']),
                     build_consolidated_report_html([item], item['analysis_date'])):
        assert 'A failed renewal invalidates the setup.' in rendered
        assert '65/100' in rendered and '25/100' in rendered
        assert rendered.count('enterprise buyers') <= 1
        assert '### Fundamentals Report' not in rendered
        assert '<h3>Trader Plan</h3>' not in rendered


def test_material_conditions_and_accounting_qualifications_survive_sentence_selection():
    item = result()
    item['final_state']['portfolio_decision']['investment_thesis'] = 'Revenue growth and cash margins improved. Customer demand and valuation support recovery. Exit immediately if the license is revoked.'
    item['final_state']['fundamentals_report'] = 'Revenue and cash growth improved. These figures use preliminary unaudited estimates only.'
    for text in (render_brief_markdown(build_stock_brief(item)), render_brief_html(build_stock_brief(item))):
        assert 'Exit immediately if the license is revoked.' in text
        assert 'These figures use preliminary unaudited estimates only.' in text


def test_saved_cli_reports_have_detail_links_without_a_web_job_id():
    item = result()
    item.pop('run_id')
    item['report_path'] = '/tmp/main/full.md'
    item['investor_report_path'] = '/tmp/main/investor.html'
    for text in (render_brief_markdown(build_stock_brief(item)), render_brief_html(build_stock_brief(item))):
        from pathlib import Path
        assert Path('/tmp/main/full.md').resolve().as_uri() in text
        assert Path('/tmp/main/investor.html').resolve().as_uri() in text


def test_accepted_tactical_target_conditions_and_assumptions_survive_both_formats():
    from cli.main import build_consolidated_report, build_consolidated_report_html
    item = result()
    item['target_summary'] = 'The tactical target depends on regulatory approval. Abandon this target if the operating license is revoked.'
    item['target_assumptions'] = 'Use the verified reference price of 100. Assume a 5% recovery, giving a target of 105. This remains conditional on renewal.'
    for text in (build_consolidated_report([item],item['analysis_date']),build_consolidated_report_html([item],item['analysis_date'])):
        assert 'The tactical target depends on regulatory approval.' in text
        assert 'Abandon this target if the operating license is revoked.' in text
        assert 'Assume a 5% recovery, giving a target of 105.' in text
        assert 'conditional on renewal' in text


def test_model_valuation_method_is_not_reclassified_as_technical_support():
    item = result()
    item['target_basis'] = 'Model estimate'
    item['target_method'] = 'Discounted cash flow uses explicit growth assumptions and customer support expectations together with a stated discount rate and terminal cash flow scenario.'
    text = render_brief_markdown(build_stock_brief(item))
    assert 'Discounted cash flow' in text
    assert 'Source-anchored technical scenario' not in text
