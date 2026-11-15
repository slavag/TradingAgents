from datetime import datetime
from types import SimpleNamespace

import pytest

from tradingagents.dataflows import market_reaction as reaction


def epoch(value):
    return datetime.fromisoformat(value).timestamp()


@pytest.fixture
def quotes(monkeypatch):
    now = datetime.fromisoformat('2026-11-13T12:00:00+00:00')
    monkeypatch.setattr(reaction, '_now', lambda: now)
    info = {'exchange': 'NMS', 'exchangeTimezoneName': 'America/New_York', 'currency': 'USD',
                'regularMarketPrice': 100, 'regularMarketTime': epoch('2026-11-12T21:00:00+00:00'),
                'postMarketPrice': 110, 'postMarketTime': epoch('2026-11-12T23:00:00+00:00'),
                'preMarketPrice': 120, 'preMarketTime': epoch('2026-11-13T11:50:00+00:00')}
    calls = []
    monkeypatch.setattr(reaction.yf, 'Ticker', lambda symbol: calls.append(symbol) or SimpleNamespace(get_info=lambda: info))
    return info, calls


def test_selects_newest_valid_quote_with_separate_regular_reference(quotes):
    record = reaction.fetch_extended_hours_reaction('CBRS', '2026-11-13')
    assert record['status'] == 'available'
    assert record['quote_type'] == 'pre_market'
    assert record['price'] == 120 and record['regular_close'] == 100
    assert record['change_pct'] == pytest.approx(20)
    assert record['observed_at'] == '2026-11-13T11:50:00+00:00'
    assert record['age_seconds'] == 600
    assert 'causal' in reaction.render_market_reaction(record)


def test_historical_request_never_fetches(quotes):
    _, calls = quotes
    assert reaction.fetch_extended_hours_reaction('CBRS', '2026-11-12')['status'] == 'unavailable'
    assert calls == []


@pytest.mark.parametrize('field,value', [('preMarketPrice', float('nan')), ('preMarketPrice', -1),
    ('preMarketTime', '2026-11-13 06:50:00'), ('preMarketTime', '2026-11-13T13:00:00+00:00'),
    ('preMarketTime', '2026-11-12T11:00:00+00:00')])
def test_invalid_premarket_can_fall_back_to_recent_postmarket(quotes, field, value):
    info, _ = quotes
    info[field] = value
    record = reaction.fetch_extended_hours_reaction('CBRS', '2026-11-13')
    assert record['quote_type'] == 'post_market'
    assert record['price'] == 110


@pytest.mark.parametrize('field,value', [('regularMarketTime', None), ('regularMarketPrice', 0),
    ('exchangeTimezoneName', None), ('currency', None), ('exchange', 'LSE')])
def test_requires_verified_reference_and_us_metadata(quotes, field, value):
    info, _ = quotes
    info[field] = value
    assert reaction.fetch_extended_hours_reaction('CBRS', '2026-11-13')['status'] in {'unavailable', 'unsupported'}


def test_superseded_quote_hidden_during_regular_session(quotes, monkeypatch):
    monkeypatch.setattr(reaction, '_now', lambda: datetime.fromisoformat('2026-11-13T17:00:00+00:00'))
    info, _ = quotes
    info['regularMarketTime'] = epoch('2026-11-13T16:59:00+00:00')
    assert reaction.fetch_extended_hours_reaction('CBRS', '2026-11-13')['status'] == 'unavailable'


def test_provider_errors_do_not_leak_exception(quotes, monkeypatch):
    monkeypatch.setattr(reaction.yf, 'Ticker', lambda _: (_ for _ in ()).throw(RuntimeError('token=secret')))
    record = reaction.fetch_extended_hours_reaction('CBRS', '2026-11-13')
    assert record['reason'] == 'provider_error'
    assert 'secret' not in str(record)


def test_stale_overnight_quote_not_published(quotes, monkeypatch):
    info, _ = quotes
    info.pop('preMarketTime')
    info['postMarketTime'] = epoch('2026-11-11T23:00:00+00:00')
    assert reaction.fetch_extended_hours_reaction('CBRS', '2026-11-13')['status'] == 'unavailable'


def test_market_stage_fetches_once_injects_prompt_and_publishes_marker(monkeypatch):
    from langchain_core.messages import AIMessage

    import tradingagents.agents.analysts.market_analyst as market
    record = {'status': 'available', 'quote_type': 'post_market', 'price': 110, 'currency': 'USD',
                  'observed_at': '2026-11-12T23:00:00+00:00', 'change_pct': 10, 'regular_close': 100,
                  'regular_observed_at': '2026-11-12T21:00:00+00:00', 'provider': 'yfinance',
                  'fetched_at': '2026-11-13T12:00:00+00:00', 'age_seconds': 46800}
    calls, prompts = [], []
    monkeypatch.setattr(market, 'fetch_extended_hours_reaction', lambda *args: calls.append(args) or record)
    monkeypatch.setattr(market, 'take_turn', lambda prompt, *args: prompts.append(prompt.format(messages=[])) or AIMessage(content='Analysis.'))
    monkeypatch.setattr(market, 'verified_market_snapshot', lambda *args: SimpleNamespace(model_dump=lambda **kwargs: {'close': 100}))
    state = {'trade_date': '2026-11-13', 'company_of_interest': 'CBRS', 'analysis_mode': 'live', 'messages': [], 'shared_evidence': []}
    node = market.create_market_analyst(None)
    response = node(state)
    assert 'post market' in prompts[0]
    assert response['market_report'] == 'Analysis.'
    assert response['verified_market_snapshot']['close'] == 100
    assert response['shared_evidence'][0]['category'] == 'market_reaction'
    assert response['shared_evidence'][0]['published_at'] == record['observed_at']
    state.update(response)
    node(state)
    assert len(calls) == 1
    assert len(response['shared_evidence']) == 1


def test_historical_stage_skips_live_reaction_adapter(monkeypatch):
    from langchain_core.messages import AIMessage

    import tradingagents.agents.analysts.market_analyst as market
    monkeypatch.setattr(market, 'fetch_extended_hours_reaction', lambda *args: pytest.fail('Historical provider request'))
    monkeypatch.setattr(market, 'take_turn', lambda *args: AIMessage(content='Historical analysis.'))
    monkeypatch.setattr(market, 'verified_market_snapshot', lambda *args: SimpleNamespace(model_dump=lambda **kwargs: {}))
    result = market.create_market_analyst(None)({'trade_date': '2026-11-13', 'company_of_interest': 'CBRS', 'analysis_mode': 'historical', 'messages': []})
    assert result['extended_hours_reaction']['reason'] == 'historical_analysis'


def test_prior_evening_reaction_survives_overnight_until_new_regular_quote(quotes, monkeypatch):
    info, _ = quotes
    info.pop('preMarketTime')
    monkeypatch.setattr(reaction, '_now', lambda: datetime.fromisoformat('2026-11-13T09:20:00+00:00'))
    record = reaction.fetch_extended_hours_reaction('CBRS', '2026-11-13')
    assert record['status'] == 'available'
    assert record['quote_type'] == 'post_market'
    assert record['observed_at'].startswith('2026-11-12')
    monkeypatch.setattr(reaction, '_now', lambda: datetime.fromisoformat('2026-11-13T14:40:00+00:00'))
    info['regularMarketTime'] = epoch('2026-11-13T14:39:00+00:00')
    assert reaction.fetch_extended_hours_reaction('CBRS', '2026-11-13')['status'] == 'unavailable'


def test_after_hours_must_compare_to_same_regular_session(quotes):
    info, _ = quotes
    info.pop('preMarketTime')
    info['regularMarketTime'] = epoch('2026-11-11T21:00:00+00:00')
    assert reaction.fetch_extended_hours_reaction('CBRS', '2026-11-13')['status'] == 'unavailable'


def test_current_evening_postmarket_quote_is_published(quotes, monkeypatch):
    info, _ = quotes
    monkeypatch.setattr(reaction, '_now', lambda: datetime.fromisoformat('2026-11-13T23:20:00+00:00'))
    info.update(regularMarketTime=epoch('2026-11-13T21:00:00+00:00'),
                postMarketTime=epoch('2026-11-13T23:10:00+00:00'))
    assert reaction.fetch_extended_hours_reaction('CBRS', '2026-11-13')['quote_type'] == 'post_market'


@pytest.mark.parametrize('regular_time', ['2026-11-11T21:00:00+00:00', '2026-11-12T14:30:00+00:00'])
def test_premarket_rejects_stale_session_or_opening_quote_reference(quotes, regular_time):
    info, _ = quotes
    info['regularMarketTime'] = epoch(regular_time)
    assert reaction.fetch_extended_hours_reaction('CBRS', '2026-11-13')['status'] == 'unavailable'


@pytest.mark.parametrize('now,regular_time,pre_time', [
    ('2026-11-16T12:00:00+00:00', '2026-11-13T21:00:00+00:00', '2026-11-16T11:50:00+00:00'),
    ('2026-07-06T11:00:00+00:00', '2026-07-02T20:00:00+00:00', '2026-07-06T10:50:00+00:00'),
])
def test_premarket_uses_latest_completed_session_across_weekend_and_holiday(quotes, monkeypatch, now, regular_time, pre_time):
    info, _ = quotes
    monkeypatch.setattr(reaction, '_now', lambda: datetime.fromisoformat(now))
    info.update(regularMarketTime=epoch(regular_time), preMarketTime=epoch(pre_time))
    result = reaction.fetch_extended_hours_reaction('CBRS', now[:10])
    assert result['status'] == 'available'
    assert result['quote_type'] == 'pre_market'
    assert result['regular_observed_at'] == regular_time


def test_half_day_postmarket_uses_calendar_close(quotes, monkeypatch):
    info, _ = quotes
    monkeypatch.setattr(reaction, '_now', lambda: datetime.fromisoformat('2026-11-27T19:10:00+00:00'))
    info.update(regularMarketTime=epoch('2026-11-27T18:00:00+00:00'),
                postMarketTime=epoch('2026-11-27T19:00:00+00:00'))
    result = reaction.fetch_extended_hours_reaction('CBRS', '2026-11-27')
    assert result['status'] == 'available'
    assert result['quote_type'] == 'post_market'


def test_exchange_holiday_never_fetches_quote(quotes, monkeypatch):
    _, calls = quotes
    monkeypatch.setattr(reaction, '_now', lambda: datetime.fromisoformat('2026-11-26T12:00:00+00:00'))
    assert reaction.fetch_extended_hours_reaction('CBRS', '2026-11-26')['status'] == 'unavailable'
    assert calls == []


def test_calendar_failure_does_not_guess_session_or_call_provider(quotes, monkeypatch):
    _, calls = quotes
    monkeypatch.setattr(reaction.mcal, 'get_calendar', lambda *args: (_ for _ in ()).throw(RuntimeError('broken')))
    result = reaction.fetch_extended_hours_reaction('CBRS', '2026-11-13')
    assert result['reason'] == 'session_calendar_unavailable'
    assert calls == []


@pytest.mark.parametrize('stamp', ['2026-11-12T20:58:59+00:00', '2026-11-12T21:01:01+00:00'])
def test_regular_reference_must_be_in_closing_window(quotes, stamp):
    info, _ = quotes
    info['regularMarketTime'] = epoch(stamp)
    assert reaction.fetch_extended_hours_reaction('CBRS', '2026-11-13')['reason'] == 'invalid_regular_reference'
