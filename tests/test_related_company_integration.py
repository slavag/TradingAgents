"""Related facts reach every decision stage without becoming primary stock data."""
from unittest.mock import MagicMock

import pytest
from langgraph.prebuilt import ToolNode

from tradingagents.agents.utils.agent_utils import get_agent_context_from_state
from tradingagents.agents.utils.decision_integrity import (
    build_decision_evidence,
    verified_price_context,
    verified_target_candidates,
)
from tradingagents.graph.conditional_logic import ConditionalLogic
from tradingagents.graph.setup import GraphSetup
from tradingagents.reporting import build_run_manifest, compare_run_manifests
from tradingagents.research_ledger import build_research_ledger


def source():
    return {
        'company_of_interest':'MAIN', 'trade_date':'2026-11-13',
        'shared_evidence':[{'evidence_id':'main-price','source':'get_verified_market_snapshot','category':'market',
                            'status':'available','as_of_date':'2026-11-13','published_at':'2026-11-13',
                            'content':'- Latest trading row used: 2026-11-12\n| Close | 100.00 |'}],
        'related_company_research':{
            'target':'MAIN','as_of':'2026-11-13','status':'partial','coverage':[], 'sources':[],
            'entities':[{'name':'Peer Corporation','symbol':'PEER','identity_status':'source_verified',
                         'roles':[{'role':'competitor','quote':'We compete with Peer Corporation in cloud services.',
                                   'source_id':'proof','relationship_date':'2026-11-12'}],
                         'facts':[], 'performance':[], 'coverage':[],
                         'news':{'status':'available','latest_7_days':['peer-news'],'material_30_days':['peer-news']},
                         'sources':[{'evidence_id':'peer-news','source':'wire','category':'news','title':'Peer capacity expansion',
                                     'content':'Peer Corporation announced an expansion of cloud capacity.',
                                     'as_of_date':'2026-11-13','published_at':'2026-11-12','status':'available'}]}],
        },
    }


@pytest.mark.parametrize('parallel',[False,True])
def test_related_company_stage_precedes_sequential_and_parallel_analysts(parallel):
    setup=GraphSetup(MagicMock(),MagicMock(),{'market':ToolNode([])},ConditionalLogic(1,1))
    graph=setup.setup_graph(['market'],parallel_analysts=parallel)
    assert ('Evidence Collector','Related Company Research') in graph.edges
    assert ('Related Company Research','Market Analyst') in graph.edges
    assert ('Evidence Collector','Market Analyst') not in graph.edges


def test_labeled_related_evidence_reaches_decision_roles_without_changing_primary_reference():
    item=source()
    for role in ('market','news','fundamentals','bull','bear','research_manager','trader','risk','portfolio_manager'):
        context=get_agent_context_from_state(item,role,include_reports=True)
        assert 'Peer Corporation announced an expansion' in context
        assert 'peer-news' in context
    assert 'Peer Corporation announced an expansion' in build_decision_evidence(item)
    assert verified_price_context(item)['close']==100
    assert verified_target_candidates(item)==['| Close | 100.00 |']
    assert all(fact.value != 9999 for fact in build_research_ledger(item,'2026-11-13').facts)
    assert len(item['shared_evidence'])==1


def test_related_news_is_evidence_drift_but_retrieval_timestamp_is_not():
    item=source()
    first=build_run_manifest(item,'MAIN')
    item['related_company_research']['entities'][0]['sources'][0]['retrieved_at']='2026-11-13T01:00:00Z'
    same=build_run_manifest(item,'MAIN')
    assert first['evidence_fingerprint']==same['evidence_fingerprint']
    item['related_company_research']['entities'][0]['sources'][0]['content']='Peer Corporation reported a capacity outage.'
    second=build_run_manifest(item,'MAIN')
    assert compare_run_manifests(second,first)=='evidence_changed'


def test_primary_comparison_performance_uses_same_scoped_adapter_without_price_anchor_pollution(monkeypatch):
    from datetime import date

    from tradingagents.related_companies import primary
    calls=[]
    monkeypatch.setattr(primary, '_performance', lambda *args: calls.append(args) or ([{'months':6,'return_pct':10,'end_date':date.today().isoformat(),'currency':'USD','adjustment':'same'}], []))
    item=source() | {'analysis_mode':'live','trade_date':date.today().isoformat(),'instrument_context':'Quote currency: USD.'}
    update={'related_company_research':item['related_company_research'] | {'as_of':date.today().isoformat()}}
    result=primary.augment_primary_performance(item,update)
    assert calls==[('MAIN',date.today().isoformat(),'USD')]
    assert result['related_company_research']['primary_performance'][0]['return_pct']==10
    assert 'verified_market_snapshot' not in result
    assert len(item['shared_evidence'])==1


def test_historical_primary_comparison_does_not_fetch_live_price_data(monkeypatch):
    from tradingagents.related_companies import primary
    monkeypatch.setattr(primary,'_performance',lambda *_: (_ for _ in ()).throw(AssertionError('live fetch')))
    item=source() | {'analysis_mode':'historical'}
    result=primary.augment_primary_performance(item,{'related_company_research':item['related_company_research']})
    assert result['related_company_research']['primary_performance']==[]


def test_mismatched_related_bundle_cannot_leak_later_evidence_into_earlier_context():
    item=source()
    item['trade_date']='2026-11-01'
    for role in ('market','portfolio_manager'):
        assert 'Peer Corporation announced an expansion' not in get_agent_context_from_state(item,role)
    assert 'Peer Corporation announced an expansion' not in build_decision_evidence(item)
