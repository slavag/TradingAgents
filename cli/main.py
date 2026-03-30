from typing import Optional
import datetime
import json
import re
import typer
from html import escape
from pathlib import Path
from functools import wraps
from rich.console import Console
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
from rich.panel import Panel
from rich.spinner import Spinner
from rich.live import Live
from rich.columns import Columns
from rich.markdown import Markdown
from rich.layout import Layout
from rich.text import Text
from rich.table import Table
from collections import deque
import time
from rich.tree import Tree
from rich import box
from rich.align import Align
from rich.rule import Rule

from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.default_config import DEFAULT_CONFIG
from cli.models import AnalystType
from cli.utils import *
from cli.announcements import fetch_announcements, display_announcements
from cli.stats_handler import StatsCallbackHandler

console = Console()

app = typer.Typer(
    name="TradingAgents",
    help="TradingAgents CLI: Multi-Agents LLM Financial Trading Framework",
    add_completion=True,  # Enable shell completion
)


# Create a deque to store recent messages with a maximum length
class MessageBuffer:
    # Fixed teams that always run (not user-selectable)
    FIXED_AGENTS = {
        "Research Team": ["Bull Researcher", "Bear Researcher", "Research Manager"],
        "Trading Team": ["Trader"],
        "Risk Management": ["Aggressive Analyst", "Neutral Analyst", "Conservative Analyst"],
        "Portfolio Management": ["Portfolio Manager"],
    }

    # Analyst name mapping
    ANALYST_MAPPING = {
        "market": "Market Analyst",
        "social": "Social Analyst",
        "news": "News Analyst",
        "fundamentals": "Fundamentals Analyst",
    }

    # Report section mapping: section -> (analyst_key for filtering, finalizing_agent)
    # analyst_key: which analyst selection controls this section (None = always included)
    # finalizing_agent: which agent must be "completed" for this report to count as done
    REPORT_SECTIONS = {
        "market_report": ("market", "Market Analyst"),
        "sentiment_report": ("social", "Social Analyst"),
        "news_report": ("news", "News Analyst"),
        "fundamentals_report": ("fundamentals", "Fundamentals Analyst"),
        "investment_plan": (None, "Research Manager"),
        "trader_investment_plan": (None, "Trader"),
        "final_trade_decision": (None, "Portfolio Manager"),
    }

    def __init__(self, max_length=100):
        self.messages = deque(maxlen=max_length)
        self.tool_calls = deque(maxlen=max_length)
        self.current_report = None
        self.final_report = None  # Store the complete final report
        self.agent_status = {}
        self.current_agent = None
        self.report_sections = {}
        self.selected_analysts = []
        self._last_message_id = None

    def init_for_analysis(self, selected_analysts):
        """Initialize agent status and report sections based on selected analysts.

        Args:
            selected_analysts: List of analyst type strings (e.g., ["market", "news"])
        """
        self.selected_analysts = [a.lower() for a in selected_analysts]

        # Build agent_status dynamically
        self.agent_status = {}

        # Add selected analysts
        for analyst_key in self.selected_analysts:
            if analyst_key in self.ANALYST_MAPPING:
                self.agent_status[self.ANALYST_MAPPING[analyst_key]] = "pending"

        # Add fixed teams
        for team_agents in self.FIXED_AGENTS.values():
            for agent in team_agents:
                self.agent_status[agent] = "pending"

        # Build report_sections dynamically
        self.report_sections = {}
        for section, (analyst_key, _) in self.REPORT_SECTIONS.items():
            if analyst_key is None or analyst_key in self.selected_analysts:
                self.report_sections[section] = None

        # Reset other state
        self.current_report = None
        self.final_report = None
        self.current_agent = None
        self.messages.clear()
        self.tool_calls.clear()
        self._last_message_id = None

    def get_completed_reports_count(self):
        """Count reports that are finalized (their finalizing agent is completed).

        A report is considered complete when:
        1. The report section has content (not None), AND
        2. The agent responsible for finalizing that report has status "completed"

        This prevents interim updates (like debate rounds) from counting as completed.
        """
        count = 0
        for section in self.report_sections:
            if section not in self.REPORT_SECTIONS:
                continue
            _, finalizing_agent = self.REPORT_SECTIONS[section]
            # Report is complete if it has content AND its finalizing agent is done
            has_content = self.report_sections.get(section) is not None
            agent_done = self.agent_status.get(finalizing_agent) == "completed"
            if has_content and agent_done:
                count += 1
        return count

    def add_message(self, message_type, content):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        self.messages.append((timestamp, message_type, content))

    def add_tool_call(self, tool_name, args):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        self.tool_calls.append((timestamp, tool_name, args))

    def update_agent_status(self, agent, status):
        if agent in self.agent_status:
            self.agent_status[agent] = status
            self.current_agent = agent

    def update_report_section(self, section_name, content):
        if section_name in self.report_sections:
            normalized_content = extract_content_string(content)
            self.report_sections[section_name] = normalized_content
            self._update_current_report()

    def _update_current_report(self):
        # For the panel display, only show the most recently updated section
        latest_section = None
        latest_content = None

        # Find the most recently updated section
        for section, content in self.report_sections.items():
            if content is not None:
                latest_section = section
                latest_content = content
               
        if latest_section and latest_content:
            # Format the current section for display
            section_titles = {
                "market_report": "Market Analysis",
                "sentiment_report": "Social Sentiment",
                "news_report": "News Analysis",
                "fundamentals_report": "Fundamentals Analysis",
                "investment_plan": "Research Team Decision",
                "trader_investment_plan": "Trading Team Plan",
                "final_trade_decision": "Portfolio Management Decision",
            }
            self.current_report = (
                f"### {section_titles[latest_section]}\n{latest_content}"
            )

        # Update the final complete report
        self._update_final_report()

    def _update_final_report(self):
        report_parts = []

        # Analyst Team Reports - use .get() to handle missing sections
        analyst_sections = ["market_report", "sentiment_report", "news_report", "fundamentals_report"]
        if any(self.report_sections.get(section) for section in analyst_sections):
            report_parts.append("## Analyst Team Reports")
            if self.report_sections.get("market_report"):
                report_parts.append(
                    f"### Market Analysis\n{self.report_sections['market_report']}"
                )
            if self.report_sections.get("sentiment_report"):
                report_parts.append(
                    f"### Social Sentiment\n{self.report_sections['sentiment_report']}"
                )
            if self.report_sections.get("news_report"):
                report_parts.append(
                    f"### News Analysis\n{self.report_sections['news_report']}"
                )
            if self.report_sections.get("fundamentals_report"):
                report_parts.append(
                    f"### Fundamentals Analysis\n{self.report_sections['fundamentals_report']}"
                )

        # Research Team Reports
        if self.report_sections.get("investment_plan"):
            report_parts.append("## Research Team Decision")
            report_parts.append(f"{self.report_sections['investment_plan']}")

        # Trading Team Reports
        if self.report_sections.get("trader_investment_plan"):
            report_parts.append("## Trading Team Plan")
            report_parts.append(f"{self.report_sections['trader_investment_plan']}")

        # Portfolio Management Decision
        if self.report_sections.get("final_trade_decision"):
            report_parts.append("## Portfolio Management Decision")
            report_parts.append(f"{self.report_sections['final_trade_decision']}")

        self.final_report = "\n\n".join(report_parts) if report_parts else None


message_buffer = MessageBuffer()


def create_layout():
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="main"),
        Layout(name="footer", size=3),
    )
    layout["main"].split_column(
        Layout(name="upper", ratio=3), Layout(name="analysis", ratio=5)
    )
    layout["upper"].split_row(
        Layout(name="progress", ratio=2), Layout(name="messages", ratio=3)
    )
    return layout


def format_tokens(n):
    """Format token count for display."""
    if n >= 1000:
        return f"{n/1000:.1f}k"
    return str(n)


def update_display(layout, spinner_text=None, stats_handler=None, start_time=None):
    # Header with welcome message
    layout["header"].update(
        Panel(
            "[bold green]Welcome to TradingAgents CLI[/bold green]\n"
            "[dim]© [Tauric Research](https://github.com/TauricResearch)[/dim]",
            title="Welcome to TradingAgents",
            border_style="green",
            padding=(1, 2),
            expand=True,
        )
    )

    # Progress panel showing agent status
    progress_table = Table(
        show_header=True,
        header_style="bold magenta",
        show_footer=False,
        box=box.SIMPLE_HEAD,  # Use simple header with horizontal lines
        title=None,  # Remove the redundant Progress title
        padding=(0, 2),  # Add horizontal padding
        expand=True,  # Make table expand to fill available space
    )
    progress_table.add_column("Team", style="cyan", justify="center", width=20)
    progress_table.add_column("Agent", style="green", justify="center", width=20)
    progress_table.add_column("Status", style="yellow", justify="center", width=20)

    # Group agents by team - filter to only include agents in agent_status
    all_teams = {
        "Analyst Team": [
            "Market Analyst",
            "Social Analyst",
            "News Analyst",
            "Fundamentals Analyst",
        ],
        "Research Team": ["Bull Researcher", "Bear Researcher", "Research Manager"],
        "Trading Team": ["Trader"],
        "Risk Management": ["Aggressive Analyst", "Neutral Analyst", "Conservative Analyst"],
        "Portfolio Management": ["Portfolio Manager"],
    }

    # Filter teams to only include agents that are in agent_status
    teams = {}
    for team, agents in all_teams.items():
        active_agents = [a for a in agents if a in message_buffer.agent_status]
        if active_agents:
            teams[team] = active_agents

    for team, agents in teams.items():
        # Add first agent with team name
        first_agent = agents[0]
        status = message_buffer.agent_status.get(first_agent, "pending")
        if status == "in_progress":
            spinner = Spinner(
                "dots", text="[blue]in_progress[/blue]", style="bold cyan"
            )
            status_cell = spinner
        else:
            status_color = {
                "pending": "yellow",
                "completed": "green",
                "error": "red",
            }.get(status, "white")
            status_cell = f"[{status_color}]{status}[/{status_color}]"
        progress_table.add_row(team, first_agent, status_cell)

        # Add remaining agents in team
        for agent in agents[1:]:
            status = message_buffer.agent_status.get(agent, "pending")
            if status == "in_progress":
                spinner = Spinner(
                    "dots", text="[blue]in_progress[/blue]", style="bold cyan"
                )
                status_cell = spinner
            else:
                status_color = {
                    "pending": "yellow",
                    "completed": "green",
                    "error": "red",
                }.get(status, "white")
                status_cell = f"[{status_color}]{status}[/{status_color}]"
            progress_table.add_row("", agent, status_cell)

        # Add horizontal line after each team
        progress_table.add_row("─" * 20, "─" * 20, "─" * 20, style="dim")

    layout["progress"].update(
        Panel(progress_table, title="Progress", border_style="cyan", padding=(1, 2))
    )

    # Messages panel showing recent messages and tool calls
    messages_table = Table(
        show_header=True,
        header_style="bold magenta",
        show_footer=False,
        expand=True,  # Make table expand to fill available space
        box=box.MINIMAL,  # Use minimal box style for a lighter look
        show_lines=True,  # Keep horizontal lines
        padding=(0, 1),  # Add some padding between columns
    )
    messages_table.add_column("Time", style="cyan", width=8, justify="center")
    messages_table.add_column("Type", style="green", width=10, justify="center")
    messages_table.add_column(
        "Content", style="white", no_wrap=False, ratio=1
    )  # Make content column expand

    # Combine tool calls and messages
    all_messages = []

    # Add tool calls
    for timestamp, tool_name, args in message_buffer.tool_calls:
        formatted_args = format_tool_args(args)
        all_messages.append((timestamp, "Tool", f"{tool_name}: {formatted_args}"))

    # Add regular messages
    for timestamp, msg_type, content in message_buffer.messages:
        content_str = str(content) if content else ""
        if len(content_str) > 200:
            content_str = content_str[:197] + "..."
        all_messages.append((timestamp, msg_type, content_str))

    # Sort by timestamp descending (newest first)
    all_messages.sort(key=lambda x: x[0], reverse=True)

    # Calculate how many messages we can show based on available space
    max_messages = 12

    # Get the first N messages (newest ones)
    recent_messages = all_messages[:max_messages]

    # Add messages to table (already in newest-first order)
    for timestamp, msg_type, content in recent_messages:
        # Format content with word wrapping
        wrapped_content = Text(content, overflow="fold")
        messages_table.add_row(timestamp, msg_type, wrapped_content)

    layout["messages"].update(
        Panel(
            messages_table,
            title="Messages & Tools",
            border_style="blue",
            padding=(1, 2),
        )
    )

    # Analysis panel showing current report
    if message_buffer.current_report:
        layout["analysis"].update(
            Panel(
                Markdown(message_buffer.current_report),
                title="Current Report",
                border_style="green",
                padding=(1, 2),
            )
        )
    else:
        layout["analysis"].update(
            Panel(
                "[italic]Waiting for analysis report...[/italic]",
                title="Current Report",
                border_style="green",
                padding=(1, 2),
            )
        )

    # Footer with statistics
    # Agent progress - derived from agent_status dict
    agents_completed = sum(
        1 for status in message_buffer.agent_status.values() if status == "completed"
    )
    agents_total = len(message_buffer.agent_status)

    # Report progress - based on agent completion (not just content existence)
    reports_completed = message_buffer.get_completed_reports_count()
    reports_total = len(message_buffer.report_sections)

    # Build stats parts
    stats_parts = [f"Agents: {agents_completed}/{agents_total}"]

    # LLM and tool stats from callback handler
    if stats_handler:
        stats = stats_handler.get_stats()
        stats_parts.append(f"LLM: {stats['llm_calls']}")
        stats_parts.append(f"Tools: {stats['tool_calls']}")

        # Token display with graceful fallback
        if stats["tokens_in"] > 0 or stats["tokens_out"] > 0:
            tokens_str = f"Tokens: {format_tokens(stats['tokens_in'])}\u2191 {format_tokens(stats['tokens_out'])}\u2193"
        else:
            tokens_str = "Tokens: --"
        stats_parts.append(tokens_str)

    stats_parts.append(f"Reports: {reports_completed}/{reports_total}")

    # Elapsed time
    if start_time:
        elapsed = time.time() - start_time
        elapsed_str = f"\u23f1 {int(elapsed // 60):02d}:{int(elapsed % 60):02d}"
        stats_parts.append(elapsed_str)

    stats_table = Table(show_header=False, box=None, padding=(0, 2), expand=True)
    stats_table.add_column("Stats", justify="center")
    stats_table.add_row(" | ".join(stats_parts))

    layout["footer"].update(Panel(stats_table, border_style="grey50"))


def get_user_selections():
    """Get all user selections before starting the analysis display."""
    # Display ASCII art welcome message
    with open(Path(__file__).parent / "static" / "welcome.txt", "r") as f:
        welcome_ascii = f.read()

    # Create welcome box content
    welcome_content = f"{welcome_ascii}\n"
    welcome_content += "[bold green]TradingAgents: Multi-Agents LLM Financial Trading Framework - CLI[/bold green]\n\n"
    welcome_content += "[bold]Workflow Steps:[/bold]\n"
    welcome_content += "I. Analyst Team → II. Research Team → III. Trader → IV. Risk Management → V. Portfolio Management\n\n"
    welcome_content += (
        "[dim]Built by [Tauric Research](https://github.com/TauricResearch)[/dim]"
    )

    # Create and center the welcome box
    welcome_box = Panel(
        welcome_content,
        border_style="green",
        padding=(1, 2),
        title="Welcome to TradingAgents",
        subtitle="Multi-Agents LLM Financial Trading Framework",
    )
    console.print(Align.center(welcome_box))
    console.print()
    console.print()  # Add vertical space before announcements

    # Fetch and display announcements (silent on failure)
    announcements = fetch_announcements()
    display_announcements(console, announcements)

    # Create a boxed questionnaire for each step
    def create_question_box(title, prompt, default=None):
        box_content = f"[bold]{title}[/bold]\n"
        box_content += f"[dim]{prompt}[/dim]"
        if default:
            box_content += f"\n[dim]Default: {default}[/dim]"
        return Panel(box_content, border_style="blue", padding=(1, 2))

    # Step 1: Ticker symbols
    console.print(
        create_question_box(
            "Step 1: Ticker Symbols",
            "Enter one or more ticker symbols to analyze, preserving any exchange suffix when needed (examples: SPY, CNC.TO, 7203.T, 0700.HK)",
            "SPY",
        )
    )
    selected_tickers = get_tickers()

    # Step 2: Analysis date
    default_date = datetime.datetime.now().strftime("%Y-%m-%d")
    console.print(
        create_question_box(
            "Step 2: Analysis Date",
            "Enter the analysis date (YYYY-MM-DD)",
            default_date,
        )
    )
    analysis_date = get_analysis_date()

    # Step 3: Output language
    console.print(
        create_question_box(
            "Step 3: Output Language",
            "Select the language for analyst reports and final decision"
        )
    )
    output_language = ask_output_language()

    # Step 4: Select analysts
    console.print(
        create_question_box(
            "Step 4: Analysts Team", "Select your LLM analyst agents for the analysis"
        )
    )
    selected_analysts = select_analysts()
    console.print(
        f"[green]Selected analysts:[/green] {', '.join(analyst.value for analyst in selected_analysts)}"
    )

    # Step 5: Research depth
    console.print(
        create_question_box(
            "Step 5: Research Depth", "Select your research depth level"
        )
    )
    selected_research_depth = select_research_depth()

    # Step 6: LLM Provider
    console.print(
        create_question_box(
            "Step 6: LLM Provider", "Select your LLM provider"
        )
    )
    selected_llm_provider, backend_url = select_llm_provider()

    # Step 7: Thinking agents
    console.print(
        create_question_box(
            "Step 7: Thinking Agents", "Select your thinking agents for analysis"
        )
    )
    selected_shallow_thinker = select_shallow_thinking_agent(selected_llm_provider)
    selected_deep_thinker = select_deep_thinking_agent(selected_llm_provider)

    # Step 8: Provider-specific thinking configuration
    thinking_level = None
    reasoning_effort = None
    anthropic_effort = None

    provider_lower = selected_llm_provider.lower()
    if provider_lower == "google":
        console.print(
            create_question_box(
                "Step 8: Thinking Mode",
                "Configure Gemini thinking mode"
            )
        )
        thinking_level = ask_gemini_thinking_config()
    elif provider_lower == "openai":
        console.print(
            create_question_box(
                "Step 8: Reasoning Effort",
                "Configure OpenAI reasoning effort level"
            )
        )
        reasoning_effort = ask_openai_reasoning_effort()
    elif provider_lower == "anthropic":
        console.print(
            create_question_box(
                "Step 8: Effort Level",
                "Configure Claude effort level"
            )
        )
        anthropic_effort = ask_anthropic_effort()

    return {
        "ticker": selected_tickers[0],
        "tickers": selected_tickers,
        "analysis_date": analysis_date,
        "analysts": selected_analysts,
        "research_depth": selected_research_depth,
        "llm_provider": selected_llm_provider.lower(),
        "backend_url": backend_url,
        "shallow_thinker": selected_shallow_thinker,
        "deep_thinker": selected_deep_thinker,
        "google_thinking_level": thinking_level,
        "openai_reasoning_effort": reasoning_effort,
        "anthropic_effort": anthropic_effort,
        "output_language": output_language,
    }


def parse_tickers(raw_value: str) -> list[str]:
    """Parse a ticker input string into a normalized unique list."""
    seen = set()
    tickers = []

    for token in re.split(r"[\s,]+", raw_value.strip()):
        ticker = token.strip().upper()
        if ticker and ticker not in seen:
            seen.add(ticker)
            tickers.append(ticker)

    return tickers


def get_tickers():
    """Get one or more ticker symbols from user input."""
    while True:
        raw_value = typer.prompt("", default="SPY")
        tickers = parse_tickers(raw_value)
        if tickers:
            return tickers
        console.print(
            "[red]Error: Please enter at least one ticker symbol[/red]"
        )


def get_analysis_date():
    """Get the analysis date from user input."""
    while True:
        date_str = typer.prompt(
            "", default=datetime.datetime.now().strftime("%Y-%m-%d")
        )
        try:
            # Validate date format and ensure it's not in the future
            analysis_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
            if analysis_date.date() > datetime.datetime.now().date():
                console.print("[red]Error: Analysis date cannot be in the future[/red]")
                continue
            return date_str
        except ValueError:
            console.print(
                "[red]Error: Invalid date format. Please use YYYY-MM-DD[/red]"
            )


def get_save_preferences(selections):
    """Ask export/save preferences before analysis starts."""
    save_choice = typer.prompt("Save report?", default="Y").strip().upper()
    save_enabled = save_choice in ("Y", "YES", "")
    save_path = None

    if save_enabled:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        tickers = selections["tickers"]
        default_path = (
            Path.cwd() / "reports" / f"{tickers[0]}_{timestamp}"
            if len(tickers) == 1
            else Path.cwd() / "reports" / f"batch_{selections['analysis_date']}_{timestamp}"
        )
        save_path_str = typer.prompt(
            "Save path (press Enter for default)",
            default=str(default_path),
        ).strip()
        save_path = Path(save_path_str)

    return {
        "save_enabled": save_enabled,
        "save_path": save_path,
    }


def format_price_target(price_target) -> str:
    """Format price target for display."""
    if price_target is None:
        return "-"
    return f"${price_target:,.2f}"


def parse_json_response(raw_content: str) -> dict | None:
    """Extract a JSON object from model output."""
    if not raw_content:
        return None

    content = raw_content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```(?:json)?\s*", "", content)
        content = re.sub(r"\s*```$", "", content)

    try:
        parsed = json.loads(content)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", content, re.DOTALL)
    if not match:
        return None

    try:
        parsed = json.loads(match.group(0))
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        return None


def fetch_reference_price(ticker: str, analysis_date: str) -> float | None:
    """Fetch the latest close near the analysis date for target estimation."""
    try:
        import yfinance as yf

        end_date = datetime.datetime.strptime(analysis_date, "%Y-%m-%d") + datetime.timedelta(days=1)
        start_date = end_date - datetime.timedelta(days=30)
        history = yf.Ticker(ticker).history(
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            auto_adjust=False,
        )
        if history.empty:
            return None
        close_values = history["Close"].dropna()
        if close_values.empty:
            return None
        return round(float(close_values.iloc[-1]), 2)
    except Exception:
        return None


def estimate_target_profile(llm, ticker: str, analysis_date: str, final_state, decision: str):
    """Estimate a per-ticker target price and confidence score."""
    current_price = fetch_reference_price(ticker, analysis_date)
    decision_text = compact_report_text(final_state.get("final_trade_decision"), max_chars=1400)
    market_text = compact_report_text(final_state.get("market_report"), max_chars=1200)
    social_text = compact_report_text(final_state.get("sentiment_report"), max_chars=900)
    news_text = compact_report_text(final_state.get("news_report"), max_chars=900)
    fundamentals_text = compact_report_text(final_state.get("fundamentals_report"), max_chars=1000)
    trader_text = compact_report_text(final_state.get("trader_investment_plan"), max_chars=900)

    messages = [
        (
            "system",
            "You summarize trading analysis. Return JSON only with keys "
            'price_target, confidence_score, horizon, summary. '
            "price_target must be a number in USD or null. "
            "confidence_score must be an integer from 0 to 100 representing the probability "
            "the stock reaches or exceeds the target within the stated horizon. "
            "Use a realistic single target, not a range. Keep summary under 80 words.",
        ),
        (
            "human",
            f"""Ticker: {ticker}
Analysis date: {analysis_date}
Current reference price: {current_price if current_price is not None else 'unknown'}
Decision: {decision}

Portfolio decision summary:
{decision_text}

Market summary:
{market_text}

Social summary:
{social_text}

News summary:
{news_text}

Fundamentals summary:
{fundamentals_text}

Trader plan summary:
{trader_text}

Return strict JSON only.""",
        ),
    ]

    try:
        response = llm.invoke(messages)
        payload = parse_json_response(extract_content_string(response.content) or "")
    except Exception:
        payload = None

    price_target = None
    confidence_score = 50
    horizon = "12 months"
    summary = "Target estimate unavailable."

    if payload:
        raw_target = payload.get("price_target")
        if isinstance(raw_target, (int, float)):
            price_target = round(float(raw_target), 2)
        elif isinstance(raw_target, str):
            match = re.search(r"-?\d+(?:\.\d+)?", raw_target.replace(",", ""))
            if match:
                price_target = round(float(match.group(0)), 2)

        raw_confidence = payload.get("confidence_score")
        if isinstance(raw_confidence, (int, float)):
            confidence_score = int(round(float(raw_confidence)))
        elif isinstance(raw_confidence, str):
            match = re.search(r"\d+", raw_confidence)
            if match:
                confidence_score = int(match.group(0))

        horizon = str(payload.get("horizon") or horizon).strip()
        summary = str(payload.get("summary") or summary).strip()

    confidence_score = max(0, min(100, confidence_score))

    if price_target is None:
        price_target = current_price

    return {
        "reference_price": current_price,
        "price_target": price_target,
        "confidence_score": confidence_score,
        "target_horizon": horizon,
        "target_summary": summary,
    }


def save_report_to_disk(final_state, ticker: str, save_path: Path):
    """Save complete analysis report to disk with organized subfolders."""
    save_path.mkdir(parents=True, exist_ok=True)
    sections = []
    market_report = extract_content_string(final_state.get("market_report")) or ""
    sentiment_report = extract_content_string(final_state.get("sentiment_report")) or ""
    news_report = extract_content_string(final_state.get("news_report")) or ""
    fundamentals_report = extract_content_string(final_state.get("fundamentals_report")) or ""
    trader_plan = extract_content_string(final_state.get("trader_investment_plan")) or ""

    # 1. Analysts
    analysts_dir = save_path / "1_analysts"
    analyst_parts = []
    if market_report:
        analysts_dir.mkdir(exist_ok=True)
        (analysts_dir / "market.md").write_text(market_report)
        analyst_parts.append(("Market Analyst", market_report))
    if sentiment_report:
        analysts_dir.mkdir(exist_ok=True)
        (analysts_dir / "sentiment.md").write_text(sentiment_report)
        analyst_parts.append(("Social Analyst", sentiment_report))
    if news_report:
        analysts_dir.mkdir(exist_ok=True)
        (analysts_dir / "news.md").write_text(news_report)
        analyst_parts.append(("News Analyst", news_report))
    if fundamentals_report:
        analysts_dir.mkdir(exist_ok=True)
        (analysts_dir / "fundamentals.md").write_text(fundamentals_report)
        analyst_parts.append(("Fundamentals Analyst", fundamentals_report))
    if analyst_parts:
        content = "\n\n".join(f"### {name}\n{text}" for name, text in analyst_parts)
        sections.append(f"## I. Analyst Team Reports\n\n{content}")

    # 2. Research
    if final_state.get("investment_debate_state"):
        research_dir = save_path / "2_research"
        debate = final_state["investment_debate_state"]
        bull_history = extract_content_string(debate.get("bull_history")) or ""
        bear_history = extract_content_string(debate.get("bear_history")) or ""
        judge_decision = extract_content_string(debate.get("judge_decision")) or ""
        research_parts = []
        if bull_history:
            research_dir.mkdir(exist_ok=True)
            (research_dir / "bull.md").write_text(bull_history)
            research_parts.append(("Bull Researcher", bull_history))
        if bear_history:
            research_dir.mkdir(exist_ok=True)
            (research_dir / "bear.md").write_text(bear_history)
            research_parts.append(("Bear Researcher", bear_history))
        if judge_decision:
            research_dir.mkdir(exist_ok=True)
            (research_dir / "manager.md").write_text(judge_decision)
            research_parts.append(("Research Manager", judge_decision))
        if research_parts:
            content = "\n\n".join(f"### {name}\n{text}" for name, text in research_parts)
            sections.append(f"## II. Research Team Decision\n\n{content}")

    # 3. Trading
    if trader_plan:
        trading_dir = save_path / "3_trading"
        trading_dir.mkdir(exist_ok=True)
        (trading_dir / "trader.md").write_text(trader_plan)
        sections.append(f"## III. Trading Team Plan\n\n### Trader\n{trader_plan}")

    # 4. Risk Management
    if final_state.get("risk_debate_state"):
        risk_dir = save_path / "4_risk"
        risk = final_state["risk_debate_state"]
        aggressive_history = extract_content_string(risk.get("aggressive_history")) or ""
        conservative_history = extract_content_string(risk.get("conservative_history")) or ""
        neutral_history = extract_content_string(risk.get("neutral_history")) or ""
        judge_decision = extract_content_string(risk.get("judge_decision")) or ""
        risk_parts = []
        if aggressive_history:
            risk_dir.mkdir(exist_ok=True)
            (risk_dir / "aggressive.md").write_text(aggressive_history)
            risk_parts.append(("Aggressive Analyst", aggressive_history))
        if conservative_history:
            risk_dir.mkdir(exist_ok=True)
            (risk_dir / "conservative.md").write_text(conservative_history)
            risk_parts.append(("Conservative Analyst", conservative_history))
        if neutral_history:
            risk_dir.mkdir(exist_ok=True)
            (risk_dir / "neutral.md").write_text(neutral_history)
            risk_parts.append(("Neutral Analyst", neutral_history))
        if risk_parts:
            content = "\n\n".join(f"### {name}\n{text}" for name, text in risk_parts)
            sections.append(f"## IV. Risk Management Team Decision\n\n{content}")

        # 5. Portfolio Manager
        if judge_decision:
            portfolio_dir = save_path / "5_portfolio"
            portfolio_dir.mkdir(exist_ok=True)
            (portfolio_dir / "decision.md").write_text(judge_decision)
            sections.append(f"## V. Portfolio Manager Decision\n\n### Portfolio Manager\n{judge_decision}")

    # Write consolidated report
    header = f"# Trading Analysis Report: {ticker}\n\nGenerated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    (save_path / "complete_report.md").write_text(header + "\n\n".join(sections))
    return save_path / "complete_report.md"


def display_complete_report(final_state):
    """Display the complete analysis report sequentially (avoids truncation)."""
    console.print()
    console.print(Rule("Complete Analysis Report", style="bold green"))
    market_report = extract_content_string(final_state.get("market_report")) or ""
    sentiment_report = extract_content_string(final_state.get("sentiment_report")) or ""
    news_report = extract_content_string(final_state.get("news_report")) or ""
    fundamentals_report = extract_content_string(final_state.get("fundamentals_report")) or ""
    trader_plan = extract_content_string(final_state.get("trader_investment_plan")) or ""

    # I. Analyst Team Reports
    analysts = []
    if market_report:
        analysts.append(("Market Analyst", market_report))
    if sentiment_report:
        analysts.append(("Social Analyst", sentiment_report))
    if news_report:
        analysts.append(("News Analyst", news_report))
    if fundamentals_report:
        analysts.append(("Fundamentals Analyst", fundamentals_report))
    if analysts:
        console.print(Panel("[bold]I. Analyst Team Reports[/bold]", border_style="cyan"))
        for title, content in analysts:
            console.print(Panel(Markdown(content), title=title, border_style="blue", padding=(1, 2)))

    # II. Research Team Reports
    if final_state.get("investment_debate_state"):
        debate = final_state["investment_debate_state"]
        bull_history = extract_content_string(debate.get("bull_history")) or ""
        bear_history = extract_content_string(debate.get("bear_history")) or ""
        judge_decision = extract_content_string(debate.get("judge_decision")) or ""
        research = []
        if bull_history:
            research.append(("Bull Researcher", bull_history))
        if bear_history:
            research.append(("Bear Researcher", bear_history))
        if judge_decision:
            research.append(("Research Manager", judge_decision))
        if research:
            console.print(Panel("[bold]II. Research Team Decision[/bold]", border_style="magenta"))
            for title, content in research:
                console.print(Panel(Markdown(content), title=title, border_style="blue", padding=(1, 2)))

    # III. Trading Team
    if trader_plan:
        console.print(Panel("[bold]III. Trading Team Plan[/bold]", border_style="yellow"))
        console.print(Panel(Markdown(trader_plan), title="Trader", border_style="blue", padding=(1, 2)))

    # IV. Risk Management Team
    if final_state.get("risk_debate_state"):
        risk = final_state["risk_debate_state"]
        aggressive_history = extract_content_string(risk.get("aggressive_history")) or ""
        conservative_history = extract_content_string(risk.get("conservative_history")) or ""
        neutral_history = extract_content_string(risk.get("neutral_history")) or ""
        judge_decision = extract_content_string(risk.get("judge_decision")) or ""
        risk_reports = []
        if aggressive_history:
            risk_reports.append(("Aggressive Analyst", aggressive_history))
        if conservative_history:
            risk_reports.append(("Conservative Analyst", conservative_history))
        if neutral_history:
            risk_reports.append(("Neutral Analyst", neutral_history))
        if risk_reports:
            console.print(Panel("[bold]IV. Risk Management Team Decision[/bold]", border_style="red"))
            for title, content in risk_reports:
                console.print(Panel(Markdown(content), title=title, border_style="blue", padding=(1, 2)))

        # V. Portfolio Manager Decision
        if judge_decision:
            console.print(Panel("[bold]V. Portfolio Manager Decision[/bold]", border_style="green"))
            console.print(Panel(Markdown(judge_decision), title="Portfolio Manager", border_style="blue", padding=(1, 2)))


def compact_report_text(content, max_chars=280):
    """Condense markdown-heavy report text into a short readable excerpt."""
    text = extract_content_string(content) or ""
    if not text:
        return ""

    cleaned_lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("|"):
            continue
        if set(stripped) <= {"-", ":", "|", " "}:
            continue
        stripped = stripped.lstrip("#").strip()
        cleaned_lines.append(stripped)

    cleaned = " ".join(cleaned_lines) if cleaned_lines else text
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) <= max_chars:
        return cleaned

    truncated = cleaned[:max_chars].rsplit(" ", 1)[0].strip()
    return f"{truncated}..." if truncated else f"{cleaned[:max_chars]}..."


def format_target_gap_percent(reference_price, price_target) -> str:
    """Format target gap as a percentage versus the reference price."""
    if reference_price in (None, 0) or price_target is None:
        return "-"
    gap_pct = ((price_target - reference_price) / reference_price) * 100.0
    return f"{gap_pct:+.2f}%"


def full_report_text(content, fallback: str) -> str:
    """Return the full extracted report text with no truncation."""
    text = extract_content_string(content)
    return text if text else fallback


REPORT_CHATTER_PATTERNS = (
    "if you want, i can",
    "if you want, i will",
    "if you want to",
    "which follow-up would you like",
    "which follow up would you like",
    "would you like me to",
    "if you'd like",
    "if you’d like",
    "let me know if you want",
)


def sanitize_report_language(content) -> str:
    """Remove interactive assistant phrasing so text reads like a report."""
    text = extract_content_string(content) or str(content or "")
    if not text:
        return ""

    filtered_lines = []
    for line in text.splitlines():
        lowered = line.strip().lower()
        if any(pattern in lowered for pattern in REPORT_CHATTER_PATTERNS):
            continue
        filtered_lines.append(line)

    cleaned = "\n".join(filtered_lines).strip()
    return re.sub(r"\n{3,}", "\n\n", cleaned)


def fallback_bullet_summary(content, max_bullets: int = 5) -> str:
    """Build a concise markdown bullet summary when the LLM is unavailable."""
    cleaned = sanitize_report_language(content)
    if not cleaned:
        return "- No report generated."

    sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9(])", cleaned.replace("\n", " "))
    bullets = []
    for sentence in sentences:
        line = sentence.strip().lstrip("#").strip()
        if not line:
            continue
        if len(line) > 260:
            line = line[:257].rsplit(" ", 1)[0].strip() + "..."
        bullets.append(f"- {line}")
        if len(bullets) >= max_bullets:
            break

    return "\n".join(bullets) if bullets else "- No report generated."


def summarize_consolidated_section(llm, ticker: str, section_name: str, content) -> str:
    """Summarize a full section into report-ready markdown bullets."""
    cleaned = sanitize_report_language(content)
    if not cleaned:
        return "- No report generated."

    if llm is None:
        return fallback_bullet_summary(cleaned)

    messages = [
        (
            "system",
            "You rewrite trading analysis into finished report bullets. "
            "Return markdown bullets only. Use 4 to 6 bullets. "
            "Each bullet must be a complete report-ready statement. "
            "Do not ask the reader questions. Do not offer follow-up work. "
            "Do not use phrases like 'If you want', 'Would you like', or 'I can now'.",
        ),
        (
            "human",
            f"""Ticker: {ticker}
Section: {section_name}

Source analysis:
{cleaned}

Return only markdown bullet points.""",
        ),
    ]

    try:
        response = llm.invoke(messages)
        summary = sanitize_report_language(extract_content_string(response.content) or "")
        bullet_lines = [
            line.strip()
            for line in summary.splitlines()
            if line.strip().startswith(("-", "*"))
        ]
        if bullet_lines:
            return "\n".join(bullet_lines)
    except Exception:
        pass

    return fallback_bullet_summary(cleaned)


def get_consolidated_section_summaries(result, summary_llm=None) -> dict[str, str]:
    """Cache per-result section summaries for consolidated report output."""
    cached = result.get("_consolidated_section_summaries")
    if isinstance(cached, dict):
        return cached

    final_state = result["final_state"]
    section_map = {
        "Portfolio Management Decision": final_state.get("final_trade_decision"),
        "Market": final_state.get("market_report"),
        "Social": final_state.get("sentiment_report"),
        "News": final_state.get("news_report"),
        "Fundamentals": final_state.get("fundamentals_report"),
    }
    summaries = {
        name: summarize_consolidated_section(summary_llm, result["ticker"], name, content)
        for name, content in section_map.items()
    }
    result["_consolidated_section_summaries"] = summaries
    return summaries


def bullet_markdown_to_html(summary: str) -> str:
    """Convert simple markdown bullets into HTML list markup."""
    bullets = []
    for line in summary.splitlines():
        stripped = line.strip()
        if stripped.startswith(("- ", "* ")):
            bullets.append(f"<li>{escape(stripped[2:].strip())}</li>")
    if not bullets:
        fallback = sanitize_report_language(summary) or "No report generated."
        bullets.append(f"<li>{escape(fallback)}</li>")
    return "<ul>" + "".join(bullets) + "</ul>"


def build_consolidated_report(analysis_results, analysis_date: str, summary_llm=None) -> str:
    """Build a consolidated markdown report for a batch of tickers."""
    lines = [
        "# Consolidated Trading Analysis Report",
        "",
        f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Analysis Date: {analysis_date}",
        "",
        "## Batch Summary",
        "",
        "| Ticker | Decision | Price Target | Target Gap | Confidence | Status | Default Results |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]

    for result in analysis_results:
        status = "Failed" if result.get("error") else "Completed"
        decision = result.get("decision") or "-"
        price_target = format_price_target(result.get("price_target"))
        target_gap = format_target_gap_percent(
            result.get("reference_price"),
            result.get("price_target"),
        )
        confidence = (
            f"{result['confidence_score']}/100"
            if result.get("confidence_score") is not None
            else "-"
        )
        default_results = result.get("results_dir", "-")
        lines.append(
            f"| {result['ticker']} | {decision} | {price_target} | {target_gap} | {confidence} | {status} | `{default_results}` |"
        )

    for result in analysis_results:
        lines.extend(
            [
                "",
                "---",
                "",
                f"## {result['ticker']}",
                "",
                f"Analysis Date: {result['analysis_date']}",
                f"Average Price Target: {format_price_target(result.get('price_target'))}",
                f"Reference Price: {format_price_target(result.get('reference_price'))}",
                f"Target Gap: {format_target_gap_percent(result.get('reference_price'), result.get('price_target'))}",
                f"Confidence: {result.get('confidence_score', '-')}/100"
                if result.get("confidence_score") is not None
                else "Confidence: -",
                f"Horizon: {result.get('target_horizon') or '-'}",
            ]
        )

        if result.get("error"):
            lines.extend(
                [
                    "",
                    "### Status",
                    "Analysis failed.",
                    "",
                    "### Error",
                    extract_content_string(result["error"]) or str(result["error"]),
                ]
            )
            continue

        final_state = result["final_state"]
        section_summaries = get_consolidated_section_summaries(result, summary_llm=summary_llm)
        lines.extend(
            [
                f"Decision: {result.get('decision') or 'Unknown'}",
                "",
                "### Target Outlook",
                sanitize_report_language(result.get("target_summary") or "No target outlook generated."),
                "",
                "### Portfolio Management Decision",
                section_summaries["Portfolio Management Decision"],
                "",
                "### Market Report",
                section_summaries["Market"],
                "",
                "### Social Report",
                section_summaries["Social"],
                "",
                "### News Report",
                section_summaries["News"],
                "",
                "### Fundamentals Report",
                section_summaries["Fundamentals"],
                "",
                "### Trader Plan",
                sanitize_report_language(full_report_text(final_state.get("trader_investment_plan"), "No trader plan generated.")),
                "",
                "### Default Results",
                f"`{result['results_dir']}`",
            ]
        )

    return "\n".join(lines) + "\n"


def build_consolidated_report_html(analysis_results, analysis_date: str, summary_llm=None) -> str:
    """Build an HTML version of the consolidated report."""
    generated_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    completed_results = [result for result in analysis_results if not result.get("error")]
    avg_target = (
        round(
            sum(result.get("price_target", 0) for result in completed_results if result.get("price_target") is not None)
            / max(1, len([result for result in completed_results if result.get("price_target") is not None])),
            2,
        )
        if any(result.get("price_target") is not None for result in completed_results)
        else None
    )

    def decision_kind(decision: str | None) -> str:
        text = (decision or "").lower()
        if "buy" in text:
            return "buy"
        if "sell" in text:
            return "sell"
        if decision:
            return "hold"
        return "failed"

    def decision_icon(kind: str) -> str:
        if kind == "buy":
            return (
                "<svg viewBox='0 0 24 24' aria-hidden='true'>"
                "<path d='M12 4l6 7h-4v9H10v-9H6z'/></svg>"
            )
        if kind == "sell":
            return (
                "<svg viewBox='0 0 24 24' aria-hidden='true'>"
                "<path d='M12 20l-6-7h4V4h4v9h4z'/></svg>"
            )
        return (
            "<svg viewBox='0 0 24 24' aria-hidden='true'>"
            "<path d='M12 4l8 8-8 8-8-8z'/></svg>"
        )

    def metric_icon(kind: str) -> str:
        icons = {
            "target": "<svg viewBox='0 0 24 24'><path d='M12 3a9 9 0 109 9h-2a7 7 0 11-7-7V3zm0 4a5 5 0 105 5h2A7 7 0 1112 7V5zm8-2v6h-6l2.2-2.2-3.1-3.1 1.4-1.4 3.1 3.1z'/></svg>",
            "confidence": "<svg viewBox='0 0 24 24'><path d='M3 13h3v8H3zm5-4h3v12H8zm5-6h3v18h-3zm5 9h3v9h-3z'/></svg>",
            "reference": "<svg viewBox='0 0 24 24'><path d='M12 2l7 4v6c0 5-3.4 9.7-7 10-3.6-.3-7-5-7-10V6zm0 4a3 3 0 100 6 3 3 0 000-6z'/></svg>",
            "delta": "<svg viewBox='0 0 24 24'><path d='M4 17l5-5 4 4 7-8v5h2V4h-9v2h5.4L13 13 9 9 2.6 15.4z'/></svg>",
        }
        return icons[kind]

    def highlight_icon(kind: str) -> str:
        icons = {
            "market": "<svg viewBox='0 0 24 24'><path d='M4 18h16v2H4zm2-2V9h3v7zm5 0V5h3v11zm5 0v-4h3v4z'/></svg>",
            "social": "<svg viewBox='0 0 24 24'><path d='M4 4h16v11H7l-3 3z'/></svg>",
            "news": "<svg viewBox='0 0 24 24'><path d='M5 4h12v16H5zm14 3h2v13a2 2 0 01-2 2h-1v-2h1zM7 7h8v2H7zm0 4h8v2H7zm0 4h5v2H7z'/></svg>",
            "fundamentals": "<svg viewBox='0 0 24 24'><path d='M3 19h18v2H3zm2-2V9h3v8zm5 0V5h3v12zm5 0v-6h3v6z'/></svg>",
        }
        return icons[kind]

    rows = []
    for result in analysis_results:
        status = "Failed" if result.get("error") else "Completed"
        decision = escape(result.get("decision") or "-")
        price_target = escape(format_price_target(result.get("price_target")))
        target_gap = escape(
            format_target_gap_percent(
                result.get("reference_price"),
                result.get("price_target"),
            )
        )
        confidence = (
            f"{result['confidence_score']}/100"
            if result.get("confidence_score") is not None
            else "-"
        )
        rows.append(
            "<tr>"
            f"<td>{escape(result['ticker'])}</td>"
            f"<td>{decision}</td>"
            f"<td>{price_target}</td>"
            f"<td>{target_gap}</td>"
            f"<td>{escape(confidence)}</td>"
            f"<td>{escape(status)}</td>"
            f"<td><code>{escape(result.get('results_dir', '-'))}</code></td>"
            "</tr>"
        )

    sections = []
    for result in analysis_results:
        kind = decision_kind(result.get("decision"))
        confidence_value = result.get("confidence_score")
        confidence_width = max(6, min(100, confidence_value)) if confidence_value is not None else 6

        if result.get("error"):
            sections.append(
                "<section class='stock stock-failed'>"
                "<div class='stock-head'>"
                f"<div class='stock-title'><span class='decision-glyph failed'>{decision_icon('failed')}</span><div><p class='stock-kicker'>Failed Run</p><h2>{escape(result['ticker'])}</h2></div></div>"
                "<div class='decision-pill failed'>Failed</div>"
                "</div>"
                f"<p class='meta-line'>Analysis Date: {escape(result['analysis_date'])}</p>"
                "<div class='narrative-block'>"
                "<h3>Run Error</h3>"
                f"<pre class='report-body'>{escape(extract_content_string(result['error']) or str(result['error']))}</pre>"
                "</div>"
                "</section>"
            )
            continue

        final_state = result["final_state"]
        section_summaries = get_consolidated_section_summaries(result, summary_llm=summary_llm)
        reference_price = format_price_target(result.get("reference_price"))
        delta_label = format_target_gap_percent(
            result.get("reference_price"),
            result.get("price_target"),
        )

        sections.append(
            f"<section class='stock stock-{kind}'>"
            "<div class='stock-head'>"
            f"<div class='stock-title'><span class='decision-glyph {kind}'>{decision_icon(kind)}</span><div><p class='stock-kicker'>Sequential Stock Brief</p><h2>{escape(result['ticker'])}</h2></div></div>"
            f"<div class='decision-pill {kind}'>{escape(result.get('decision') or 'Unknown')}</div>"
            "</div>"
            f"<p class='meta-line'>Analysis Date: {escape(result['analysis_date'])} · Horizon: {escape(result.get('target_horizon') or '-')} · Results: <code>{escape(result['results_dir'])}</code></p>"
            "<div class='metric-ribbon'>"
            f"<article class='metric-card'><span class='metric-icon'>{metric_icon('target')}</span><div><span class='metric-label'>Average Price Target</span><strong>{escape(format_price_target(result.get('price_target')))}</strong></div></article>"
            f"<article class='metric-card'><span class='metric-icon'>{metric_icon('reference')}</span><div><span class='metric-label'>Reference Price</span><strong>{escape(reference_price)}</strong></div></article>"
            f"<article class='metric-card'><span class='metric-icon'>{metric_icon('delta')}</span><div><span class='metric-label'>Target Gap</span><strong>{escape(delta_label)}</strong></div></article>"
            f"<article class='metric-card'><span class='metric-icon'>{metric_icon('confidence')}</span><div><span class='metric-label'>Confidence</span><strong>{escape(str(confidence_value) + '/100' if confidence_value is not None else '-')}</strong></div></article>"
            "</div>"
            "<div class='confidence-strip'>"
            f"<div class='confidence-bar'><span style='width:{confidence_width}%'></span></div>"
            f"<div class='confidence-copy'>{escape(sanitize_report_language(result.get('target_summary') or 'No target outlook generated.'))}</div>"
            "</div>"
            "<div class='narrative-block'>"
            "<h3>Portfolio Management Decision</h3>"
            f"<div class='bullet-summary'>{bullet_markdown_to_html(section_summaries['Portfolio Management Decision'])}</div>"
            "</div>"
            "<div class='highlight-stack'>"
            f"<article class='highlight-card'><div class='highlight-head'><span class='highlight-icon'>{highlight_icon('market')}</span><h3>Market</h3></div><div class='bullet-summary'>{bullet_markdown_to_html(section_summaries['Market'])}</div></article>"
            f"<article class='highlight-card'><div class='highlight-head'><span class='highlight-icon'>{highlight_icon('social')}</span><h3>Social</h3></div><div class='bullet-summary'>{bullet_markdown_to_html(section_summaries['Social'])}</div></article>"
            f"<article class='highlight-card'><div class='highlight-head'><span class='highlight-icon'>{highlight_icon('news')}</span><h3>News</h3></div><div class='bullet-summary'>{bullet_markdown_to_html(section_summaries['News'])}</div></article>"
            f"<article class='highlight-card'><div class='highlight-head'><span class='highlight-icon'>{highlight_icon('fundamentals')}</span><h3>Fundamentals</h3></div><div class='bullet-summary'>{bullet_markdown_to_html(section_summaries['Fundamentals'])}</div></article>"
            "</div>"
            "<div class='narrative-block accent'>"
            "<h3>Trader Plan</h3>"
            f"<pre class='report-body'>{escape(sanitize_report_language(full_report_text(final_state.get('trader_investment_plan'), 'No trader plan generated.')))}</pre>"
            "</div>"
            "</section>"
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Consolidated Trading Analysis Report</title>
  <style>
    :root {{
      --bg: #f3efe7;
      --paper: #fffdf8;
      --ink: #11222b;
      --muted: #60727a;
      --hero-start: #13252e;
      --hero-end: #23505a;
      --teal: #0f766e;
      --coral: #ef6b4a;
      --gold: #b68a14;
      --slate: #2a3c45;
      --line: rgba(17, 34, 43, 0.12);
      --failed: #8d2f24;
      --shadow: 0 22px 48px rgba(17, 34, 43, 0.10);
    }}
    body {{
      margin: 0;
      padding: 32px;
      background:
        radial-gradient(circle at top right, rgba(239, 107, 74, 0.18), transparent 34%),
        radial-gradient(circle at top left, rgba(15, 118, 110, 0.18), transparent 30%),
        linear-gradient(180deg, #fbf7f0, var(--bg));
      color: var(--ink);
      font: 16px/1.55 "Avenir Next", "Segoe UI", sans-serif;
    }}
    h1, h2, h3 {{
      margin-top: 0;
      font-family: Georgia, "Times New Roman", serif;
    }}
    .shell {{
      max-width: 1200px;
      margin: 0 auto;
      display: grid;
      gap: 24px;
    }}
    .hero {{
      background: linear-gradient(135deg, var(--hero-start), var(--hero-end));
      color: white;
      border-radius: 28px;
      padding: 30px;
      box-shadow: var(--shadow);
    }}
    .hero h1 {{
      margin-bottom: 8px;
      font-size: 44px;
      line-height: 0.96;
      max-width: 12ch;
    }}
    .hero-grid {{
      display: grid;
      grid-template-columns: 1.4fr 1fr;
      gap: 22px;
      align-items: end;
    }}
    .hero p {{
      margin: 0;
      color: rgba(255,255,255,0.82);
    }}
    .kpi-strip {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
    }}
    .kpi {{
      background: rgba(255,255,255,0.10);
      border: 1px solid rgba(255,255,255,0.12);
      border-radius: 18px;
      padding: 14px 16px;
    }}
    .kpi span {{
      display: block;
      font-size: 11px;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: rgba(255,255,255,0.65);
      margin-bottom: 4px;
    }}
    .kpi strong {{
      font-size: 24px;
    }}
    .summary-table,
    .stock {{
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 24px;
      box-shadow: var(--shadow);
    }}
    .summary-table {{
      padding: 24px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    th, td {{
      text-align: left;
      padding: 12px 10px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }}
    th {{
      color: var(--teal);
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}
    .stack {{
      display: grid;
      gap: 22px;
    }}
    .stock {{
      padding: 24px;
    }}
    .stock-failed {{
      border-color: rgba(141, 47, 36, 0.25);
      color: var(--failed);
    }}
    .stock-head {{
      display: flex;
      justify-content: space-between;
      gap: 18px;
      align-items: center;
      margin-bottom: 10px;
    }}
    .stock-title {{
      display: flex;
      align-items: center;
      gap: 16px;
    }}
    .stock-title h2 {{
      margin-bottom: 0;
      font-size: 34px;
    }}
    .stock-kicker {{
      margin: 0 0 4px;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.14em;
      color: var(--muted);
    }}
    .decision-glyph {{
      width: 54px;
      height: 54px;
      border-radius: 18px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      background: rgba(17, 34, 43, 0.06);
    }}
    .decision-glyph svg,
    .metric-icon svg,
    .highlight-icon svg {{
      width: 26px;
      height: 26px;
      fill: currentColor;
    }}
    .decision-glyph.buy {{ color: var(--teal); background: rgba(15, 118, 110, 0.12); }}
    .decision-glyph.sell {{ color: var(--coral); background: rgba(239, 107, 74, 0.12); }}
    .decision-glyph.hold {{ color: var(--gold); background: rgba(182, 138, 20, 0.12); }}
    .decision-glyph.failed {{ color: var(--failed); background: rgba(141, 47, 36, 0.12); }}
    .decision-pill {{
      display: inline-flex;
      align-items: center;
      padding: 9px 14px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.1em;
    }}
    .decision-pill.buy {{ background: rgba(15, 118, 110, 0.12); color: var(--teal); }}
    .decision-pill.sell {{ background: rgba(239, 107, 74, 0.12); color: var(--coral); }}
    .decision-pill.hold {{ background: rgba(182, 138, 20, 0.14); color: var(--gold); }}
    .decision-pill.failed {{ background: rgba(141, 47, 36, 0.12); color: var(--failed); }}
    .meta-line {{
      margin: 0 0 18px;
      color: var(--muted);
      font-size: 14px;
    }}
    .metric-ribbon {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }}
    .metric-card {{
      display: flex;
      gap: 12px;
      align-items: center;
      padding: 14px;
      border-radius: 18px;
      background: rgba(17, 34, 43, 0.045);
    }}
    .metric-icon {{
      width: 42px;
      height: 42px;
      border-radius: 14px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      background: white;
      color: var(--slate);
      box-shadow: inset 0 0 0 1px rgba(17,34,43,0.08);
    }}
    .metric-label {{
      display: block;
      margin-bottom: 3px;
      font-size: 11px;
      letter-spacing: 0.1em;
      text-transform: uppercase;
      color: var(--muted);
    }}
    .metric-card strong {{
      font-size: 22px;
    }}
    .confidence-strip {{
      display: grid;
      gap: 10px;
      margin-bottom: 18px;
    }}
    .confidence-bar {{
      width: 100%;
      height: 14px;
      overflow: hidden;
      border-radius: 999px;
      background: rgba(17, 34, 43, 0.08);
    }}
    .confidence-bar span {{
      display: block;
      height: 100%;
      border-radius: inherit;
      background: linear-gradient(90deg, var(--coral), var(--gold), var(--teal));
    }}
    .confidence-copy {{
      color: var(--slate);
      font-size: 15px;
    }}
    .narrative-block {{
      margin-bottom: 18px;
      padding: 18px 20px;
      border-radius: 20px;
      background: rgba(17, 34, 43, 0.04);
    }}
    .narrative-block.accent {{
      background: linear-gradient(135deg, rgba(15, 118, 110, 0.10), rgba(35, 80, 90, 0.08));
    }}
    .narrative-block h3 {{
      margin-bottom: 8px;
      font-size: 20px;
    }}
    .narrative-block p {{
      margin: 0;
      color: var(--slate);
    }}
    .report-body {{
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      overflow-wrap: anywhere;
      color: var(--slate);
      font: inherit;
    }}
    .bullet-summary ul {{
      margin: 0;
      padding-left: 20px;
      color: var(--slate);
    }}
    .bullet-summary li + li {{
      margin-top: 8px;
    }}
    .highlight-stack {{
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      margin-bottom: 18px;
    }}
    .highlight-card {{
      padding: 16px;
      border-radius: 20px;
      border: 1px solid rgba(17,34,43,0.08);
      background: white;
    }}
    .highlight-head {{
      display: flex;
      gap: 10px;
      align-items: center;
      margin-bottom: 8px;
    }}
    .highlight-head h3 {{
      margin-bottom: 0;
      font-size: 18px;
    }}
    .highlight-icon {{
      width: 36px;
      height: 36px;
      border-radius: 12px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      background: rgba(17, 34, 43, 0.05);
      color: var(--teal);
    }}
    code {{
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 12px;
    }}
    .meta {{
      color: rgba(255,255,255,0.72);
      margin-bottom: 8px;
    }}
    @media (max-width: 900px) {{
      body {{ padding: 18px; }}
      .hero-grid,
      .metric-ribbon,
      .highlight-stack {{
        grid-template-columns: 1fr;
      }}
      .stock-head {{
        align-items: flex-start;
        flex-direction: column;
      }}
    }}
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <div class="hero-grid">
        <div>
          <h1>Consolidated Trading Analysis Report</h1>
          <p class="meta">Generated: {escape(generated_at)}</p>
          <p class="meta">Analysis Date: {escape(analysis_date)}</p>
        </div>
        <div class="kpi-strip">
          <article class="kpi">
            <span>Total Stocks</span>
            <strong>{len(analysis_results)}</strong>
          </article>
          <article class="kpi">
            <span>Completed Runs</span>
            <strong>{len(completed_results)}</strong>
          </article>
          <article class="kpi">
            <span>Failed Runs</span>
            <strong>{len(analysis_results) - len(completed_results)}</strong>
          </article>
        </div>
      </div>
    </section>
    <section class="summary-table">
      <h2>Batch Summary</h2>
      <p style="margin:0 0 18px;color:var(--muted);">Average target across completed runs: {escape(format_price_target(avg_target)) if avg_target is not None else '-'}</p>
      <table>
        <thead>
          <tr>
            <th>Ticker</th>
            <th>Decision</th>
            <th>Price Target</th>
            <th>Target Gap</th>
            <th>Confidence</th>
            <th>Status</th>
            <th>Default Results</th>
          </tr>
        </thead>
        <tbody>
          {''.join(rows)}
        </tbody>
      </table>
    </section>
    <section class="stack">
      {''.join(sections)}
    </section>
  </main>
</body>
</html>
"""


def save_consolidated_report(analysis_results, analysis_date: str, save_path: Path, summary_llm=None) -> dict[str, Path]:
    """Write the consolidated batch report to disk in markdown and HTML."""
    save_path.mkdir(parents=True, exist_ok=True)
    markdown_path = save_path / "consolidated_report.md"
    html_path = save_path / "consolidated_report.html"
    markdown_path.write_text(build_consolidated_report(analysis_results, analysis_date, summary_llm=summary_llm))
    html_path.write_text(build_consolidated_report_html(analysis_results, analysis_date, summary_llm=summary_llm))
    return {"markdown": markdown_path, "html": html_path}


def display_consolidated_report(analysis_results, analysis_date: str, summary_llm=None):
    """Render the consolidated batch report in the terminal."""
    console.print()
    console.print(Rule("Consolidated Analysis Report", style="bold green"))
    console.print(Markdown(build_consolidated_report(analysis_results, analysis_date, summary_llm=summary_llm)))


def update_research_team_status(status):
    """Update status for research team members (not Trader)."""
    research_team = ["Bull Researcher", "Bear Researcher", "Research Manager"]
    for agent in research_team:
        message_buffer.update_agent_status(agent, status)


# Ordered list of analysts for status transitions
ANALYST_ORDER = ["market", "social", "news", "fundamentals"]
ANALYST_AGENT_NAMES = {
    "market": "Market Analyst",
    "social": "Social Analyst",
    "news": "News Analyst",
    "fundamentals": "Fundamentals Analyst",
}
ANALYST_REPORT_MAP = {
    "market": "market_report",
    "social": "sentiment_report",
    "news": "news_report",
    "fundamentals": "fundamentals_report",
}


def update_analyst_statuses(message_buffer, chunk):
    """Update analyst statuses based on accumulated report state.

    Logic:
    - Store new report content from the current chunk if present
    - Check accumulated report_sections (not just current chunk) for status
    - Analysts with reports = completed
    - First analyst without report = in_progress
    - Remaining analysts without reports = pending
    - When all analysts done, set Bull Researcher to in_progress
    """
    selected = message_buffer.selected_analysts
    found_active = False

    for analyst_key in ANALYST_ORDER:
        if analyst_key not in selected:
            continue

        agent_name = ANALYST_AGENT_NAMES[analyst_key]
        report_key = ANALYST_REPORT_MAP[analyst_key]
        report_content = extract_content_string(chunk.get(report_key))
        if report_content:
            message_buffer.update_report_section(report_key, report_content)

        has_report = bool(message_buffer.report_sections.get(report_key))

        if has_report:
            message_buffer.update_agent_status(agent_name, "completed")
        elif not found_active:
            message_buffer.update_agent_status(agent_name, "in_progress")
            found_active = True
        else:
            message_buffer.update_agent_status(agent_name, "pending")

    # When all analysts complete, transition research team to in_progress
    if not found_active and selected:
        if message_buffer.agent_status.get("Bull Researcher") == "pending":
            message_buffer.update_agent_status("Bull Researcher", "in_progress")

def extract_content_string(content):
    """Extract string content from various message formats.
    Returns None if no meaningful text content is found.
    """
    import ast

    def is_empty(val):
        """Check if value is empty using Python's truthiness."""
        if val is None or val == '':
            return True
        if isinstance(val, str):
            s = val.strip()
            if not s:
                return True
            try:
                return not bool(ast.literal_eval(s))
            except (ValueError, SyntaxError):
                return False  # Can't parse = real text
        return not bool(val)

    if is_empty(content):
        return None

    if isinstance(content, str):
        return content.strip()

    if isinstance(content, dict):
        text = content.get('text', '')
        return text.strip() if not is_empty(text) else None

    if isinstance(content, list):
        text_parts = [
            item.get('text', '').strip() if isinstance(item, dict) and item.get('type') == 'text'
            else (item.strip() if isinstance(item, str) else '')
            for item in content
        ]
        result = ' '.join(t for t in text_parts if t and not is_empty(t))
        return result if result else None

    return str(content).strip() if not is_empty(content) else None


def classify_message_type(message) -> tuple[str, str | None]:
    """Classify LangChain message into display type and extract content.

    Returns:
        (type, content) - type is one of: User, Agent, Data, Control
                        - content is extracted string or None
    """
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

    content = extract_content_string(getattr(message, 'content', None))

    if isinstance(message, HumanMessage):
        if content and content.strip() == "Continue":
            return ("Control", content)
        return ("User", content)

    if isinstance(message, ToolMessage):
        return ("Data", content)

    if isinstance(message, AIMessage):
        return ("Agent", content)

    # Fallback for unknown types
    return ("System", content)


def format_tool_args(args, max_length=80) -> str:
    """Format tool arguments for terminal display."""
    result = str(args)
    if len(result) > max_length:
        return result[:max_length - 3] + "..."
    return result

def run_single_analysis(selections, ticker: str, batch_index: int | None = None, batch_total: int | None = None):
    """Run the full TradingAgents workflow for a single ticker."""
    # Create config with selected research depth
    config = DEFAULT_CONFIG.copy()
    config["max_debate_rounds"] = selections["research_depth"]
    config["max_risk_discuss_rounds"] = selections["research_depth"]
    config["quick_think_llm"] = selections["shallow_thinker"]
    config["deep_think_llm"] = selections["deep_thinker"]
    config["backend_url"] = selections["backend_url"]
    config["llm_provider"] = selections["llm_provider"].lower()
    # Provider-specific thinking configuration
    config["google_thinking_level"] = selections.get("google_thinking_level")
    config["openai_reasoning_effort"] = selections.get("openai_reasoning_effort")
    config["anthropic_effort"] = selections.get("anthropic_effort")
    config["output_language"] = selections.get("output_language", "English")

    # Create stats callback handler for tracking LLM/tool calls
    stats_handler = StatsCallbackHandler()

    # Normalize analyst selection to predefined order (selection is a 'set', order is fixed)
    selected_set = {analyst.value for analyst in selections["analysts"]}
    selected_analyst_keys = [a for a in ANALYST_ORDER if a in selected_set]

    # Initialize the graph with callbacks bound to LLMs
    graph = TradingAgentsGraph(
        selected_analyst_keys,
        config=config,
        debug=True,
        callbacks=[stats_handler],
    )

    # Initialize message buffer with selected analysts
    message_buffer.init_for_analysis(selected_analyst_keys)
    message_buffer.add_message = MessageBuffer.add_message.__get__(message_buffer, MessageBuffer)
    message_buffer.add_tool_call = MessageBuffer.add_tool_call.__get__(message_buffer, MessageBuffer)
    message_buffer.update_report_section = MessageBuffer.update_report_section.__get__(message_buffer, MessageBuffer)

    # Track start time for elapsed display
    start_time = time.time()

    # Create result directory
    results_dir = Path(config["results_dir"]) / ticker / selections["analysis_date"]
    results_dir.mkdir(parents=True, exist_ok=True)
    report_dir = results_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    log_file = results_dir / "message_tool.log"
    log_file.touch(exist_ok=True)

    def save_message_decorator(obj, func_name):
        func = getattr(obj, func_name)
        @wraps(func)
        def wrapper(*args, **kwargs):
            func(*args, **kwargs)
            timestamp, message_type, content = obj.messages[-1]
            content = extract_content_string(content) or ""
            content = content.replace("\n", " ")  # Replace newlines with spaces
            with open(log_file, "a") as f:
                f.write(f"{timestamp} [{message_type}] {content}\n")
        return wrapper
    
    def save_tool_call_decorator(obj, func_name):
        func = getattr(obj, func_name)
        @wraps(func)
        def wrapper(*args, **kwargs):
            func(*args, **kwargs)
            timestamp, tool_name, args = obj.tool_calls[-1]
            args_str = ", ".join(f"{k}={v}" for k, v in args.items())
            with open(log_file, "a") as f:
                f.write(f"{timestamp} [Tool Call] {tool_name}({args_str})\n")
        return wrapper

    def save_report_section_decorator(obj, func_name):
        func = getattr(obj, func_name)
        @wraps(func)
        def wrapper(section_name, content):
            func(section_name, content)
            if section_name in obj.report_sections and obj.report_sections[section_name] is not None:
                content = obj.report_sections[section_name]
                if content:
                    file_name = f"{section_name}.md"
                    text = "\n".join(str(item) for item in content) if isinstance(content, list) else content
                    with open(report_dir / file_name, "w") as f:
                        f.write(text)
        return wrapper

    message_buffer.add_message = save_message_decorator(message_buffer, "add_message")
    message_buffer.add_tool_call = save_tool_call_decorator(message_buffer, "add_tool_call")
    message_buffer.update_report_section = save_report_section_decorator(message_buffer, "update_report_section")

    # Now start the display layout
    layout = create_layout()

    with Live(layout, refresh_per_second=4) as live:
        # Initial display
        update_display(layout, stats_handler=stats_handler, start_time=start_time)

        # Add initial messages
        if batch_index is not None and batch_total is not None:
            message_buffer.add_message(
                "System", f"Batch progress: {batch_index}/{batch_total}"
            )
        message_buffer.add_message("System", f"Selected ticker: {ticker}")
        message_buffer.add_message(
            "System", f"Analysis date: {selections['analysis_date']}"
        )
        message_buffer.add_message(
            "System",
            f"Selected analysts: {', '.join(analyst.value for analyst in selections['analysts'])}",
        )
        update_display(layout, stats_handler=stats_handler, start_time=start_time)

        # Update agent status to in_progress for the first analyst
        first_analyst = f"{selections['analysts'][0].value.capitalize()} Analyst"
        message_buffer.update_agent_status(first_analyst, "in_progress")
        update_display(layout, stats_handler=stats_handler, start_time=start_time)

        # Create spinner text
        spinner_text = (
            f"Analyzing {ticker} on {selections['analysis_date']}..."
        )
        update_display(layout, spinner_text, stats_handler=stats_handler, start_time=start_time)

        # Initialize state and get graph args with callbacks
        init_agent_state = graph.propagator.create_initial_state(
            ticker, selections["analysis_date"]
        )
        # Pass callbacks to graph config for tool execution tracking
        # (LLM tracking is handled separately via LLM constructor)
        args = graph.propagator.get_graph_args(callbacks=[stats_handler])

        # Stream the analysis
        trace = []
        for chunk in graph.graph.stream(init_agent_state, **args):
            # Process messages if present (skip duplicates via message ID)
            if len(chunk["messages"]) > 0:
                last_message = chunk["messages"][-1]
                msg_id = getattr(last_message, "id", None)

                if msg_id != message_buffer._last_message_id:
                    message_buffer._last_message_id = msg_id

                    # Add message to buffer
                    msg_type, content = classify_message_type(last_message)
                    if content and content.strip():
                        message_buffer.add_message(msg_type, content)

                    # Handle tool calls
                    if hasattr(last_message, "tool_calls") and last_message.tool_calls:
                        for tool_call in last_message.tool_calls:
                            if isinstance(tool_call, dict):
                                message_buffer.add_tool_call(
                                    tool_call["name"], tool_call["args"]
                                )
                            else:
                                message_buffer.add_tool_call(tool_call.name, tool_call.args)

            # Update analyst statuses based on report state (runs on every chunk)
            update_analyst_statuses(message_buffer, chunk)

            # Research Team - Handle Investment Debate State
            if chunk.get("investment_debate_state"):
                debate_state = chunk["investment_debate_state"]
                bull_hist = extract_content_string(debate_state.get("bull_history")) or ""
                bear_hist = extract_content_string(debate_state.get("bear_history")) or ""
                judge = extract_content_string(debate_state.get("judge_decision")) or ""

                # Only update status when there's actual content
                if bull_hist or bear_hist:
                    update_research_team_status("in_progress")
                if bull_hist:
                    message_buffer.update_report_section(
                        "investment_plan", f"### Bull Researcher Analysis\n{bull_hist}"
                    )
                if bear_hist:
                    message_buffer.update_report_section(
                        "investment_plan", f"### Bear Researcher Analysis\n{bear_hist}"
                    )
                if judge:
                    message_buffer.update_report_section(
                        "investment_plan", f"### Research Manager Decision\n{judge}"
                    )
                    update_research_team_status("completed")
                    message_buffer.update_agent_status("Trader", "in_progress")

            # Trading Team
            if chunk.get("trader_investment_plan"):
                message_buffer.update_report_section(
                    "trader_investment_plan", chunk["trader_investment_plan"]
                )
                if message_buffer.agent_status.get("Trader") != "completed":
                    message_buffer.update_agent_status("Trader", "completed")
                    message_buffer.update_agent_status("Aggressive Analyst", "in_progress")

            # Risk Management Team - Handle Risk Debate State
            if chunk.get("risk_debate_state"):
                risk_state = chunk["risk_debate_state"]
                agg_hist = extract_content_string(risk_state.get("aggressive_history")) or ""
                con_hist = extract_content_string(risk_state.get("conservative_history")) or ""
                neu_hist = extract_content_string(risk_state.get("neutral_history")) or ""
                judge = extract_content_string(risk_state.get("judge_decision")) or ""

                if agg_hist:
                    if message_buffer.agent_status.get("Aggressive Analyst") != "completed":
                        message_buffer.update_agent_status("Aggressive Analyst", "in_progress")
                    message_buffer.update_report_section(
                        "final_trade_decision", f"### Aggressive Analyst Analysis\n{agg_hist}"
                    )
                if con_hist:
                    if message_buffer.agent_status.get("Conservative Analyst") != "completed":
                        message_buffer.update_agent_status("Conservative Analyst", "in_progress")
                    message_buffer.update_report_section(
                        "final_trade_decision", f"### Conservative Analyst Analysis\n{con_hist}"
                    )
                if neu_hist:
                    if message_buffer.agent_status.get("Neutral Analyst") != "completed":
                        message_buffer.update_agent_status("Neutral Analyst", "in_progress")
                    message_buffer.update_report_section(
                        "final_trade_decision", f"### Neutral Analyst Analysis\n{neu_hist}"
                    )
                if judge:
                    if message_buffer.agent_status.get("Portfolio Manager") != "completed":
                        message_buffer.update_agent_status("Portfolio Manager", "in_progress")
                        message_buffer.update_report_section(
                            "final_trade_decision", f"### Portfolio Manager Decision\n{judge}"
                        )
                        message_buffer.update_agent_status("Aggressive Analyst", "completed")
                        message_buffer.update_agent_status("Conservative Analyst", "completed")
                        message_buffer.update_agent_status("Neutral Analyst", "completed")
                        message_buffer.update_agent_status("Portfolio Manager", "completed")

            # Update the display
            update_display(layout, stats_handler=stats_handler, start_time=start_time)

            trace.append(chunk)

        # Get final state and decision
        final_state = trace[-1]
        decision = graph.process_signal(
            extract_content_string(final_state["final_trade_decision"]) or ""
        )

        # Update all agent statuses to completed
        for agent in message_buffer.agent_status:
            message_buffer.update_agent_status(agent, "completed")

        message_buffer.add_message(
            "System", f"Completed analysis for {selections['analysis_date']}"
        )

        # Update final report sections
        for section in message_buffer.report_sections.keys():
            if section in final_state:
                message_buffer.update_report_section(section, final_state[section])

        update_display(layout, stats_handler=stats_handler, start_time=start_time)

    console.print(f"\n[bold cyan]Analysis Complete for {ticker}![/bold cyan]\n")
    target_profile = estimate_target_profile(
        graph.quick_thinking_llm,
        ticker,
        selections["analysis_date"],
        final_state,
        decision,
    )
    return {
        "ticker": ticker,
        "analysis_date": selections["analysis_date"],
        "decision": decision,
        "final_state": final_state,
        "results_dir": str(results_dir.resolve()),
        **target_profile,
    }


def run_analysis():
    # First get all user selections
    selections = get_user_selections()
    save_preferences = get_save_preferences(selections)
    tickers = selections["tickers"]
    analysis_results = []

    if len(tickers) > 1:
        console.print(
            f"\n[bold cyan]Starting batch analysis for {len(tickers)} tickers:[/bold cyan] "
            f"{', '.join(tickers)}\n"
        )

    for index, ticker in enumerate(tickers, start=1):
        if len(tickers) > 1:
            console.print(
                Rule(
                    f"Ticker {index}/{len(tickers)}: {ticker}",
                    style="bold cyan",
                )
            )
        try:
            analysis_results.append(
                run_single_analysis(
                    selections,
                    ticker,
                    batch_index=index if len(tickers) > 1 else None,
                    batch_total=len(tickers) if len(tickers) > 1 else None,
                )
            )
        except Exception as exc:
            console.print(f"[red]Analysis failed for {ticker}: {exc}[/red]")
            analysis_results.append(
                {
                    "ticker": ticker,
                    "analysis_date": selections["analysis_date"],
                    "decision": None,
                    "final_state": None,
                    "results_dir": str(
                        (Path(DEFAULT_CONFIG["results_dir"]) / ticker / selections["analysis_date"]).resolve()
                    ),
                    "price_target": None,
                    "confidence_score": None,
                    "target_horizon": None,
                    "target_summary": None,
                    "reference_price": None,
                    "error": str(exc),
                }
            )

    consolidated_default_report = None
    if len(analysis_results) > 1:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        consolidated_default_dir = (
            Path(DEFAULT_CONFIG["results_dir"])
            / "batch"
            / selections["analysis_date"]
            / timestamp
        )
        try:
            consolidated_default_report = save_consolidated_report(
                analysis_results,
                selections["analysis_date"],
                consolidated_default_dir,
            )
            console.print(
                f"[green]✓ Default consolidated reports saved to:[/green] {consolidated_default_dir.resolve()}"
            )
            console.print(
                f"  [dim]Markdown:[/dim] {consolidated_default_report['markdown'].name}"
            )
            console.print(
                f"  [dim]HTML:[/dim] {consolidated_default_report['html'].name}"
            )
        except Exception as exc:
            console.print(f"[red]Error saving default consolidated report: {exc}[/red]")

    # Post-analysis prompts (outside Live context for clean interaction)
    console.print("\n[bold cyan]Batch Analysis Complete![/bold cyan]\n" if len(tickers) > 1 else "\n[bold cyan]Analysis Complete![/bold cyan]\n")
    successful_results = [result for result in analysis_results if result.get("final_state")]

    if not successful_results and len(tickers) == 1:
        console.print("[yellow]No successful report was generated to save or display.[/yellow]")
    if save_preferences["save_enabled"] and successful_results:
        save_path = save_preferences["save_path"]
        try:
            if len(tickers) == 1:
                report_file = save_report_to_disk(
                    successful_results[0]["final_state"],
                    successful_results[0]["ticker"],
                    save_path,
                )
                console.print(f"\n[green]✓ Report saved to:[/green] {save_path.resolve()}")
                console.print(f"  [dim]Complete report:[/dim] {report_file.name}")
            else:
                save_path.mkdir(parents=True, exist_ok=True)
                for result in analysis_results:
                    if result.get("error") or not result.get("final_state"):
                        continue
                    save_report_to_disk(
                        result["final_state"],
                        result["ticker"],
                        save_path / result["ticker"],
                    )
                report_file = save_consolidated_report(
                    analysis_results,
                    selections["analysis_date"],
                    save_path,
                )
                console.print(f"\n[green]✓ Batch reports saved to:[/green] {save_path.resolve()}")
                console.print(f"  [dim]Markdown:[/dim] {report_file['markdown'].name}")
                console.print(f"  [dim]HTML:[/dim] {report_file['html'].name}")
        except Exception as e:
            console.print(f"[red]Error saving report: {e}[/red]")

    # Prompt to display full report
    display_prompt = (
        "\nDisplay full report on screen?"
        if len(tickers) == 1
        else "\nDisplay consolidated report on screen?"
    )
    display_choice = typer.prompt(display_prompt, default="Y").strip().upper() if successful_results else "N"
    if display_choice in ("Y", "YES", ""):
        if len(tickers) == 1:
            display_complete_report(successful_results[0]["final_state"])
        else:
            display_consolidated_report(analysis_results, selections["analysis_date"])


@app.command()
def analyze():
    run_analysis()


@app.command("serve-web")
def serve_web(
    host: str = "127.0.0.1",
    port: int = 8000,
    reload: bool = False,
    open_browser: bool = True,
    log_level: str = "WARNING",
    log_file: str | None = None,
):
    """Run the TradingAgents web application."""
    from tradingagents.web.app import run

    run(
        host=host,
        port=port,
        reload=reload,
        open_browser=open_browser,
        log_level=log_level,
        log_file=log_file,
    )


if __name__ == "__main__":
    app()
