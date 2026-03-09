import os

from tradingagents.web.app import run


if __name__ == "__main__":
    run(
        log_level=os.getenv("TRADINGAGENTS_WEB_LOG_LEVEL", "WARNING"),
        log_file=os.getenv("TRADINGAGENTS_WEB_LOG_FILE"),
    )
