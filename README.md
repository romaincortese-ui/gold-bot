# Gold-bot

Gold-bot is a dedicated XAU/USD bot for the same OANDA account used by the FX bot, but with its own capital sleeve and strategy set. The structure is intentionally similar to the FX-bot layout: thin entrypoints, a runtime orchestrator, small pure helper modules, and strategy logic separated from market access.

## Architecture

- `main.py`: live runtime loop
- `macro_engine.py`: red-folder event snapshot builder for gold
- `run_telegram_bot.py`: separate Telegram worker entrypoint
- `goldbot/config.py`: environment parsing and validation
- `goldbot/marketdata.py`: OANDA account, pricing, candles, and order calls
- `goldbot/news.py`: economic-calendar fetch and filtering for gold-relevant USD events
- `goldbot/telegram.py`: Telegram polling worker for runtime alerts and commands
- `goldbot/indicators.py`: EMA, RSI, MACD, ATR, divergence, candlestick helpers, consolidation boxes
- `goldbot/strategies.py`: three XAU/USD strategy evaluators
- `goldbot/budget.py`: shared-sleeve budget tracking with 50% FX / 50% Gold allocation
- `goldbot/runtime.py`: session filters, strategy selection, sizing, and execution
- `tests/`: pure-logic unit tests

## Strategy Set

The bot trades only `XAU_USD` and uses three low-frequency gold playbooks:

1. `MACRO_BREAKOUT`
   Looks for a compressed 12-24 hour box ahead of major USD events such as NFP, CPI, FOMC, and PCE. It waits for the release, ignores the first whipsaw period, and then only trades a clean break of the pre-news range.

2. `EXHAUSTION_REVERSAL`
   Looks for H4 or Daily overextension with RSI and MACD divergence near historical support or resistance. This is the mean-reversion sleeve for catching exhaustion after parabolic moves.

3. `TREND_PULLBACK`
   Uses the H4 50 EMA and 200 EMA to define direction. In a confirmed trend it waits for price to pull back toward the 50 EMA and requires a bullish or bearish reversal candle before entering.

## Risk Model

- Capital allocation is split `50% FX / 50% Gold` at the account level.
- Gold-bot sizes risk off the gold sleeve, not the whole account.
- Position sizing is ATR-aware because XAU/USD volatility is unstable across sessions and macro regimes.
- Entry execution is blocked when the live XAU/USD spread is wider than `MAX_ENTRY_SPREAD` so the bot does not enter into distorted post-news pricing.
- Breakout entries require volume confirmation against the recent 20-candle average and, by default, are limited to the London/New York overlap.
- Static take-profit caps are removed from new signals. Opportunities now carry an ATR-based exit plan with partial-profit, break-even, and trailing-stop parameters.
- Low-liquidity Asian-session signals are ignored by default; the bot trades London, New York, and the overlap.

## Environment Variables

Core:

- `OANDA_API_KEY`
- `OANDA_ACCOUNT_ID`
- `OANDA_ENVIRONMENT=practice|live`
- `ACCOUNT_TYPE=spread_bet|standard`
- `EXECUTION_MODE=signal_only|paper|live`
- `PAPER_BALANCE=10000`
- `GOLD_TELEGRAM_TOKEN`
- `GOLD_TELEGRAM_CHAT_ID`
- `GOLD_TELEGRAM_POLL_SECONDS=5`
- `GOLD_TELEGRAM_HEARTBEAT_MINUTES=60`
- `GOLD_TELEGRAM_OFFSET_FILE=telegram_state.json`
- `REDIS_URL`
- `GOLD_RUNTIME_STATE_KEY=gold_runtime_state`
- `GOLD_BOT_STATUS_KEY=gold_bot_runtime_status`
- `GOLD_TELEGRAM_STATUS_KEY=gold_telegram_runtime_status`
- `GOLD_SHARED_BUDGET_KEY=shared_budget_state`
- `GOLD_STATUS_TTL=1800`

Budget and risk:

- `GOLD_BUDGET_ALLOCATION=0.50`
- `FX_BUDGET_ALLOCATION=0.50`
- `MAX_RISK_PER_TRADE=0.0075`
- `MAX_TOTAL_GOLD_RISK=0.03`
- `MAX_OPEN_GOLD_TRADES=1`
- `MAX_ENTRY_SPREAD=0.80`
- `BREAKOUT_MIN_VOLUME_RATIO=1.10`
- `BREAKOUT_OVERLAP_ONLY=true`
- `PARTIAL_PROFIT_RR=1.0`
- `BREAK_EVEN_RR=1.0`
- `TRAILING_ATR_MULT=2.2`
- `TRAILING_EMA_PERIOD=20`

Files:

- `GOLD_STATE_FILE=state.json`
- `GOLD_SHARED_BUDGET_FILE=shared_budget_state.json`
- `GOLD_MACRO_STATE_FILE=gold_macro_state.json`
- `GOLD_NEWS_CACHE_FILE=gold_news_cache.json`

## Running

```bash
python -m pip install -r requirements.txt
python run_macro_engine.py
python main.py
python run_telegram_bot.py
```

## Railway Layout

Use separate Railway services or workers for the Gold stack:

- `worker`: `python main.py`
- `macro`: `python run_macro_engine.py`
- `telegram`: `python run_telegram_bot.py`

The included [Gold-bot/Procfile](Gold-bot/Procfile) and [Gold-bot/Dockerfile](Gold-bot/Dockerfile) support that layout directly.

For local runs the services can share [Gold-bot/state.json](Gold-bot/state.json). For separate Railway services, set `REDIS_URL` so the runtime state, pause/resume controls, runtime-status heartbeats, and shared budget all move onto Redis.

The Telegram worker reads runtime events and open-trade status from the shared Gold state and supports these commands:

- `/help`
- `/status`
- `/last`
- `/events`
- `/open`
- `/risk`
- `/pause`
- `/resume`
- `/sync`
- `/closeall`

`/pause`, `/resume`, `/sync`, and `/closeall` are queued into shared runtime state and executed by the trading runtime on its next cycle. That keeps all broker-affecting actions in one process even when Telegram runs as a separate Railway service.

## Notes

- `EXECUTION_MODE=signal_only` is the safest default while you verify OANDA contract sizing for your spread-bet account and bring up the separate Gold Telegram bot and Railway worker.
- If you later want stricter cross-bot budget coordination, the FX bot can publish its reserved risk into the same `shared_budget_state.json` file under the `fx` key without changing Gold-bot's runtime contract.
- Runtime events now include entry opens, spread-blocked executions, partial profits, break-even moves, trailing-stop updates, and tracked closures so the Telegram worker can notify independently from the trading runtime.