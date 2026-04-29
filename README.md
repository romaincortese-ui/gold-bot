# Gold-bot

Gold-bot is a dedicated XAU/USD bot intended to run against its own OANDA sub-account, with its own risk budget and strategy set. The structure is intentionally similar to the FX-bot layout: thin entrypoints, a runtime orchestrator, small pure helper modules, and strategy logic separated from market access.

## Architecture

- `main.py`: live runtime loop
- `macro_engine.py`: red-folder event snapshot builder for gold
- `goldbot/config.py`: environment parsing and validation
- `goldbot/marketdata.py`: OANDA account, pricing, candles, and order calls
- `goldbot/news.py`: economic-calendar fetch and filtering for gold-relevant USD events
- `goldbot/telegram.py`: Telegram polling client embedded inside the live worker for runtime alerts and commands
- `goldbot/indicators.py`: EMA, RSI, MACD, ATR, divergence, candlestick helpers, consolidation boxes
- `goldbot/strategies.py`: three XAU/USD strategy evaluators
- `goldbot/budget.py`: Gold risk-budget tracking for the dedicated account
- `goldbot/runtime.py`: session filters, strategy selection, sizing, and execution
- `goldbot/backtest_config.py`: historical backtest window and artifact settings
- `goldbot/backtest_data.py`: OANDA historical candle loading with local cache files
- `goldbot/backtest_engine.py`: bar-by-bar XAU/USD backtest engine reusing live strategy scorers and exit plans
- `goldbot/backtest_reporter.py`: trade journal, equity curve, and summary export helpers
- `run_backtest.py`: backtest runner entrypoint
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

- Gold-bot sizes risk off the dedicated OANDA account balance it sees.
- `GOLD_BUDGET_ALLOCATION` remains as a per-service safety knob in `[0, 1]`, defaulting to `1.00` for a separated account.
- Position sizing is ATR-aware because XAU/USD volatility is unstable across sessions and macro regimes.
- Entry execution is blocked when the live XAU/USD spread is wider than `MAX_ENTRY_SPREAD` so the bot does not enter into distorted post-news pricing.
- Macro breakout entries can wait for live spread stabilization before firing, which is meant to avoid the worst post-news OANDA spread blowouts instead of chasing the first quote spike.
- Breakout entries require volume confirmation against the recent 20-candle average and, when configured, can also require an external exchange-volume oracle snapshot before a macro breakout is allowed.
- Static take-profit caps are removed from new signals. Opportunities now carry an ATR-based exit plan with partial-profit, break-even, and trailing-stop parameters.
- Low-liquidity Asian-session signals are ignored by default; the bot trades London, New York, and the overlap.
- Trend pullback entries now require stronger H4 EMA separation, fast-EMA slope confirmation, and H1 EMA alignment so the bot is less likely to chase weak pullbacks.
- Trend pullback entries can also be blocked when a simple USD proxy basket points the wrong way. The proxy uses OANDA H4 trends from `EUR_USD`, `GBP_USD`, and `USD_JPY` to avoid taking fresh gold longs into broad USD strength.
- An optional real-yield overlay can veto or halve risk on gold entries when US 10Y TIPS real yields move sharply against the trade over the recent lookback window.
- Same-direction stopout cooldowns are supported for the trend sleeve to reduce repeated re-entry attempts after a failed pullback.

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
- `GOLD_HEARTBEAT_INTERVAL=3600`

Budget and risk:

- `GOLD_BUDGET_ALLOCATION=1.00`
- `FX_BUDGET_ALLOCATION=1.00`
- `MAX_RISK_PER_TRADE=0.0075`
- `MAX_TOTAL_GOLD_RISK=0.03`
- `MAX_OPEN_GOLD_TRADES=1`
- `MAX_ENTRY_SPREAD=0.80`
- `BREAKOUT_MIN_VOLUME_RATIO=1.10`
- `BREAKOUT_VOLUME_MODE=tick|external|hybrid`
- `BREAKOUT_EXTERNAL_VOLUME_FILE=artifacts/gc_volume_snapshot.json`
- `BREAKOUT_EXTERNAL_VOLUME_MAX_AGE_MINUTES=30`
- `BREAKOUT_EXTERNAL_MIN_VOLUME_RATIO=1.05`
- `MACRO_BREAKOUT_SPREAD_SETTLE_SECONDS=45`
- `MACRO_BREAKOUT_SPREAD_STABILITY_CHECKS=3`
- `MACRO_BREAKOUT_SPREAD_STABILITY_TOLERANCE=0.15`
- `REAL_YIELD_FILTER_ENABLED=false`
- `REAL_YIELD_STATE_MAX_AGE_HOURS=24`
- `REAL_YIELD_LOOKBACK_DAYS=5`
- `REAL_YIELD_REDUCE_RISK_BPS=7.5`
- `REAL_YIELD_VETO_BPS=15.0`
- `REAL_YIELD_ADVERSE_RISK_MULTIPLIER=0.5`
- `BREAKOUT_OVERLAP_ONLY=true`
- `PARTIAL_PROFIT_RR=1.25`
- `BREAK_EVEN_RR=1.25`
- `TRAILING_ATR_MULT=2.8`
- `TRAILING_EMA_PERIOD=20`
- `TREND_H1_CONFIRM_EMA_PERIOD=50`
- `TREND_MIN_STRENGTH_ATR=1.25`
- `TREND_FAST_SLOPE_BARS=3`
- `TREND_MIN_SLOPE_ATR=0.10`
- `TREND_STOPOUT_COOLDOWN_HOURS=48`
- `USD_REGIME_FILTER_ENABLED=true`
- `USD_REGIME_FAST_EMA=20`
- `USD_REGIME_SLOW_EMA=50`
- `USD_REGIME_MIN_BIAS_ATR=0.35`

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
```

## Backtesting

Gold-bot now includes an internal historical backtest runner for XAU/USD. It reuses the live strategy scorers, the same ATR-based exit plans, and exports a trade journal, equity curve, and summary JSON.

Run a 30-day backtest:

```bash
set GOLD_BACKTEST_ROLLING_DAYS=30
set GOLD_BACKTEST_OUTPUT_DIR=backtest_output/30day
python run_backtest.py
```

Explicit window example:

```bash
python run_backtest.py --start 2026-03-07T00:00:00Z --end 2026-04-06T00:00:00Z --output-dir backtest_output/manual_window
```

Robustness example:

```bash
set GOLD_BACKTEST_WALK_FORWARD_TRAIN_DAYS=30
set GOLD_BACKTEST_WALK_FORWARD_TEST_DAYS=15
set GOLD_BACKTEST_WALK_FORWARD_STEP_DAYS=15
set GOLD_BACKTEST_MONTE_CARLO_ITERATIONS=1000
set GOLD_BACKTEST_MONTE_CARLO_RUIN_THRESHOLD_PCT=25
python run_backtest.py --output-dir backtest_output/robustness
```

Backtest artifacts:

- `backtest_output/.../equity_curve.csv`
- `backtest_output/.../trade_journal.csv`
- `backtest_output/.../summary.json`

Backtest notes:

- Historical candles are pulled from OANDA and cached under `backtest_cache/`.
- The runner needs valid OANDA API access for historical XAU/USD candles.
- Macro breakout backtests use `GOLD_BACKTEST_EVENT_FILE` for historical calendar events. The repo includes `historical_events/usd_major_events_2026_q1_q2.json`, seeded from official BLS, BEA, and Federal Reserve schedules for CPI, Employment Situation, Personal Income and Outlays, and FOMC statements.
- Walk-forward output is written into `summary.json` under `robustness.walk_forward`, and Monte Carlo sequence-risk output is written under `robustness.monte_carlo`.
- When `REAL_YIELD_FILTER_ENABLED=true`, the macro worker fetches current `DGS10` and `DFII10` data from FRED, writes a current real-yield snapshot into the macro state file, and the backtester pulls the matching historical series from FRED into the cache directory.

External volume oracle snapshot format:

```json
{
   "source": "databento_gc",
   "generated_at": "2026-04-07T12:31:00Z",
   "latest_volume_ratio": 1.42,
   "current_volume": 18420,
   "baseline_volume": 12970
}
```

If `BREAKOUT_VOLUME_MODE=hybrid`, Gold-bot requires both the internal OANDA tick-volume filter and the external oracle ratio to pass before a macro breakout is tradable.

For larger comparison runs, use `_run_gold_sweep.py` to rank parameter profiles over 30, 60, and 90-day windows.

## Railway Layout

Use two Railway services or workers for the Gold stack:

- `worker`: `python main.py`
- `macro`: `python run_macro_engine.py`

The included [Gold-bot/Procfile](Gold-bot/Procfile) and [Gold-bot/Dockerfile](Gold-bot/Dockerfile) support that layout directly.

For GitHub-backed Railway services, the repo also includes [Gold-bot/railway_entrypoint.py](Gold-bot/railway_entrypoint.py). Both services can use the same image/start command and select behavior via `GOLD_SERVICE_ROLE=worker|macro`.

For local runs the services can share [Gold-bot/state.json](Gold-bot/state.json). For separate Railway services, set `REDIS_URL` so the runtime state, runtime-status heartbeats, and shared budget all move onto Redis.

The worker owns Telegram polling directly and supports these commands:

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

`/pause`, `/resume`, `/sync`, and `/closeall` are still queued into runtime state and executed by the trading runtime on its next cycle. That keeps all broker-affecting actions in one process while removing the extra Telegram deployment and its shared-state failure mode.

## Notes

- `EXECUTION_MODE=signal_only` is the safest default while you verify OANDA contract sizing for your spread-bet account and bring up the Gold worker and macro services.
- If you later want stricter cross-bot budget coordination, the FX bot can publish its reserved risk into the same `shared_budget_state.json` file under the `fx` key without changing Gold-bot's runtime contract.
- Runtime events now include entry opens, spread-blocked executions, partial profits, break-even moves, trailing-stop updates, and tracked closures so the embedded Telegram client can notify from the same process that owns trading state.