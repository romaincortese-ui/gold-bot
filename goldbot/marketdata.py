import json
import logging
import math
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

from goldbot.config import Settings
from goldbot.models import Opportunity


log = logging.getLogger(__name__)

_TRANSIENT_STATUS_CODES = {502, 503, 504, 429}
_MAX_RETRIES = 3
_RETRY_BACKOFF = 2  # seconds, doubles each attempt

_URL_PATTERN = __import__("re").compile(r'for url: https?://\S+|https?://\S+')


def _strip_url(text: str) -> str:
    return _URL_PATTERN.sub("[OANDA API]", text).strip()


class SpreadTooWideError(RuntimeError):
    pass


class OandaClient:
    MAX_CANDLES_PER_REQUEST = 4500

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.api_url = (
            "https://api-fxpractice.oanda.com"
            if settings.oanda_environment == "practice"
            else "https://api-fxtrade.oanda.com"
        )

    def uses_native_units(self) -> bool:
        return self.settings.execution_mode == "live" and bool(self.settings.oanda_api_key and self.settings.oanda_account_id)

    def get_account_summary(self, *, force_broker: bool = False) -> dict:
        if ((not force_broker) and self.settings.execution_mode in {"signal_only", "paper"}) or not self.settings.oanda_account_id:
            return {
                "balance": self.settings.paper_balance,
                "nav": self.settings.paper_balance,
                "unrealized_pl": 0.0,
                "margin_used": 0.0,
                "margin_available": self.settings.paper_balance,
                "currency": "GBP" if self.settings.account_type == "spread_bet" else "USD",
            }
        try:
            payload = self._get(f"/v3/accounts/{self.settings.oanda_account_id}/summary", _force=force_broker)
        except requests.exceptions.RequestException as exc:
            if self.settings.execution_mode == "live":
                log.error("CRITICAL: Cannot fetch broker balance in live mode: %s", exc)
                raise
            log.warning("Failed to fetch account summary after retries: %s", exc)
            return {
                "balance": self.settings.paper_balance,
                "nav": self.settings.paper_balance,
                "unrealized_pl": 0.0,
                "margin_used": 0.0,
                "margin_available": self.settings.paper_balance,
                "currency": "GBP" if self.settings.account_type == "spread_bet" else "USD",
            }
        account = payload.get("account", {})
        return {
            "balance": float(account.get("balance", 0.0)),
            "nav": float(account.get("NAV", account.get("balance", 0.0))),
            "unrealized_pl": float(account.get("unrealizedPL", 0.0)),
            "margin_used": float(account.get("marginUsed", 0.0)),
            "margin_available": float(account.get("marginAvailable", 0.0)),
            "currency": str(account.get("currency", "USD")),
        }

    def fetch_candles(self, instrument: str, granularity: str, count: int) -> pd.DataFrame | None:
        try:
            payload = self._get(
                f"/v3/instruments/{instrument}/candles",
                params={"granularity": granularity, "count": count, "price": "M"},
            )
        except requests.exceptions.RequestException as exc:
            log.warning("Failed to fetch candles for %s after retries: %s", instrument, exc)
            return None
        return self._candles_to_frame(candles=payload.get("candles", []), granularity=granularity)

    def fetch_candles_range(self, instrument: str, granularity: str, start: datetime, end: datetime) -> pd.DataFrame | None:
        step = self._granularity_to_timedelta(granularity)
        cursor = start.astimezone(timezone.utc)
        upper_bound = end.astimezone(timezone.utc)
        candles: list[dict] = []

        while cursor < upper_bound:
            chunk_end = min(upper_bound, cursor + (step * self.MAX_CANDLES_PER_REQUEST))
            try:
                payload = self._get(
                    f"/v3/instruments/{instrument}/candles",
                    params={
                        "granularity": granularity,
                        "from": cursor.isoformat().replace("+00:00", "Z"),
                        "to": chunk_end.isoformat().replace("+00:00", "Z"),
                        "price": "M",
                    },
                )
            except requests.exceptions.RequestException as exc:
                log.warning("Failed to fetch candles range for %s after retries: %s", instrument, exc)
                return None
            candles.extend(payload.get("candles", []))

            if chunk_end <= cursor:
                break
            cursor = chunk_end

        return self._candles_to_frame(candles=candles, granularity=granularity)

    @staticmethod
    def _candles_to_frame(*, candles: list[dict], granularity: str) -> pd.DataFrame | None:
        rows = []
        for candle in candles:
            if not candle.get("complete", True) and granularity != "M1":
                continue
            mid = candle.get("mid", {})
            rows.append(
                {
                    "time": pd.to_datetime(candle.get("time"), utc=True, errors="coerce"),
                    "open": float(mid.get("o", 0.0)),
                    "high": float(mid.get("h", 0.0)),
                    "low": float(mid.get("l", 0.0)),
                    "close": float(mid.get("c", 0.0)),
                    "volume": int(candle.get("volume", 0)),
                }
            )
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df = df.dropna(subset=["close"])
        return df.sort_values("time").drop_duplicates(subset=["time"]).reset_index(drop=True)

    def get_price(self, instrument: str) -> dict[str, float]:
        if self.settings.execution_mode in {"signal_only", "paper"}:
            df = self.fetch_candles(instrument, "M15", 2)
            if df is None or df.empty:
                return {"bid": 0.0, "ask": 0.0, "mid": 0.0, "spread": 0.0}
            price = float(df["close"].iloc[-1])
            return {"bid": price, "ask": price, "mid": price, "spread": 0.0}
        try:
            payload = self._get(
                f"/v3/accounts/{self.settings.oanda_account_id}/pricing",
                params={"instruments": instrument},
            )
        except requests.exceptions.RequestException as exc:
            log.warning("Failed to fetch price for %s after retries: %s", instrument, exc)
            return {"bid": 0.0, "ask": 0.0, "mid": 0.0, "spread": 0.0}
        prices = payload.get("prices", [])
        if not prices:
            return {"bid": 0.0, "ask": 0.0, "mid": 0.0, "spread": 0.0}
        price = prices[0]
        bid = float(price.get("bids", [{}])[0].get("price", 0.0))
        ask = float(price.get("asks", [{}])[0].get("price", 0.0))
        return {
            "bid": bid,
            "ask": ask,
            "mid": (bid + ask) / 2 if bid > 0 and ask > 0 else 0.0,
            "spread": max(0.0, ask - bid) if ask > 0 and bid > 0 else 0.0,
        }

    def validate_entry_spread(self, quote: dict[str, float]) -> None:
        spread = float(quote.get("spread", 0.0) or 0.0)
        if spread > self.settings.max_entry_spread:
            raise SpreadTooWideError(
                f"Spread {spread:.3f} exceeds max allowed {self.settings.max_entry_spread:.3f}"
            )

    def list_open_positions(self) -> list[dict]:
        if self.settings.execution_mode in {"signal_only", "paper"} or not self.settings.oanda_account_id:
            return []
        payload = self._get(f"/v3/accounts/{self.settings.oanda_account_id}/openPositions")
        return list(payload.get("positions", []))

    def list_open_trades(self, *, force_broker: bool = False) -> list[dict]:
        if ((not force_broker) and self.settings.execution_mode in {"signal_only", "paper"}) or not self.settings.oanda_account_id:
            return []
        try:
            payload = self._get(f"/v3/accounts/{self.settings.oanda_account_id}/openTrades", _force=force_broker)
        except requests.exceptions.RequestException as exc:
            log.warning("Failed to fetch open trades after retries: %s", exc)
            return []
        return list(payload.get("trades", []))

    def calculate_xau_size(self, risk_amount: float, stop_distance: float, account_currency: str) -> float:
        if stop_distance <= 0:
            return 0.0
        if not self.uses_native_units():
            stake = risk_amount / stop_distance
            return max(0.1, math.floor(stake * 100) / 100)  # round DOWN to never exceed intended risk
        conversion = self._estimate_conversion_rate("USD", account_currency)
        per_unit_risk = stop_distance * conversion
        if per_unit_risk <= 0:
            return 0.0
        return max(1.0, round(risk_amount / per_unit_risk))

    def place_market_order(self, opportunity: Opportunity, size: float, *, quote: dict[str, float] | None = None) -> dict:
        quote = quote or self.get_price(self.settings.instrument)
        self.validate_entry_spread(quote)
        if self.settings.execution_mode in {"signal_only", "paper"}:
            entry_price = quote["ask"] if opportunity.direction == "LONG" else quote["bid"]
            return {
                "id": f"PAPER_{int(datetime.now(timezone.utc).timestamp() * 1000)}",
                "instrument": self.settings.instrument,
                "direction": opportunity.direction,
                "size": size,
                "price": entry_price,
                "spread": quote.get("spread", 0.0),
                "mode": self.settings.execution_mode,
                "strategy": opportunity.strategy,
            }

        signed_units = int(abs(size)) if opportunity.direction == "LONG" else -int(abs(size))
        order = {
            "order": {
                "type": "MARKET",
                "instrument": self.settings.instrument,
                "units": str(signed_units),
                "timeInForce": "FOK",
                "positionFill": "DEFAULT",
                "stopLossOnFill": {"price": self._format_price(opportunity.stop_price)},
            }
        }
        payload = self._post(f"/v3/accounts/{self.settings.oanda_account_id}/orders", order)
        fill = payload.get("orderFillTransaction", {})
        return {
            "id": str(fill.get("id") or fill.get("orderID") or ""),
            "instrument": self.settings.instrument,
            "direction": opportunity.direction,
            "size": abs(signed_units),
            "price": float(fill.get("price", 0.0)),
            "spread": quote.get("spread", 0.0),
            "mode": "live",
            "strategy": opportunity.strategy,
            "raw": payload,
        }

    def modify_trade(self, trade_id: str, *, stop_price: float | None = None) -> bool:
        if stop_price is None:
            return True
        if self.settings.execution_mode in {"signal_only", "paper"}:
            return True
        payload = self._put(
            f"/v3/accounts/{self.settings.oanda_account_id}/trades/{trade_id}/orders",
            {"stopLoss": {"price": self._format_price(stop_price)}},
        )
        return "errorMessage" not in payload and "error" not in payload

    def close_trade(self, trade_id: str, *, size: float | None = None) -> bool:
        if self.settings.execution_mode in {"signal_only", "paper"}:
            return True
        payload: dict[str, str] = {}
        if size is not None:
            payload["units"] = str(int(round(abs(size))))
        response = self._put(
            f"/v3/accounts/{self.settings.oanda_account_id}/trades/{trade_id}/close",
            payload,
        )
        return "errorMessage" not in response and "error" not in response

    def _estimate_conversion_rate(self, base_currency: str, quote_currency: str) -> float:
        if base_currency == quote_currency:
            return 1.0
        direct = f"{base_currency}_{quote_currency}"
        inverse = f"{quote_currency}_{base_currency}"
        direct_price = self.get_price(direct)
        if direct_price["bid"] > 0:
            return direct_price["bid"]
        inverse_price = self.get_price(inverse)
        if inverse_price["ask"] > 0:
            return 1.0 / inverse_price["ask"]
        return 1.0

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.settings.oanda_api_key:
            headers["Authorization"] = f"Bearer {self.settings.oanda_api_key}"
        return headers

    def _get(self, path: str, params: dict | None = None, *, _force: bool = False) -> dict:
        if not _force and self.settings.execution_mode in {"signal_only", "paper"} and path.startswith("/v3/accounts"):
            return {}
        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            try:
                response = requests.get(f"{self.api_url}{path}", headers=self._headers(), params=params, timeout=20)
                if response.status_code in _TRANSIENT_STATUS_CODES and attempt < _MAX_RETRIES - 1:
                    wait = _RETRY_BACKOFF * (2 ** attempt)
                    log.warning("OANDA %s on %s – retry %d/%d in %ds", response.status_code, path, attempt + 1, _MAX_RETRIES, wait)
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as exc:
                raise requests.exceptions.HTTPError(
                    _strip_url(str(exc)), response=exc.response,
                ) from None
            except requests.exceptions.ConnectionError as exc:
                last_exc = exc
                if attempt < _MAX_RETRIES - 1:
                    wait = _RETRY_BACKOFF * (2 ** attempt)
                    log.warning("OANDA connection error on %s – retry %d/%d in %ds: %s", path, attempt + 1, _MAX_RETRIES, wait, exc)
                    time.sleep(wait)
                    continue
                raise requests.exceptions.ConnectionError(_strip_url(str(exc))) from None
        raise last_exc  # type: ignore[misc]

    def _post(self, path: str, payload: dict) -> dict:
        return self._mutate("POST", path, payload)

    def _put(self, path: str, payload: dict) -> dict:
        return self._mutate("PUT", path, payload)

    def _mutate(self, method: str, path: str, payload: dict) -> dict:
        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            try:
                fn = requests.post if method == "POST" else requests.put
                response = fn(f"{self.api_url}{path}", headers=self._headers(), data=json.dumps(payload), timeout=20)
                if response.status_code in _TRANSIENT_STATUS_CODES and attempt < _MAX_RETRIES - 1:
                    wait = _RETRY_BACKOFF * (2 ** attempt)
                    log.warning("OANDA %s on %s %s \u2013 retry %d/%d in %ds", response.status_code, method, path, attempt + 1, _MAX_RETRIES, wait)
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as exc:
                raise requests.exceptions.HTTPError(
                    _strip_url(str(exc)), response=exc.response,
                ) from None
            except requests.exceptions.ConnectionError as exc:
                last_exc = exc
                if attempt < _MAX_RETRIES - 1:
                    wait = _RETRY_BACKOFF * (2 ** attempt)
                    log.warning("OANDA connection error on %s %s \u2013 retry %d/%d in %ds: %s", method, path, attempt + 1, _MAX_RETRIES, wait, exc)
                    time.sleep(wait)
                    continue
                raise requests.exceptions.ConnectionError(_strip_url(str(exc))) from None
        raise last_exc  # type: ignore[misc]

    @staticmethod
    def _format_price(price: float) -> str:
        return f"{price:.3f}"

    @staticmethod
    def _granularity_to_timedelta(granularity: str) -> timedelta:
        normalized = granularity.strip().upper()
        mapping = {
            "M1": timedelta(minutes=1),
            "M5": timedelta(minutes=5),
            "M15": timedelta(minutes=15),
            "M30": timedelta(minutes=30),
            "H1": timedelta(hours=1),
            "H4": timedelta(hours=4),
            "D": timedelta(days=1),
        }
        if normalized not in mapping:
            raise ValueError(f"Unsupported OANDA granularity for range fetch: {granularity}")
        return mapping[normalized]