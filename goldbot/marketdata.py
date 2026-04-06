import json
import logging
from datetime import datetime, timezone

import pandas as pd
import requests

from goldbot.config import Settings
from goldbot.models import Opportunity


log = logging.getLogger(__name__)


class SpreadTooWideError(RuntimeError):
    pass


class OandaClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.api_url = (
            "https://api-fxpractice.oanda.com"
            if settings.oanda_environment == "practice"
            else "https://api-fxtrade.oanda.com"
        )

    def uses_native_units(self) -> bool:
        return self.settings.execution_mode == "live" and bool(self.settings.oanda_api_key and self.settings.oanda_account_id)

    def get_account_summary(self) -> dict:
        if self.settings.execution_mode in {"signal_only", "paper"} or not self.settings.oanda_account_id:
            return {
                "balance": self.settings.paper_balance,
                "currency": "GBP" if self.settings.account_type == "spread_bet" else "USD",
            }
        payload = self._get(f"/v3/accounts/{self.settings.oanda_account_id}/summary")
        account = payload.get("account", {})
        return {
            "balance": float(account.get("balance", 0.0)),
            "currency": str(account.get("currency", "USD")),
        }

    def fetch_candles(self, instrument: str, granularity: str, count: int) -> pd.DataFrame | None:
        payload = self._get(
            f"/v3/instruments/{instrument}/candles",
            params={"granularity": granularity, "count": count, "price": "M"},
        )
        candles = payload.get("candles", [])
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
        return df.dropna(subset=["close"])

    def get_price(self, instrument: str) -> dict[str, float]:
        if self.settings.execution_mode in {"signal_only", "paper"}:
            df = self.fetch_candles(instrument, "M15", 2)
            if df is None or df.empty:
                return {"bid": 0.0, "ask": 0.0, "mid": 0.0, "spread": 0.0}
            price = float(df["close"].iloc[-1])
            return {"bid": price, "ask": price, "mid": price, "spread": 0.0}
        payload = self._get(
            f"/v3/accounts/{self.settings.oanda_account_id}/pricing",
            params={"instruments": instrument},
        )
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

    def list_open_trades(self) -> list[dict]:
        if self.settings.execution_mode in {"signal_only", "paper"} or not self.settings.oanda_account_id:
            return []
        payload = self._get(f"/v3/accounts/{self.settings.oanda_account_id}/openTrades")
        return list(payload.get("trades", []))

    def calculate_xau_size(self, risk_amount: float, stop_distance: float, account_currency: str) -> float:
        if stop_distance <= 0:
            return 0.0
        if not self.uses_native_units():
            stake = risk_amount / stop_distance
            return max(0.1, round(stake, 2))
        conversion = self._estimate_conversion_rate("USD", account_currency)
        per_unit_risk = stop_distance * conversion
        if per_unit_risk <= 0:
            return 0.0
        return max(1.0, round(risk_amount / per_unit_risk))

    def place_market_order(self, opportunity: Opportunity, size: float) -> dict:
        quote = self.get_price(self.settings.instrument)
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

    def _get(self, path: str, params: dict | None = None) -> dict:
        if self.settings.execution_mode in {"signal_only", "paper"} and path.startswith("/v3/accounts"):
            return {}
        response = requests.get(f"{self.api_url}{path}", headers=self._headers(), params=params, timeout=20)
        response.raise_for_status()
        return response.json()

    def _post(self, path: str, payload: dict) -> dict:
        response = requests.post(f"{self.api_url}{path}", headers=self._headers(), data=json.dumps(payload), timeout=20)
        response.raise_for_status()
        return response.json()

    def _put(self, path: str, payload: dict) -> dict:
        response = requests.put(f"{self.api_url}{path}", headers=self._headers(), data=json.dumps(payload), timeout=20)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _format_price(price: float) -> str:
        return f"{price:.3f}"