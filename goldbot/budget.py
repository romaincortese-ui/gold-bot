import os
from datetime import datetime, timezone
from pathlib import Path

from goldbot.config import Settings
from goldbot.models import BudgetSnapshot
from goldbot.shared_backend import load_json_payload, merge_bot_budget_slot, save_json_payload


class SharedBudgetManager:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.path = Path(settings.shared_budget_file)
        self.redis_key = os.getenv("GOLD_SHARED_BUDGET_KEY", os.getenv("SHARED_BUDGET_KEY", "shared_budget_state")).strip()

    def build_snapshot(self, account_balance: float) -> BudgetSnapshot:
        payload = self._load()
        bots = payload.get("bots", {})
        gold_reserved = float(bots.get("gold", {}).get("reserved_risk", 0.0) or 0.0)
        # Accounts are separated per-bot (Gold-bot and FX-bot run on dedicated
        # OANDA sub-accounts). Sibling reserved-risk is therefore 0 by
        # definition — the FX bot's positions cannot consume Gold-bot account
        # margin.
        fx_reserved = 0.0
        gold_sleeve_balance = account_balance * self.settings.gold_budget_allocation
        max_trade_risk_amount = gold_sleeve_balance * self.settings.max_risk_per_trade
        max_total_risk_amount = gold_sleeve_balance * self.settings.max_total_gold_risk
        return BudgetSnapshot(
            account_balance=account_balance,
            gold_sleeve_balance=gold_sleeve_balance,
            max_trade_risk_amount=max_trade_risk_amount,
            max_total_risk_amount=max_total_risk_amount,
            reserved_gold_risk=gold_reserved,
            sibling_fx_reserved_risk=fx_reserved,
            available_gold_risk=max(0.0, max_total_risk_amount - gold_reserved),
        )

    def reserve_gold_risk(self, trade_id: str, risk_amount: float, strategy: str) -> None:
        payload = self._load()
        bots = payload.setdefault("bots", {})
        gold = bots.setdefault("gold", {"reserved_risk": 0.0, "trades": {}})
        trades = gold.setdefault("trades", {})
        if trade_id not in trades:
            gold["reserved_risk"] = float(gold.get("reserved_risk", 0.0)) + float(risk_amount)
        trades[trade_id] = {"risk_amount": float(risk_amount), "strategy": strategy}
        gold["updated_at"] = datetime.now(timezone.utc).isoformat()
        merge_bot_budget_slot(str(self.path), self.redis_key, "gold", gold)

    def release_gold_risk(self, trade_id: str) -> None:
        payload = self._load()
        bots = payload.get("bots", {})
        gold = bots.get("gold", {})
        trades = gold.get("trades", {})
        trade = trades.pop(trade_id, None)
        if trade is not None:
            gold["reserved_risk"] = max(0.0, float(gold.get("reserved_risk", 0.0)) - float(trade.get("risk_amount", 0.0)))
            gold["updated_at"] = datetime.now(timezone.utc).isoformat()
            merge_bot_budget_slot(str(self.path), self.redis_key, "gold", gold)

    def _load(self) -> dict:
        return load_json_payload(str(self.path), self.redis_key, {"bots": {}})