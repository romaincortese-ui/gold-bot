from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class CalendarEvent:
    title: str
    currency: str
    impact: str
    occurs_at: datetime
    source: str


@dataclass
class Opportunity:
    strategy: str
    direction: str
    score: float
    entry_price: float
    stop_price: float
    take_profit_price: float | None
    risk_per_unit: float
    rationale: str
    metadata: dict[str, Any] = field(default_factory=dict)
    exit_plan: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BudgetSnapshot:
    account_balance: float
    gold_sleeve_balance: float
    max_trade_risk_amount: float
    max_total_risk_amount: float
    reserved_gold_risk: float
    sibling_fx_reserved_risk: float
    available_gold_risk: float