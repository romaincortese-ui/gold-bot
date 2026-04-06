import os

from goldbot.runtime import GoldBotRuntime
from goldbot.telegram import run_telegram_bot
from macro_engine import main as run_macro_engine


def main() -> None:
    role = os.getenv("GOLD_SERVICE_ROLE", "worker").strip().lower()
    if role == "worker":
        GoldBotRuntime().run_forever()
        return
    if role == "macro":
        run_macro_engine()
        return
    if role == "telegram":
        run_telegram_bot()
        return
    raise ValueError(f"Unsupported GOLD_SERVICE_ROLE: {role}")


if __name__ == "__main__":
    main()