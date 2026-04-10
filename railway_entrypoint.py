import os

from goldbot.runtime import GoldBotRuntime
from macro_engine import main as run_macro_engine


def main() -> None:
    role = os.getenv("GOLD_SERVICE_ROLE", "worker").strip().lower()
    if role == "worker":
        GoldBotRuntime().run_forever()
        return
    if role == "macro":
        run_macro_engine()
        return
    if role == "calibration":
        from run_daily_calibration import main as run_calibration
        run_calibration()
        return
    if role == "telegram":
        raise RuntimeError(
            "The standalone Telegram service has been removed. "
            "Telegram is now embedded in the worker. "
            "Delete this Railway service and keep only the worker."
        )
    raise ValueError(f"Unsupported GOLD_SERVICE_ROLE: {role}. Supported roles are worker, macro, and calibration.")


if __name__ == "__main__":
    main()