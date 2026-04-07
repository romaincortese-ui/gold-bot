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
    raise ValueError(f"Unsupported GOLD_SERVICE_ROLE: {role}. Supported roles are worker and macro.")


if __name__ == "__main__":
    main()