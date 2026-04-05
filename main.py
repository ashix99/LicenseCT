from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from bot_app import ActivationBotApp
from settings import Settings


def setup_logging(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("activation_bot")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


async def run_bot() -> None:
    settings = Settings.from_env()
    logger = setup_logging(settings.log_path)
    app = ActivationBotApp(settings, logger)
    await app.run()


if __name__ == "__main__":
    asyncio.run(run_bot())
