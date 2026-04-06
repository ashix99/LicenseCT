from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from bot_app import ActivationBotApp
from settings import Settings


def setup_logging(log_path: Path, logger_name: str) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

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


async def run_bots() -> None:
    settings_list = Settings.load_all_from_env()
    apps: list[ActivationBotApp] = []

    for settings in settings_list:
        logger = setup_logging(
            settings.log_path,
            logger_name=f"activation_bot.bot{settings.bot_index}",
        )
        logger.info("Bot instance prepared bot_index=%s", settings.bot_index)
        apps.append(ActivationBotApp(settings, logger))

    await asyncio.gather(*(app.run() for app in apps))


if __name__ == "__main__":
    asyncio.run(run_bots())
