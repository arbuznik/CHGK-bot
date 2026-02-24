from __future__ import annotations

import argparse
import asyncio
import logging

from aiogram import Bot
from dotenv import load_dotenv

from app.config import get_settings
from app.db import build_session_factory
from app.logging_setup import setup_logging
from app.main import init_db
from app.services import PoolService

logger = logging.getLogger(__name__)


def format_report(result) -> str:
    levels = []
    for level in range(1, 11):
        levels.append(f"{level}:{result.questions_added_by_level.get(level, 0)}")
    excluded_total = (
        result.questions_existing
        + result.questions_filtered_likes
        + result.questions_filtered_bucket_missing
    )
    return (
        "Отчет парсера (ручной CLI запуск)\n"
        f"Время: {result.duration_sec:.2f} сек\n"
        f"Добавлено вопросов: {result.added_questions}\n"
        f"Паков проверено: {result.packs_checked}\n"
        f"Паков найдено: {result.packs_found}\n"
        f"Паков не найдено (404/пусто): {result.packs_not_found}\n"
        f"Паков с HTTP-ошибками: {result.packs_failed_http}\n"
        f"Батчей: {result.pages_scanned}\n"
        f"Курсор: {result.cursor_before} -> {result.cursor_after}\n"
        f"Сетевые ошибки: {result.network_errors}\n"
        f"Сетевые ретраи: {result.network_retries}\n"
        f"Ошибки парсера: {result.parser_errors}\n"
        f"Блокировка (403/429): {'да' if result.blocked else 'нет'}\n"
        f"Вопросов найдено всего: {result.questions_seen_total}\n"
        f"Вопросов отсечено всего: {excluded_total}\n"
        f"Отсечено как уже существующие: {result.questions_existing}\n"
        f"Отсечено по фильтру лайков/рейтинга: {result.questions_filtered_likes}\n"
        f"Отсечено без валидной сложности: {result.questions_filtered_bucket_missing}\n"
        f"Добавлено по уровням: {' | '.join(levels)}"
    )


async def main() -> None:
    cli = argparse.ArgumentParser(description="Run one-off parser batch")
    cli.add_argument("--cursor-start", type=int, default=None, help="Reset parser cursor before run")
    cli.add_argument("--batch-size", type=int, default=None, help="Batch size override")
    cli.add_argument("--max-batches", type=int, default=1, help="How many batches to run")
    args = cli.parse_args()

    load_dotenv()
    settings = get_settings()
    setup_logging(settings.log_level)

    session_factory = build_session_factory(settings.database_url)
    init_db(session_factory)

    pool = PoolService(settings, session_factory)

    if args.cursor_start is not None:
        with session_factory() as db:
            pool.parser.set_cursor(db, args.cursor_start)

    batch_size = args.batch_size if args.batch_size is not None else settings.parser_batch_size
    max_batches = args.max_batches if args.max_batches > 0 else 1

    result = await asyncio.to_thread(
        lambda: _run_sync(pool, batch_size=batch_size, max_batches=max_batches)
    )

    report = format_report(result)
    logger.info(report)
    print(report)

    if settings.parser_report_user_id is None:
        return

    bot = Bot(token=settings.bot_token)
    try:
        await bot.send_message(chat_id=settings.parser_report_user_id, text=report)
    finally:
        await bot.session.close()


def _run_sync(pool: PoolService, batch_size: int, max_batches: int):
    with pool.session_factory() as db:
        return pool.parser.replenish_cursor_batches(
            db,
            target_per_level=pool.settings.replenish_target_per_level,
            batch_size=batch_size,
            max_batches=max_batches,
        )


if __name__ == "__main__":
    asyncio.run(main())
