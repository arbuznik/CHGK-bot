from __future__ import annotations

import asyncio
import logging

from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from dotenv import load_dotenv

from app.bot_app import BotApp
from app.config import get_settings
from app.db import build_session_factory
from app.logging_setup import setup_logging
from app.models import Base
from app.services import GameService, PoolService

logger = logging.getLogger(__name__)


def init_db(session_factory) -> None:
    engine = session_factory.kw["bind"]
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        # Ensure supergroup chat IDs fit Postgres type (-100xxxxxxxxxx needs BIGINT).
        if engine.dialect.name == "postgresql":
            conn.exec_driver_sql(
                "ALTER TABLE IF EXISTS chat_sessions "
                "ALTER COLUMN chat_id TYPE BIGINT"
            )
            col_exists = conn.exec_driver_sql(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_name='chat_sessions' AND column_name='selected_difficulty'"
            ).scalar_one_or_none()
            if col_exists is None:
                conn.exec_driver_sql(
                    "ALTER TABLE chat_sessions "
                    "ADD COLUMN selected_difficulty INTEGER NULL"
                )
            min_likes_col = conn.exec_driver_sql(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_name='chat_sessions' AND column_name='selected_min_likes'"
            ).scalar_one_or_none()
            if min_likes_col is None:
                conn.exec_driver_sql(
                    "ALTER TABLE chat_sessions "
                    "ADD COLUMN selected_min_likes INTEGER NOT NULL DEFAULT 1"
                )
            min_take_col = conn.exec_driver_sql(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_name='chat_sessions' AND column_name='selected_min_take_percent'"
            ).scalar_one_or_none()
            if min_take_col is None:
                conn.exec_driver_sql(
                    "ALTER TABLE chat_sessions "
                    "ADD COLUMN selected_min_take_percent DOUBLE PRECISION NOT NULL DEFAULT 20.0"
                )
            q_col_exists = conn.exec_driver_sql(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_name='questions' AND column_name='razdatka_text'"
            ).scalar_one_or_none()
            if q_col_exists is None:
                conn.exec_driver_sql(
                    "ALTER TABLE questions "
                    "ADD COLUMN razdatka_text TEXT"
                )
        elif engine.dialect.name == "sqlite":
            rows = conn.exec_driver_sql("PRAGMA table_info(chat_sessions)").fetchall()
            has_col = any(r[1] == "selected_difficulty" for r in rows)
            if not has_col:
                conn.exec_driver_sql(
                    "ALTER TABLE chat_sessions "
                    "ADD COLUMN selected_difficulty INTEGER"
                )
            has_min_likes = any(r[1] == "selected_min_likes" for r in rows)
            if not has_min_likes:
                conn.exec_driver_sql(
                    "ALTER TABLE chat_sessions "
                    "ADD COLUMN selected_min_likes INTEGER NOT NULL DEFAULT 1"
                )
            has_min_take = any(r[1] == "selected_min_take_percent" for r in rows)
            if not has_min_take:
                conn.exec_driver_sql(
                    "ALTER TABLE chat_sessions "
                    "ADD COLUMN selected_min_take_percent REAL NOT NULL DEFAULT 20.0"
                )
            q_rows = conn.exec_driver_sql("PRAGMA table_info(questions)").fetchall()
            has_q_col = any(r[1] == "razdatka_text" for r in q_rows)
            if not has_q_col:
                conn.exec_driver_sql(
                    "ALTER TABLE questions "
                    "ADD COLUMN razdatka_text TEXT"
                )


async def async_main() -> None:
    load_dotenv()
    settings = get_settings()
    setup_logging(settings.log_level)

    session_factory = build_session_factory(settings.database_url)
    init_db(session_factory)

    pool = PoolService(settings, session_factory)
    game = GameService(settings, session_factory, pool)
    app = BotApp(settings, game)

    if settings.bot_mode == "polling":
        await app.run_polling()
        return

    if settings.bot_mode != "webhook":
        raise RuntimeError("BOT_MODE must be either 'polling' or 'webhook'")

    if not settings.webhook_base_url:
        raise RuntimeError("WEBHOOK_BASE_URL is required when BOT_MODE=webhook")

    webhook_path = settings.webhook_path if settings.webhook_path.startswith("/") else f"/{settings.webhook_path}"
    webhook_url = f"{settings.webhook_base_url.rstrip('/')}{webhook_path}"

    aio_app = web.Application()

    async def healthcheck(_: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    aio_app.router.add_get("/healthz", healthcheck)
    aio_app.router.add_get("/", healthcheck)

    request_handler = SimpleRequestHandler(
        dispatcher=app.dp,
        bot=app.bot,
        secret_token=settings.webhook_secret_token or None,
    )
    request_handler.register(aio_app, path=webhook_path)
    setup_application(aio_app, app.dp, bot=app.bot)

    async def on_startup(_: web.Application) -> None:
        await app.setup_commands_menu()
        await app.start_background_tasks()
        await app.bot.set_webhook(
            url=webhook_url,
            secret_token=settings.webhook_secret_token or None,
            drop_pending_updates=True,
        )
        logger.info("Webhook set: %s", webhook_url)

    async def on_shutdown(_: web.Application) -> None:
        await app.shutdown_background_tasks()
        await app.bot.delete_webhook()

    aio_app.on_startup.append(on_startup)
    aio_app.on_shutdown.append(on_shutdown)

    runner = web.AppRunner(aio_app)
    await runner.setup()
    site = web.TCPSite(runner, host=settings.web_server_host, port=settings.web_server_port)
    await site.start()
    logger.info("Webhook server started on %s:%s", settings.web_server_host, settings.web_server_port)

    stop_event = asyncio.Event()
    await stop_event.wait()


if __name__ == "__main__":
    asyncio.run(async_main())
