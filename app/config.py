from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bot_token: str
    bot_mode: str
    database_url: str
    next_delay_sec: int
    replenish_target_per_level: int
    likes_dislikes_ratio_min: float
    min_likes: int
    zero_dislikes_policy: str
    parser_cursor_start_pack_id: int
    parser_batch_size: int
    parser_max_batches_per_run: int
    parser_report_user_id: int | None
    request_timeout_sec: int
    log_level: str
    web_server_host: str
    web_server_port: int
    webhook_base_url: str
    webhook_path: str
    webhook_secret_token: str
    daily_usage_report_interval_sec: int



def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)



def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    return float(value)


def _env_optional_int(name: str, default: int | None = None) -> int | None:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    if not value:
        return default
    return int(value)



def get_settings() -> Settings:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN is required")

    webhook_base_url = os.getenv("WEBHOOK_BASE_URL", "").strip()
    koyeb_domain = os.getenv("KOYEB_PUBLIC_DOMAIN", "").strip()
    if not webhook_base_url and koyeb_domain:
        webhook_base_url = f"https://{koyeb_domain}"

    database_url = os.getenv("DATABASE_URL", "sqlite:///./data/chgk_bot.db").strip()
    # Koyeb/other providers often expose postgres URL without explicit driver.
    # Normalize to psycopg driver since we ship psycopg binary package.
    if database_url.startswith("postgres://"):
        database_url = "postgresql+psycopg://" + database_url[len("postgres://") :]
    elif database_url.startswith("postgresql://") and "+psycopg" not in database_url:
        database_url = "postgresql+psycopg://" + database_url[len("postgresql://") :]

    return Settings(
        bot_token=token,
        bot_mode=os.getenv("BOT_MODE", "polling").strip().lower(),
        database_url=database_url,
        next_delay_sec=_env_int("NEXT_DELAY_SEC", 2),
        replenish_target_per_level=_env_int("REPLENISH_TARGET_PER_LEVEL", 100),
        likes_dislikes_ratio_min=_env_float("LIKES_DISLIKES_RATIO_MIN", 15.0),
        min_likes=_env_int("MIN_LIKES", 5),
        zero_dislikes_policy=os.getenv("ZERO_DISLIKES_POLICY", "fallback").strip().lower(),
        parser_cursor_start_pack_id=_env_int("PARSER_CURSOR_START_PACK_ID", 6300),
        parser_batch_size=_env_int("PARSER_BATCH_SIZE", 500),
        parser_max_batches_per_run=_env_int("PARSER_MAX_BATCHES_PER_RUN", 3),
        parser_report_user_id=_env_optional_int("PARSER_REPORT_USER_ID", 221749482),
        request_timeout_sec=_env_int("REQUEST_TIMEOUT_SEC", 20),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        web_server_host=os.getenv("WEB_SERVER_HOST", "0.0.0.0"),
        web_server_port=_env_int("WEB_SERVER_PORT", _env_int("PORT", 8080)),
        webhook_base_url=webhook_base_url,
        webhook_path=os.getenv("WEBHOOK_PATH", "/telegram/webhook").strip(),
        webhook_secret_token=os.getenv("WEBHOOK_SECRET_TOKEN", "").strip(),
        daily_usage_report_interval_sec=_env_int("DAILY_USAGE_REPORT_INTERVAL_SEC", 86400),
    )
