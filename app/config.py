from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bot_token: str
    bot_mode: str
    database_url: str
    next_delay_sec: int
    min_ready_questions: int
    max_ready_questions: int
    likes_dislikes_ratio_min: float
    min_likes: int
    zero_dislikes_policy: str
    parser_max_pages: int
    request_timeout_sec: int
    log_level: str
    web_server_host: str
    web_server_port: int
    webhook_base_url: str
    webhook_path: str
    webhook_secret_token: str



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



def get_settings() -> Settings:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN is required")

    webhook_base_url = os.getenv("WEBHOOK_BASE_URL", "").strip()
    koyeb_domain = os.getenv("KOYEB_PUBLIC_DOMAIN", "").strip()
    if not webhook_base_url and koyeb_domain:
        webhook_base_url = f"https://{koyeb_domain}"

    return Settings(
        bot_token=token,
        bot_mode=os.getenv("BOT_MODE", "polling").strip().lower(),
        database_url=os.getenv("DATABASE_URL", "sqlite:///./data/chgk_bot.db"),
        next_delay_sec=_env_int("NEXT_DELAY_SEC", 2),
        min_ready_questions=_env_int("MIN_READY_QUESTIONS", 20),
        max_ready_questions=_env_int("MAX_READY_QUESTIONS", 50),
        likes_dislikes_ratio_min=_env_float("LIKES_DISLIKES_RATIO_MIN", 15.0),
        min_likes=_env_int("MIN_LIKES", 5),
        zero_dislikes_policy=os.getenv("ZERO_DISLIKES_POLICY", "fallback").strip().lower(),
        parser_max_pages=_env_int("PARSER_MAX_PAGES", 200),
        request_timeout_sec=_env_int("REQUEST_TIMEOUT_SEC", 20),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        web_server_host=os.getenv("WEB_SERVER_HOST", "0.0.0.0"),
        web_server_port=_env_int("WEB_SERVER_PORT", _env_int("PORT", 8080)),
        webhook_base_url=webhook_base_url,
        webhook_path=os.getenv("WEBHOOK_PATH", "/telegram/webhook").strip(),
        webhook_secret_token=os.getenv("WEBHOOK_SECRET_TOKEN", "").strip(),
    )
