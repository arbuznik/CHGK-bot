from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import Settings
from app.difficulty import difficulty_bucket
from app.models import Pack, ParserState, Question

logger = logging.getLogger(__name__)

_NEXT_CHUNK_RE = re.compile(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)</script>', re.S)


@dataclass
class ReplenishResult:
    added_questions: int
    ready_count: int
    pages_scanned: int
    packs_checked: int = 0
    packs_found: int = 0
    packs_not_found: int = 0
    packs_failed_http: int = 0
    network_errors: int = 0
    network_retries: int = 0
    parser_errors: int = 0
    blocked: bool = False
    duration_sec: float = 0.0
    batch_start_pack_id: int | None = None
    batch_end_pack_id: int | None = None
    cursor_before: int | None = None
    cursor_after: int | None = None
    questions_seen_total: int = 0
    questions_existing: int = 0
    questions_filtered_likes: int = 0
    questions_filtered_bucket_missing: int = 0
    questions_added_by_level: dict[int, int] = field(default_factory=dict)


class GotQuestionsParser:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.timeout = settings.request_timeout_sec

    def _fetch_text(self, url: str) -> str:
        resp = requests.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.text

    def _decode_next_payload(self, html: str) -> str:
        parts: list[str] = []
        for match in _NEXT_CHUNK_RE.finditer(html):
            chunk = match.group(1)
            try:
                parts.append(json.loads(f'"{chunk}"'))
            except json.JSONDecodeError:
                continue
        return "".join(parts)

    def fetch_pack(self, pack_id: int) -> dict | None:
        html = self._fetch_text(f"https://gotquestions.online/pack/{pack_id}")
        payload = self._decode_next_payload(html)
        idx = payload.find('"pack":{"id":')
        if idx < 0:
            return None
        pack = json.JSONDecoder().raw_decode(payload[idx + len('"pack":'):])[0]
        if not isinstance(pack, dict):
            return None
        return pack

    def _fetch_pack_with_retry(self, pack_id: int, result: ReplenishResult, max_retries: int = 2) -> tuple[dict | None, str]:
        attempt = 0
        while True:
            try:
                return self.fetch_pack(pack_id), "ok"
            except requests.HTTPError as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status == 404:
                    return None, "not_found"
                if status in {403, 429}:
                    result.blocked = True
                if status is not None and status < 500 and status not in {403, 429}:
                    return None, f"http_{status}"
                if attempt >= max_retries:
                    return None, f"http_{status or 'unknown'}"
                result.network_retries += 1
                attempt += 1
                time.sleep(0.25 * attempt)
            except requests.RequestException:
                if attempt >= max_retries:
                    return None, "network_error"
                result.network_retries += 1
                attempt += 1
                time.sleep(0.25 * attempt)

    def _question_passes_filter(
        self,
        likes: int,
        take_num: int | None,
        take_den: int | None,
        take_percent: float | None,
    ) -> bool:
        if likes >= 1:
            return True
        if (
            take_den is not None
            and take_den >= 10
            and take_percent is not None
            and 20.0 <= take_percent <= 90.0
        ):
            return True
        return False

    def _calc_take(self, q: dict) -> tuple[int | None, int | None, float | None]:
        correct = q.get("correct_answers")
        teams = q.get("teams")
        if not isinstance(correct, list) or not correct:
            return None, None, None
        if not isinstance(teams, list) or not teams:
            return None, None, None

        num = correct[0] if isinstance(correct[0], int) else None
        den = teams[0] if isinstance(teams[0], int) else None
        if not num or not den:
            return num, den, None
        return num, den, (num / den) * 100.0

    def _upsert_pack(self, db: Session, pack: dict) -> Pack:
        pack_id = int(pack["id"])
        existing = db.get(Pack, pack_id)
        truedl = pack.get("trueDl") if isinstance(pack.get("trueDl"), list) else []
        c1 = truedl[0] if len(truedl) > 0 and isinstance(truedl[0], (int, float)) else None
        c2 = truedl[1] if len(truedl) > 1 and isinstance(truedl[1], (int, float)) else None
        now = datetime.utcnow()

        if existing is None:
            existing = Pack(
                pack_id=pack_id,
                title=str(pack.get("title", "")),
                pub_date=str(pack.get("pubDate", "")),
                complexity_primary=c1,
                complexity_secondary=c2,
                source_url=f"https://gotquestions.online/pack/{pack_id}",
                updated_at=now,
            )
            db.add(existing)
            return existing

        existing.title = str(pack.get("title", existing.title))
        existing.pub_date = str(pack.get("pubDate", existing.pub_date))
        existing.complexity_primary = c1
        existing.complexity_secondary = c2
        existing.source_url = f"https://gotquestions.online/pack/{pack_id}"
        existing.updated_at = now
        return existing

    def _upsert_question(
        self,
        db: Session,
        pack: dict,
        q: dict,
        ready_by_category: dict[int, int],
        result: ReplenishResult,
    ) -> bool:
        question_id = int(q["id"])
        existing = db.get(Question, question_id)
        likes = int(q.get("totalLikes") or 0)

        razdatka_pic = str(q.get("razdatkaPic") or "")
        razdatka_text = str(q.get("razdatkaText") or "")

        if existing is not None:
            changed = False
            if not existing.razdatka_pic_url and razdatka_pic:
                existing.razdatka_pic_url = razdatka_pic
                changed = True
            if not existing.razdatka_text and razdatka_text:
                existing.razdatka_text = razdatka_text
                changed = True
            if changed:
                existing.updated_at = datetime.utcnow()
            result.questions_existing += 1
            return False

        dislikes = None
        take_num, take_den, take_percent = self._calc_take(q)
        if not self._question_passes_filter(likes, take_num, take_den, take_percent):
            result.questions_filtered_likes += 1
            return False

        truedl = pack.get("trueDl") if isinstance(pack.get("trueDl"), list) else []
        c1 = truedl[0] if len(truedl) > 0 and isinstance(truedl[0], (int, float)) else None
        c2 = truedl[1] if len(truedl) > 1 and isinstance(truedl[1], (int, float)) else None
        bucket = difficulty_bucket(c1, c2)
        if bucket is None:
            result.questions_filtered_bucket_missing += 1
            return False

        row = Question(
            question_id=question_id,
            pack_id=int(pack["id"]),
            number_in_pack=int(q.get("number") or 0),
            text=str(q.get("text") or ""),
            source_url=f"https://gotquestions.online/question/{question_id}",
            razdatka_pic_url=razdatka_pic,
            razdatka_text=razdatka_text,
            answer=str(q.get("answer") or ""),
            zachet=str(q.get("zachet") or ""),
            comment=str(q.get("comment") or ""),
            sources=str(q.get("source") or ""),
            likes=likes,
            dislikes=dislikes,
            take_num=take_num,
            take_den=take_den,
            take_percent=take_percent,
            pack_complexity_primary=c1,
            pack_complexity_secondary=c2,
            is_used=False,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db.add(row)
        ready_by_category[bucket] = ready_by_category.get(bucket, 0) + 1
        result.questions_added_by_level[bucket] = result.questions_added_by_level.get(bucket, 0) + 1
        return True

    def count_ready_by_category(self, db: Session) -> dict[int, int]:
        rows = db.execute(select(Question.pack_complexity_primary, Question.pack_complexity_secondary)).all()
        out = {level: 0 for level in range(1, 11)}
        for c1, c2 in rows:
            bucket = difficulty_bucket(c1, c2)
            if bucket is None:
                continue
            out[bucket] = out.get(bucket, 0) + 1
        return out

    def _get_or_create_state(self, db: Session) -> ParserState:
        state = db.get(ParserState, "main")
        if state is None:
            state = ParserState(
                key="main",
                cursor_pack_id=self.settings.parser_cursor_start_pack_id,
                updated_at=datetime.utcnow(),
            )
            db.add(state)
            db.flush()
        return state

    def set_cursor(self, db: Session, cursor_pack_id: int) -> None:
        state = self._get_or_create_state(db)
        state.cursor_pack_id = cursor_pack_id
        state.updated_at = datetime.utcnow()
        db.commit()

    def replenish_cursor_batches(
        self,
        db: Session,
        target_per_level: int,
        batch_size: int,
        max_batches: int,
    ) -> ReplenishResult:
        started = time.monotonic()
        ready_by_category = self.count_ready_by_category(db)

        state = self._get_or_create_state(db)
        cursor = state.cursor_pack_id or 0
        result = ReplenishResult(
            added_questions=0,
            ready_count=sum(ready_by_category.values()),
            pages_scanned=0,
            cursor_before=cursor,
            cursor_after=cursor,
            questions_added_by_level={level: 0 for level in range(1, 11)},
        )

        if cursor <= 0:
            result.duration_sec = round(time.monotonic() - started, 2)
            return result

        current = cursor
        batches_done = 0
        while batches_done < max_batches and current > 0:
            batch_start = current
            batch_end = max(current - batch_size + 1, 1)
            if result.batch_start_pack_id is None:
                result.batch_start_pack_id = batch_start
            result.batch_end_pack_id = batch_end

            for pack_id in range(batch_start, batch_end - 1, -1):
                result.packs_checked += 1
                try:
                    pack, fetch_status = self._fetch_pack_with_retry(pack_id, result)
                except Exception:
                    result.parser_errors += 1
                    logger.exception("Unexpected fetch wrapper error on pack id=%s", pack_id)
                    continue

                if fetch_status == "not_found":
                    result.packs_not_found += 1
                    continue
                if fetch_status.startswith("http_"):
                    result.packs_failed_http += 1
                    result.network_errors += 1
                    continue
                if fetch_status == "network_error":
                    result.network_errors += 1
                    continue

                if not pack:
                    result.packs_not_found += 1
                    continue

                result.packs_found += 1
                try:
                    self._upsert_pack(db, pack)
                    tours = pack.get("tours") if isinstance(pack.get("tours"), list) else []
                    for tour in tours:
                        questions = tour.get("questions") if isinstance(tour, dict) and isinstance(tour.get("questions"), list) else []
                        for q in questions:
                            result.questions_seen_total += 1
                            try:
                                if self._upsert_question(db, pack, q, ready_by_category, result):
                                    result.added_questions += 1
                            except Exception:
                                result.parser_errors += 1
                                logger.exception("Failed to upsert question in pack id=%s", pack_id)
                except Exception:
                    result.parser_errors += 1
                    logger.exception("Failed to process pack id=%s", pack_id)

            current = batch_end - 1
            state.cursor_pack_id = current
            state.updated_at = datetime.utcnow()
            db.commit()
            batches_done += 1

        result.cursor_after = state.cursor_pack_id
        result.ready_count = sum(ready_by_category.values())
        result.pages_scanned = batches_done
        result.duration_sec = round(time.monotonic() - started, 2)
        return result
