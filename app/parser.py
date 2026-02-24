from __future__ import annotations

import json
import logging
import re
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
    network_errors: int = 0
    parser_errors: int = 0
    blocked: bool = False
    batch_start_pack_id: int | None = None
    batch_end_pack_id: int | None = None
    cursor_before: int | None = None
    cursor_after: int | None = None
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

    def _question_passes_filter(self, likes: int, dislikes: int | None) -> bool:
        required_likes = max(self.settings.min_likes, 10)
        if likes < required_likes:
            return False

        policy = self.settings.zero_dislikes_policy
        if dislikes is None or dislikes <= 0:
            if policy == "exclude":
                return False
            if policy == "include_as_infinite_ratio":
                return True
            return likes >= required_likes

        ratio = likes / dislikes
        return ratio > self.settings.likes_dislikes_ratio_min

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
        target_per_level: int,
        ready_by_category: dict[int, int],
        added_by_level: dict[int, int],
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
            return False

        dislikes = None
        if not self._question_passes_filter(likes, dislikes):
            return False

        truedl = pack.get("trueDl") if isinstance(pack.get("trueDl"), list) else []
        c1 = truedl[0] if len(truedl) > 0 and isinstance(truedl[0], (int, float)) else None
        c2 = truedl[1] if len(truedl) > 1 and isinstance(truedl[1], (int, float)) else None
        bucket = difficulty_bucket(c1, c2)
        if bucket is None:
            return False
        if ready_by_category.get(bucket, 0) >= target_per_level:
            return False

        take_num, take_den, take_percent = self._calc_take(q)

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
        added_by_level[bucket] = added_by_level.get(bucket, 0) + 1
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

    def replenish_cursor_batches(
        self,
        db: Session,
        target_per_level: int,
        batch_size: int,
        max_batches: int,
    ) -> ReplenishResult:
        ready_by_category = self.count_ready_by_category(db)
        needed_categories = {level for level in range(1, 11) if ready_by_category.get(level, 0) < target_per_level}
        if not needed_categories:
            return ReplenishResult(added_questions=0, ready_count=sum(ready_by_category.values()), pages_scanned=0)

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
            return result

        current = cursor
        batches_done = 0
        while batches_done < max_batches and current > 0:
            if all(ready_by_category.get(level, 0) >= target_per_level for level in needed_categories):
                break

            batch_start = current
            batch_end = max(current - batch_size + 1, 1)
            result.batch_start_pack_id = batch_start if result.batch_start_pack_id is None else result.batch_start_pack_id
            result.batch_end_pack_id = batch_end

            for pack_id in range(batch_start, batch_end - 1, -1):
                result.packs_checked += 1
                try:
                    pack = self.fetch_pack(pack_id)
                except requests.HTTPError as exc:
                    result.network_errors += 1
                    status = getattr(getattr(exc, "response", None), "status_code", None)
                    if status in {403, 429}:
                        result.blocked = True
                    continue
                except requests.RequestException:
                    result.network_errors += 1
                    continue
                except Exception:
                    result.parser_errors += 1
                    logger.exception("Unexpected parser error on pack id=%s", pack_id)
                    continue

                if not pack:
                    continue

                result.packs_found += 1
                try:
                    self._upsert_pack(db, pack)
                    tours = pack.get("tours") if isinstance(pack.get("tours"), list) else []
                    for tour in tours:
                        questions = tour.get("questions") if isinstance(tour, dict) and isinstance(tour.get("questions"), list) else []
                        for q in questions:
                            try:
                                if self._upsert_question(
                                    db,
                                    pack,
                                    q,
                                    target_per_level,
                                    ready_by_category,
                                    result.questions_added_by_level,
                                ):
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
        return result
