from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import Settings
from app.difficulty import difficulty_bucket
from app.models import Pack, Question

logger = logging.getLogger(__name__)

_NEXT_CHUNK_RE = re.compile(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)</script>', re.S)


@dataclass
class ReplenishResult:
    added_questions: int
    ready_count: int
    pages_scanned: int


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

    def _extract_json_after_marker(self, payload: str, marker: str):
        idx = payload.find(marker)
        if idx < 0:
            return None
        start = idx + len(marker)
        return json.JSONDecoder().raw_decode(payload[start:])[0]

    def fetch_pack_ids(self, page: int) -> list[int]:
        html = self._fetch_text(f"https://gotquestions.online/?page={page}")
        payload = self._decode_next_payload(html)
        idx = payload.find('"packs":[{')
        if idx < 0:
            return []
        packs = json.JSONDecoder().raw_decode(payload[idx + len('"packs":'):])[0]
        if not packs:
            return []
        if isinstance(packs, dict):
            packs = [packs]
        out: list[int] = []
        for pack in packs:
            pid = pack.get("id")
            if isinstance(pid, int):
                out.append(pid)
        return out

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
        needed_categories: set[int],
        ready_by_category: dict[int, int],
    ) -> bool:
        question_id = int(q["id"])
        existing = db.get(Question, question_id)
        likes = int(q.get("totalLikes") or 0)

        razdatka_pic = str(q.get("razdatkaPic") or "")
        razdatka_text = str(q.get("razdatkaText") or "")

        if existing is not None:
            # Backfill distributive materials for already parsed questions.
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
        if bucket not in needed_categories:
            return False
        if ready_by_category.get(bucket, 0) >= self.settings.max_ready_questions:
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
        return True

    def count_ready_by_category(self, db: Session) -> dict[int, int]:
        rows = db.execute(
            select(Question.pack_complexity_primary, Question.pack_complexity_secondary)
            .where(Question.is_used.is_(False))
        ).all()
        out = {level: 0 for level in range(1, 11)}
        for c1, c2 in rows:
            bucket = difficulty_bucket(c1, c2)
            if bucket is None:
                continue
            out[bucket] = out.get(bucket, 0) + 1
        return out

    def replenish_if_needed(self, db: Session) -> ReplenishResult:
        ready_by_category = self.count_ready_by_category(db)
        needed_categories = {
            level for level in range(1, 11) if ready_by_category.get(level, 0) <= self.settings.min_ready_questions
        }
        if not needed_categories:
            return ReplenishResult(
                added_questions=0,
                ready_count=sum(ready_by_category.values()),
                pages_scanned=0,
            )

        added = 0
        pages = 0
        for page in range(1, self.settings.parser_max_pages + 1):
            pages = page
            try:
                pack_ids = self.fetch_pack_ids(page)
            except Exception:
                logger.exception("Failed to fetch packs page=%s", page)
                continue

            if not pack_ids:
                break

            for pack_id in pack_ids:
                if all(ready_by_category.get(level, 0) >= self.settings.max_ready_questions for level in needed_categories):
                    db.commit()
                    return ReplenishResult(
                        added_questions=added,
                        ready_count=sum(ready_by_category.values()),
                        pages_scanned=pages,
                    )

                try:
                    pack = self.fetch_pack(pack_id)
                except Exception:
                    logger.exception("Failed to fetch pack id=%s", pack_id)
                    continue

                if not pack:
                    continue

                self._upsert_pack(db, pack)

                tours = pack.get("tours") if isinstance(pack.get("tours"), list) else []
                for tour in tours:
                    questions = tour.get("questions") if isinstance(tour, dict) and isinstance(tour.get("questions"), list) else []
                    for q in questions:
                        try:
                            if self._upsert_question(db, pack, q, needed_categories, ready_by_category):
                                added += 1
                        except Exception:
                            logger.exception("Failed to upsert question in pack id=%s", pack_id)

                db.commit()

        return ReplenishResult(
            added_questions=added,
            ready_count=sum(ready_by_category.values()),
            pages_scanned=pages,
        )
