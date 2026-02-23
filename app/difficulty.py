from __future__ import annotations


def difficulty_score(primary: float | None, secondary: float | None) -> float | None:
    values = [v for v in (primary, secondary) if v is not None]
    if not values:
        return None
    return float(sum(values) / len(values))


def difficulty_bucket(primary: float | None, secondary: float | None) -> int | None:
    score = difficulty_score(primary, secondary)
    if score is None:
        return None
    rounded = int(round(score))
    if rounded < 1:
        return 1
    if rounded > 10:
        return 10
    return rounded
