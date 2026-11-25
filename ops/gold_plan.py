"""Choose a bounded pilot from any requested trace-day selection."""

PREFERRED_PILOT_DAYS = (3, 5, 109, 173)


def pilot_days(days):
    selected = sorted(set(days))
    if not selected or selected[0] < 0 or selected[-1] > 184:
        raise ValueError("Select trace days 0–184")
    preferred = [day for day in PREFERRED_PILOT_DAYS if day in selected]
    if preferred:
        return preferred
    # At most four spread-out observations when the historical pilot is absent.
    indices = {round(i * (len(selected) - 1) / 3) for i in range(4)}
    return [selected[i] for i in sorted(indices)]


def remaining_days(days):
    selected = sorted(set(days))
    pilot = set(pilot_days(selected))
    return [day for day in selected if day not in pilot]
