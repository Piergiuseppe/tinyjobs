from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .exceptions import ConfigurationError

MACROS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}
MONTH_NAMES = {
    name: number
    for number, name in enumerate(
        ("jan", "feb", "mar", "apr", "may", "jun",
         "jul", "aug", "sep", "oct", "nov", "dec"),
        start=1,
    )
}
WEEKDAY_NAMES = {
    name: number
    for number, name in enumerate(("sun", "mon", "tue", "wed", "thu", "fri", "sat"))
}
LONGEST_MONTH = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30,
                 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
SEARCH_YEARS = 9
GAP_SEARCH = timedelta(hours=48)
FOLD_SCAN_SECONDS = 3 * 3600
MINUTE = timedelta(minutes=1)


@dataclass(frozen=True, slots=True)
class CronExpression:
    source: str
    minutes: frozenset[int]
    hours: frozenset[int]
    days: frozenset[int]
    months: frozenset[int]
    weekdays: frozenset[int]
    days_restricted: bool
    weekdays_restricted: bool
    follows_clock: bool

    @classmethod
    def parse(cls, source: str) -> "CronExpression":
        text = MACROS.get(source.strip().lower(), source.strip())
        fields = text.split()
        if len(fields) != 5:
            raise _invalid(source, "expected five fields: minute hour day month weekday")
        minute, hour, day, month, weekday = fields
        expression = cls(
            source=source.strip(),
            minutes=_parse_field(source, minute, 0, 59),
            hours=_parse_field(source, hour, 0, 23),
            days=_parse_field(source, day, 1, 31),
            months=_parse_field(source, month, 1, 12, MONTH_NAMES),
            weekdays=frozenset(
                value % 7
                for value in _parse_field(source, weekday, 0, 7, WEEKDAY_NAMES)
            ),
            days_restricted=not day.startswith("*"),
            weekdays_restricted=not weekday.startswith("*"),
            follows_clock=hour.startswith("*"),
        )
        expression._assert_reachable()
        return expression

    def next_after(self, instant: float, zone: tzinfo) -> float:
        by_wall = self._next_by_wall(instant, zone)
        if zone is timezone.utc or not self.follows_clock:
            return by_wall
        return self._next_by_clock(instant, by_wall, zone)

    def matches(self, wall: datetime) -> bool:
        return (
            wall.minute in self.minutes
            and wall.hour in self.hours
            and wall.month in self.months
            and self._day_matches(wall)
        )

    def _next_by_wall(self, instant: float, zone: tzinfo) -> float:
        local = datetime.fromtimestamp(instant, zone).replace(
            tzinfo=None, second=0, microsecond=0
        )
        candidate = local + MINUTE
        while True:
            wall = self._next_wall(candidate)
            moment = _resolve(wall, zone)
            if moment > instant:
                return moment
            candidate = wall + MINUTE

    def _next_by_clock(self, instant: float, limit: float, zone: tzinfo) -> float:
        moment = (int(instant // 60) + 1) * 60.0
        horizon = min(limit, instant + FOLD_SCAN_SECONDS)
        while moment < horizon:
            if self.matches(datetime.fromtimestamp(moment, zone)):
                return moment
            moment += 60.0
        return limit

    def _next_wall(self, start: datetime) -> datetime:
        moment = start
        horizon = start.year + SEARCH_YEARS
        while moment.year <= horizon:
            if moment.month not in self.months:
                moment = _first_of_next_month(moment)
            elif not self._day_matches(moment):
                moment = (moment + timedelta(days=1)).replace(hour=0, minute=0)
            elif moment.hour not in self.hours:
                moment = (moment + timedelta(hours=1)).replace(minute=0)
            elif moment.minute not in self.minutes:
                moment += MINUTE
            else:
                return moment
        raise _invalid(self.source, "it never fires")

    def _day_matches(self, moment: datetime) -> bool:
        in_month = moment.day in self.days
        in_week = (moment.weekday() + 1) % 7 in self.weekdays
        if self.days_restricted and self.weekdays_restricted:
            return in_month or in_week
        return in_month and in_week

    def _assert_reachable(self) -> None:
        if self.weekdays_restricted and self.days_restricted:
            return
        if not any(
            day <= LONGEST_MONTH[month] for month in self.months for day in self.days
        ):
            raise _invalid(self.source, "no month has a day it names, so it never fires")


def load_zone(name: str) -> tzinfo:
    if name.upper() == "UTC":
        return timezone.utc
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        raise ConfigurationError(
            f"unknown timezone {name!r}; use an IANA name such as 'Europe/Rome' "
            f"(on Windows, local timezones need 'pip install tzdata')"
        ) from None


def _parse_field(
    source: str,
    text: str,
    low: int,
    high: int,
    names: dict[str, int] | None = None,
) -> frozenset[int]:
    values: set[int] = set()
    for part in text.lower().split(","):
        base, slash, step_text = part.partition("/")
        step = _number(source, step_text, names=None) if slash else 1
        if step < 1:
            raise _invalid(source, f"step {step_text!r} must be at least 1")
        if base == "*":
            start, end = low, high
        elif "-" in base:
            first, _, last = base.partition("-")
            start, end = _number(source, first, names), _number(source, last, names)
        else:
            start = _number(source, base, names)
            end = high if slash else start
        if not low <= start <= end <= high:
            raise _invalid(source, f"{part!r} is outside {low}-{high}")
        values.update(range(start, end + 1, step))
    return frozenset(values)


def _number(source: str, text: str, names: dict[str, int] | None) -> int:
    if names and text in names:
        return names[text]
    if not text.isdigit():
        raise _invalid(source, f"{text!r} is not a number")
    return int(text)


def _resolve(wall: datetime, zone: tzinfo) -> float:
    probe = wall
    while probe - wall <= GAP_SEARCH:
        aware = probe.replace(tzinfo=zone)
        if aware.astimezone(timezone.utc).astimezone(zone).replace(tzinfo=None) == probe:
            return aware.timestamp()
        probe += MINUTE
    return wall.replace(tzinfo=zone).timestamp()


def _first_of_next_month(moment: datetime) -> datetime:
    if moment.month == 12:
        return datetime(moment.year + 1, 1, 1)
    return datetime(moment.year, moment.month + 1, 1)


def _invalid(source: str, reason: str) -> ConfigurationError:
    return ConfigurationError(f"{source!r} is not a usable cron expression: {reason}")
