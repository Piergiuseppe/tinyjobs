from __future__ import annotations

import unittest
from datetime import datetime, timezone

from tinyjobs.cron import CronExpression, load_zone
from tinyjobs.exceptions import ConfigurationError

ROME = "Europe/Rome"


def instant(text: str, zone: str = "UTC") -> float:
    return datetime.fromisoformat(text).replace(tzinfo=load_zone(zone)).timestamp()


def firings(expression: str, start: str, count: int, zone: str = "UTC") -> list[str]:
    cron = CronExpression.parse(expression)
    tz = load_zone(zone)
    moment = instant(start, zone)
    found = []
    for _ in range(count):
        moment = cron.next_after(moment, tz)
        found.append(datetime.fromtimestamp(moment, tz).isoformat())
    return found


class ParsingTests(unittest.TestCase):
    def test_a_star_covers_the_whole_range(self):
        cron = CronExpression.parse("* * * * *")
        self.assertEqual(cron.minutes, frozenset(range(60)))
        self.assertEqual(cron.weekdays, frozenset(range(7)))

    def test_ranges_steps_and_lists(self):
        cron = CronExpression.parse("1-10/3,30 */6 1,15 * *")
        self.assertEqual(cron.minutes, frozenset({1, 4, 7, 10, 30}))
        self.assertEqual(cron.hours, frozenset({0, 6, 12, 18}))
        self.assertEqual(cron.days, frozenset({1, 15}))

    def test_a_start_with_a_step_runs_to_the_end_of_the_range(self):
        self.assertEqual(
            CronExpression.parse("50/5 * * * *").minutes, frozenset({50, 55})
        )

    def test_names_are_case_insensitive(self):
        cron = CronExpression.parse("0 9 * JAN-mar Mon-FRI")
        self.assertEqual(cron.months, frozenset({1, 2, 3}))
        self.assertEqual(cron.weekdays, frozenset({1, 2, 3, 4, 5}))

    def test_seven_is_sunday_too(self):
        self.assertEqual(CronExpression.parse("0 0 * * 7").weekdays, frozenset({0}))
        self.assertEqual(
            CronExpression.parse("0 0 * * 5-7").weekdays, frozenset({5, 6, 0})
        )

    def test_macros_expand(self):
        self.assertEqual(
            firings("@hourly", "2026-10-10T10:07", 2),
            ["2026-10-10T11:00:00+00:00", "2026-10-10T12:00:00+00:00"],
        )
        self.assertEqual(
            CronExpression.parse("@daily").hours, CronExpression.parse("@midnight").hours
        )

    def test_garbage_is_rejected_with_the_reason(self):
        cases = {
            "* * * *": "five fields",
            "60 * * * *": "outside 0-59",
            "* 24 * * *": "outside 0-23",
            "*/0 * * * *": "at least 1",
            "5-1 * * * *": "outside",
            "x * * * *": "not a number",
            "1,,2 * * * *": "not a number",
            "0 0 * * funday": "not a number",
        }
        for expression, reason in cases.items():
            with self.subTest(expression=expression):
                with self.assertRaises(ConfigurationError) as caught:
                    CronExpression.parse(expression)
                self.assertIn(reason, str(caught.exception))

    def test_a_date_that_never_exists_is_rejected(self):
        with self.assertRaises(ConfigurationError) as caught:
            CronExpression.parse("0 0 30 2 *")
        self.assertIn("never fires", str(caught.exception))

    def test_an_impossible_date_is_fine_when_a_weekday_can_match(self):
        self.assertEqual(
            firings("0 0 30 2 mon", "2026-10-10T00:00", 1),
            ["2027-02-01T00:00:00+00:00"],
        )

    def test_unknown_timezones_are_rejected(self):
        with self.assertRaises(ConfigurationError):
            load_zone("Mars/Olympus_Mons")

    def test_utc_needs_no_tz_database(self):
        self.assertIs(load_zone("utc"), timezone.utc)


class NextFiringTests(unittest.TestCase):
    def test_the_next_firing_is_strictly_after_the_instant(self):
        self.assertEqual(
            firings("*/15 * * * *", "2026-10-10T10:15", 2),
            ["2026-10-10T10:30:00+00:00", "2026-10-10T10:45:00+00:00"],
        )

    def test_seconds_inside_a_minute_do_not_skip_it(self):
        cron = CronExpression.parse("* * * * *")
        moment = instant("2026-10-10T10:15") + 59.9
        self.assertEqual(
            cron.next_after(moment, timezone.utc), instant("2026-10-10T10:16")
        )

    def test_weekdays_only(self):
        self.assertEqual(
            firings("0 9 * * mon-fri", "2026-10-09T10:00", 3),
            [
                "2026-10-12T09:00:00+00:00",
                "2026-10-13T09:00:00+00:00",
                "2026-10-14T09:00:00+00:00",
            ],
        )

    def test_day_and_weekday_together_mean_either(self):
        self.assertEqual(
            firings("0 0 13 * fri", "2026-02-01T00:00", 3),
            [
                "2026-02-06T00:00:00+00:00",
                "2026-02-13T00:00:00+00:00",
                "2026-02-20T00:00:00+00:00",
            ],
        )

    def test_a_starred_weekday_step_keeps_day_semantics(self):
        self.assertEqual(
            firings("0 0 1 * */1", "2026-01-15T00:00", 2),
            ["2026-02-01T00:00:00+00:00", "2026-03-01T00:00:00+00:00"],
        )

    def test_the_31st_skips_short_months(self):
        self.assertEqual(
            firings("0 0 31 * *", "2026-01-31T12:00", 2),
            ["2026-03-31T00:00:00+00:00", "2026-05-31T00:00:00+00:00"],
        )

    def test_leap_day_waits_for_a_leap_year(self):
        self.assertEqual(
            firings("0 0 29 2 *", "2026-01-01T00:00", 2),
            ["2028-02-29T00:00:00+00:00", "2032-02-29T00:00:00+00:00"],
        )

    def test_leap_day_across_a_century_that_is_not_leap(self):
        self.assertEqual(
            firings("0 0 29 2 *", "2096-03-01T00:00", 1),
            ["2104-02-29T00:00:00+00:00"],
        )

    def test_a_local_timezone_shifts_the_instant(self):
        self.assertEqual(
            firings("0 9 * * *", "2026-07-01T00:00", 1, ROME),
            ["2026-07-01T09:00:00+02:00"],
        )


class DaylightSavingTests(unittest.TestCase):
    def test_a_fixed_time_inside_the_spring_gap_fires_when_the_gap_ends(self):
        self.assertEqual(
            firings("30 2 * * *", "2026-03-28T12:00", 2, ROME),
            ["2026-03-29T03:00:00+02:00", "2026-03-30T02:30:00+02:00"],
        )

    def test_a_fixed_time_in_the_repeated_hour_fires_once(self):
        self.assertEqual(
            firings("30 2 * * *", "2026-10-24T12:00", 2, ROME),
            ["2026-10-25T02:30:00+02:00", "2026-10-26T02:30:00+01:00"],
        )

    def test_a_clock_following_schedule_skips_the_missing_hour(self):
        self.assertEqual(
            firings("*/30 * * * *", "2026-03-29T01:00", 3, ROME),
            [
                "2026-03-29T01:30:00+01:00",
                "2026-03-29T03:00:00+02:00",
                "2026-03-29T03:30:00+02:00",
            ],
        )

    def test_a_clock_following_schedule_runs_through_the_repeated_hour(self):
        self.assertEqual(
            firings("0 * * * *", "2026-10-25T00:30", 4, ROME),
            [
                "2026-10-25T01:00:00+02:00",
                "2026-10-25T02:00:00+02:00",
                "2026-10-25T02:00:00+01:00",
                "2026-10-25T03:00:00+01:00",
            ],
        )

    def test_real_time_between_firings_stays_even_across_the_fold(self):
        cron = CronExpression.parse("*/10 * * * *")
        zone = load_zone(ROME)
        moment = instant("2026-10-25T01:00", ROME)
        gaps = set()
        for _ in range(30):
            following = cron.next_after(moment, zone)
            gaps.add(following - moment)
            moment = following
        self.assertEqual(gaps, {600.0})


if __name__ == "__main__":
    unittest.main()
