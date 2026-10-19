//! The US equity session the stock price oracle quotes in.
//!
//! The stock oracle signs prices only while the US market trades, extended
//! hours included. Outside that window every item it answers is
//! `no_live_quote: No live quote for wtX.` (prod API log 14-19 Oct 2026: the
//! last of the morning at 07:59Z = 03:59 EDT, the first of the evening at
//! 23:45-23:46Z = 19:45-19:46 EDT, all day Saturday and Sunday). So on a NYSE
//! trading day the oracle window is 04:00 ET until 15 minutes before the end of
//! after-hours trading (20:00 ET, 17:00 ET on early-close days).
//!
//! This calendar is only used together with the oracle's own answer: a quote
//! is reported as "market closed" when the oracle answered `no_live_quote`
//! AND this calendar says the window is shut. Either alone keeps the outage
//! answer (503), so a calendar mistake can never hide a real oracle outage.

use time::macros::{offset, time};
use time::{Date, Duration, Month, OffsetDateTime, PrimitiveDateTime, UtcOffset, Weekday};

/// NYSE full-day closures, New York dates. Source: NYSE "Holidays & Trading
/// Hours" 2026 and 2027 tables (same table as the site's marketHours.ts).
/// Dates after 2027 are treated as ordinary weekdays: extend before 2028.
const NYSE_HOLIDAYS: &[(i32, u8, u8)] = &[
    (2026, 1, 1),
    (2026, 1, 19),
    (2026, 2, 16),
    (2026, 4, 3),
    (2026, 5, 25),
    (2026, 6, 19),
    (2026, 7, 3),
    (2026, 9, 7),
    (2026, 11, 26),
    (2026, 12, 25),
    (2027, 1, 1),
    (2027, 1, 18),
    (2027, 2, 15),
    (2027, 3, 26),
    (2027, 5, 31),
    (2027, 6, 18),
    (2027, 7, 5),
    (2027, 9, 6),
    (2027, 11, 25),
    (2027, 12, 24),
];

/// NYSE early closes (regular session ends 13:00 ET, after-hours 17:00 ET).
const NYSE_EARLY_CLOSES: &[(i32, u8, u8)] = &[(2026, 11, 27), (2026, 12, 24), (2027, 11, 26)];

/// Oracle window start: pre-market, 04:00 ET.
const ORACLE_OPEN_MINUTE: u16 = 4 * 60;
/// Oracle window end on a full day: 19:45 ET (after-hours end 20:00 minus 15 min).
const ORACLE_CLOSE_MINUTE: u16 = 19 * 60 + 45;
/// Oracle window end on an early-close day: 16:45 ET.
const ORACLE_EARLY_CLOSE_MINUTE: u16 = 16 * 60 + 45;

fn listed(table: &[(i32, u8, u8)], date: Date) -> bool {
    table
        .iter()
        .any(|&(y, m, d)| date.year() == y && u8::from(date.month()) == m && date.day() == d)
}

fn is_trading_day(date: Date) -> bool {
    !matches!(date.weekday(), Weekday::Saturday | Weekday::Sunday) && !listed(NYSE_HOLIDAYS, date)
}

const EDT: UtcOffset = offset!(-4);
const EST: UtcOffset = offset!(-5);

/// The `n`th (1-based) Sunday of a month.
fn nth_sunday(year: i32, month: Month, n: u8) -> Option<Date> {
    let first = Date::from_calendar_date(year, month, 1).ok()?;
    let to_sunday = (7 - first.weekday().number_days_from_sunday()) % 7;
    first.checked_add(Duration::days(
        i64::from(to_sunday) + 7 * i64::from(n.checked_sub(1)?),
    ))
}

/// New York's UTC offset at `instant`: EDT (-4) from the second Sunday of
/// March 02:00 EST (07:00Z) to the first Sunday of November 02:00 EDT (06:00Z),
/// EST (-5) otherwise.
fn eastern_offset(instant: OffsetDateTime) -> Option<UtcOffset> {
    let utc = instant.to_offset(UtcOffset::UTC);
    let year = utc.year();
    let dst_start =
        PrimitiveDateTime::new(nth_sunday(year, Month::March, 2)?, time!(07:00)).assume_utc();
    let dst_end =
        PrimitiveDateTime::new(nth_sunday(year, Month::November, 1)?, time!(06:00)).assume_utc();
    Some(if utc >= dst_start && utc < dst_end {
        EDT
    } else {
        EST
    })
}

/// The oracle window of a New York date, in minutes after midnight ET.
fn oracle_window(date: Date) -> Option<(u16, u16)> {
    if !is_trading_day(date) {
        return None;
    }
    let close = if listed(NYSE_EARLY_CLOSES, date) {
        ORACLE_EARLY_CLOSE_MINUTE
    } else {
        ORACLE_CLOSE_MINUTE
    };
    Some((ORACLE_OPEN_MINUTE, close))
}

/// Whether the stock oracle's US session is shut at `now`; None when the
/// calendar can't tell (date arithmetic out of range), which callers treat as
/// "not known to be closed".
pub(crate) fn oracle_session_closed(now: OffsetDateTime) -> Option<bool> {
    let local = now.to_offset(eastern_offset(now)?);
    let minute = u16::from(local.hour()) * 60 + u16::from(local.minute());
    Some(match oracle_window(local.date()) {
        Some((open, close)) => minute < open || minute >= close,
        None => true,
    })
}

/// 04:00 ET on `date` as a UTC instant (04:00 is never inside a DST change,
/// and the offset at 04:00 local equals the offset at 09:00Z that date).
fn pre_market_start(date: Date) -> Option<OffsetDateTime> {
    let offset = eastern_offset(PrimitiveDateTime::new(date, time!(09:00)).assume_utc())?;
    Some(
        PrimitiveDateTime::new(date, time!(04:00))
            .assume_offset(offset)
            .to_offset(UtcOffset::UTC),
    )
}

/// The next time the stock oracle's session opens (04:00 ET on the next
/// trading day) strictly after `now`, in UTC. None if the calendar can't tell
/// or no trading day is found within 10 days (never for the tabled years).
pub(crate) fn next_oracle_open(now: OffsetDateTime) -> Option<OffsetDateTime> {
    let today = now.to_offset(eastern_offset(now)?).date();
    (0..=10)
        .filter_map(|days| today.checked_add(Duration::days(days)))
        .filter(|date| is_trading_day(*date))
        .filter_map(pre_market_start)
        .find(|open| *open > now)
}

/// `instant` as an RFC 3339 UTC timestamp with second precision.
pub(crate) fn rfc3339_utc(instant: OffsetDateTime) -> String {
    let utc = instant.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        utc.year(),
        u8::from(utc.month()),
        utc.day(),
        utc.hour(),
        utc.minute(),
        utc.second()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    fn eastern_offset_hours(at: OffsetDateTime) -> i8 {
        eastern_offset(at).expect("offset in range").whole_hours()
    }

    fn closed(at: OffsetDateTime) -> bool {
        oracle_session_closed(at).expect("calendar in range")
    }

    #[test]
    fn eastern_offset_follows_us_dst() {
        assert_eq!(eastern_offset_hours(datetime!(2026-10-19 12:00 UTC)), -4);
        assert_eq!(eastern_offset_hours(datetime!(2026-11-01 05:59 UTC)), -4);
        assert_eq!(eastern_offset_hours(datetime!(2026-11-01 06:00 UTC)), -5);
        assert_eq!(eastern_offset_hours(datetime!(2027-01-15 12:00 UTC)), -5);
        assert_eq!(eastern_offset_hours(datetime!(2027-03-14 06:59 UTC)), -5);
        assert_eq!(eastern_offset_hours(datetime!(2027-03-14 07:00 UTC)), -4);
    }

    #[test]
    fn closed_window_matches_the_prod_oracle_log() {
        // Fri 16 Oct 2026 (EDT): no_live_quote until 07:59Z, again from 23:46Z.
        assert!(closed(datetime!(2026-10-16 07:59 UTC)));
        assert!(!closed(datetime!(2026-10-16 08:00 UTC)));
        assert!(!closed(datetime!(2026-10-16 13:30 UTC)));
        assert!(!closed(datetime!(2026-10-16 23:44 UTC)));
        assert!(closed(datetime!(2026-10-16 23:45 UTC)));
        // Weekend.
        assert!(closed(datetime!(2026-10-17 15:00 UTC)));
        assert!(closed(datetime!(2026-10-18 20:00 UTC)));
        // Mon 19 Oct before and after 04:00 ET.
        assert!(closed(datetime!(2026-10-19 04:30 UTC)));
        assert!(!closed(datetime!(2026-10-19 08:00 UTC)));
    }

    #[test]
    fn holidays_and_early_closes_shape_the_window() {
        // Thanksgiving 26 Nov 2026 (EST): closed all day.
        assert!(closed(datetime!(2026-11-26 15:00 UTC)));
        // Day after: early close, window ends 16:45 ET = 21:45Z.
        assert!(!closed(datetime!(2026-11-27 21:44 UTC)));
        assert!(closed(datetime!(2026-11-27 21:45 UTC)));
        // Ordinary EST day ends 19:45 ET = 00:45Z next day.
        assert!(!closed(datetime!(2026-12-01 00:44 UTC)));
        assert!(closed(datetime!(2026-12-01 00:45 UTC)));
    }

    #[test]
    fn next_open_skips_nights_weekends_and_holidays() {
        // Weeknight before midnight ET -> next morning 04:00 EDT = 08:00Z.
        assert_eq!(
            next_oracle_open(datetime!(2026-10-15 23:50 UTC)),
            Some(datetime!(2026-10-16 08:00 UTC))
        );
        // Same night after midnight ET.
        assert_eq!(
            next_oracle_open(datetime!(2026-10-16 05:00 UTC)),
            Some(datetime!(2026-10-16 08:00 UTC))
        );
        // Friday evening -> Monday.
        assert_eq!(
            next_oracle_open(datetime!(2026-10-16 23:50 UTC)),
            Some(datetime!(2026-10-19 08:00 UTC))
        );
        // Weekend across the DST end (1 Nov 2026) -> Monday 04:00 EST = 09:00Z.
        assert_eq!(
            next_oracle_open(datetime!(2026-10-31 12:00 UTC)),
            Some(datetime!(2026-11-02 09:00 UTC))
        );
        // Wed night before Thanksgiving -> Friday 27 Nov.
        assert_eq!(
            next_oracle_open(datetime!(2026-11-26 02:00 UTC)),
            Some(datetime!(2026-11-27 09:00 UTC))
        );
        // Christmas Eve evening -> Monday 28 Dec (25 Dec closed, then weekend).
        assert_eq!(
            next_oracle_open(datetime!(2026-12-24 23:00 UTC)),
            Some(datetime!(2026-12-28 09:00 UTC))
        );
    }

    #[test]
    fn rfc3339_formats_utc_seconds() {
        assert_eq!(
            rfc3339_utc(datetime!(2026-10-19 08:00 UTC)),
            "2026-10-19T08:00:00Z"
        );
    }
}
