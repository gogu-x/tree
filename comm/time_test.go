package comm

import (
	"math"
	"testing"
	"testing/synctest"
	"time"
)

func TestOffsetClock(t *testing.T) {
	oldOffset := timeOffsetNanos.Swap(0)
	t.Cleanup(func() { timeOffsetNanos.Store(oldOffset) })
	synctest.Test(t, func(t *testing.T) {
		real := RealNow()
		previous := int64(0)
		for _, offset := range []int64{3600, -3600, 0} {
			if old, err := SetTimeOffset(offset); err != nil || old != previous {
				t.Fatalf("SetTimeOffset(%d) = %d, %v, want %d, nil", offset, old, err, previous)
			}
			previous = offset
			want := real.Add(time.Duration(offset) * time.Second)
			if got := TimeOffsetSeconds(); got != offset {
				t.Errorf("TimeOffsetSeconds() = %d, want %d", got, offset)
			}
			if got := Now(); !got.Equal(want) {
				t.Errorf("Now() = %v, want %v", got, want)
			}
			if NowUnix() != want.Unix() || NowUnixMilli() != want.UnixMilli() || NowUnixNano() != want.UnixNano() {
				t.Errorf("timestamps do not match the offset clock at %v", want)
			}
			if got := Since(want.Add(-time.Minute)); got != time.Minute {
				t.Errorf("Since() = %v, want %v", got, time.Minute)
			}
			if got := Until(want.Add(-time.Minute)); got != -time.Minute {
				t.Errorf("Until() = %v, want %v", got, -time.Minute)
			}
			if got := RealNow(); !got.Equal(real) {
				t.Errorf("RealNow() = %v, want %v", got, real)
			}
		}
		if _, err := SetTimeOffset(3600); err != nil {
			t.Fatal(err)
		}
		for _, offset := range []int64{math.MaxInt64, math.MinInt64, -9_000_000_000, 9_000_000_000} {
			if _, err := SetTimeOffset(offset); err == nil {
				t.Errorf("SetTimeOffset(%d) accepted an invalid offset", offset)
			}
			if got := TimeOffsetSeconds(); got != 3600 {
				t.Errorf("offset after invalid update = %d, want 3600", got)
			}
		}
	})
}

func TestDayUtilitiesUseInputLocation(t *testing.T) {
	loc := time.FixedZone("UTC+8", 8*3600)
	at := time.Date(2026, 12, 31, 1, 30, 0, 0, loc)
	start := time.Date(2026, 12, 31, 0, 0, 0, 0, loc)
	next := time.Date(2027, 1, 1, 0, 0, 0, 0, loc)
	startUnix, nextUnix := DayBounds(at)
	if startUnix != start.Unix() || nextUnix != next.Unix() {
		t.Fatalf("DayBounds() = %d, %d, want %d, %d", startUnix, nextUnix, start.Unix(), next.Unix())
	}
	if !SameDay(at, at.UTC()) || SameDay(at, next.UTC()) {
		t.Fatal("SameDay did not compare dates in the first time's location")
	}
}

func TestDayStartsAcrossTimeZoneTransitions(t *testing.T) {
	for _, tc := range []struct {
		name  string
		zone  string
		at    string
		start string
		next  string
	}{
		{"short day", "America/New_York", "2026-03-08T12:00:00Z", "2026-03-08T05:00:00Z", "2026-03-09T04:00:00Z"},
		{"skipped midnight", "America/Sao_Paulo", "2018-11-04T12:00:00Z", "2018-11-04T03:00:00Z", "2018-11-05T02:00:00Z"},
		{"repeated midnight", "America/Havana", "2020-11-01T12:00:00Z", "2020-11-01T04:00:00Z", "2020-11-02T05:00:00Z"},
		{"skipped date", "Pacific/Apia", "2011-12-29T22:00:00Z", "2011-12-29T10:00:00Z", "2011-12-30T10:00:00Z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loc, err := time.LoadLocation(tc.zone)
			if err != nil {
				t.Fatal(err)
			}
			instant, err := time.Parse(time.RFC3339, tc.at)
			if err != nil {
				t.Fatal(err)
			}
			at := instant.In(loc)
			start, next := StartOfDay(at), NextDayStart(at)
			if start.Location() != loc || next.Location() != loc {
				t.Fatal("day boundaries lost the input location")
			}
			if got := start.UTC().Format(time.RFC3339); got != tc.start {
				t.Errorf("StartOfDay() = %s, want %s", got, tc.start)
			}
			if got := next.UTC().Format(time.RFC3339); got != tc.next {
				t.Errorf("NextDayStart() = %s, want %s", got, tc.next)
			}
		})
	}
}
