package comm

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"
)

var timeOffsetNanos atomic.Int64

// Now 返回系统时间加上区服偏移后的游戏时间。
// 游戏时间可随偏移变化而跳变，返回值只携带墙上时钟。
func Now() time.Time {
	return RealNow().Round(0).Add(time.Duration(timeOffsetNanos.Load()))
}

// NowUnix 返回当前游戏时间，单位为 Unix 秒。
func NowUnix() int64 { return Now().Unix() }

// NowUnixMilli 返回当前游戏时间，单位为 Unix 毫秒。
func NowUnixMilli() int64 { return Now().UnixMilli() }

// NowUnixNano 返回当前游戏时间，单位为 Unix 纳秒。
func NowUnixNano() int64 { return Now().UnixNano() }

// RealNow 返回实际时间并保留单调时钟，供限流和等待时长等计时使用。
func RealNow() time.Time {
	return time.Now()
}

// TimeOffsetSeconds 返回当前游戏时间相对系统时间的有符号秒数偏移。
func TimeOffsetSeconds() int64 {
	return int64(time.Duration(timeOffsetNanos.Load()) / time.Second)
}

// SetTimeOffset 设置相对系统时间的有符号秒数偏移，并返回上次偏移的秒数。
// 无效值会返回错误，游戏时间继续使用上次成功设置的偏移。
func SetTimeOffset(seconds int64) (int64, error) {
	if seconds < math.MinInt64/int64(time.Second) || seconds > math.MaxInt64/int64(time.Second) {
		return 0, fmt.Errorf("偏移 %d 秒超出时长范围", seconds)
	}
	offset := time.Duration(seconds) * time.Second
	at := RealNow().Add(offset)
	if !at.After(time.Unix(0, 0)) || at.After(time.Unix(0, math.MaxInt64)) {
		return 0, fmt.Errorf("偏移 %d 秒后的时间超出正 Unix 纳秒范围", seconds)
	}
	before := time.Duration(timeOffsetNanos.Swap(int64(offset))) / time.Second
	return int64(before), nil
}

// Since 返回从指定时刻到当前游戏时间的时间差，指定时刻在未来时为负数。
func Since(at time.Time) time.Duration { return Now().Sub(at) }

// Until 返回从当前游戏时间到指定时刻的时间差，指定时刻已过去时为负数。
func Until(at time.Time) time.Duration { return at.Sub(Now()) }

// StartOfDay 返回指定时间所在时区的当日开始时刻。
func StartOfDay(at time.Time) time.Time {
	year, month, day := at.Date()
	return startOfDate(year, month, day, at.Location())
}

// NextDayStart 返回指定时间所在时区的次日开始时刻。
func NextDayStart(at time.Time) time.Time {
	year, month, day := at.Date()
	return startOfDate(year, month, day+1, at.Location())
}

// DayBounds 返回指定时间所在时区的当日和次日开始时刻，单位为 Unix 秒。
func DayBounds(at time.Time) (int64, int64) {
	return StartOfDay(at).Unix(), NextDayStart(at).Unix()
}

// SameDay 按 at 所在时区判断两个时刻是否属于同一自然日。
func SameDay(at, other time.Time) bool {
	year, month, day := at.Date()
	otherYear, otherMonth, otherDay := other.In(at.Location()).Date()
	return year == otherYear && month == otherMonth && day == otherDay
}

// startOfDate 返回当地日期的第一个有效时刻。零点重复时取较早的一次，零点被跳过时取跳变时刻。
func startOfDate(year int, month time.Month, day int, loc *time.Location) time.Time {
	wallMidnight := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	nearMidnight := time.Date(year, month, day, 0, 0, 0, 0, loc)
	_, offset := nearMidnight.Zone()
	midnight := wallMidnight.Add(-time.Duration(offset) * time.Second).In(loc)
	zoneStart, zoneEnd := nearMidnight.ZoneBounds()
	if !zoneStart.IsZero() {
		previous := zoneStart.Add(-time.Nanosecond)
		previousStart, _ := previous.ZoneBounds()
		_, previousOffset := previous.Zone()
		previousMidnight := wallMidnight.Add(-time.Duration(previousOffset) * time.Second).In(loc)
		if previousMidnight.Before(zoneStart) && (previousStart.IsZero() || !previousMidnight.Before(previousStart)) {
			return previousMidnight
		}
		if midnight.Before(zoneStart) {
			return zoneStart
		}
	}
	if !zoneEnd.IsZero() && !midnight.Before(zoneEnd) {
		return zoneEnd
	}
	return midnight
}
