package task

import "time"

// Period 描述系统任务的刷新周期，例如每天 5 点、每周一 0 点。
// 系统也可以自己实现，例如赛季、活动轮次。
type Period interface {
	// Bounds 返回 now 所在周期的起止秒时间戳 [start, end)。必须满足 start <= now < end。
	Bounds(now int64) (start, end int64)
}

// Cycle 是一个刷新周期的起止秒时间戳 [Start, End)，传给 Hooks.Generate。
type Cycle struct {
	Start int64 // 周期开始时间。
	End   int64 // 下一个周期开始时间。
}

// Daily 返回每天在 loc 时区的 offset 时刻刷新的周期，例如 Daily(loc, 5*time.Hour) 表示每天 5 点。
// offset 必须在 [0, 24h) 内；loc 为 nil 时使用 time.Local。
func Daily(loc *time.Location, offset time.Duration) Period {
	return calendarPeriod{loc: location(loc), offset: offset, days: 1}
}

// Weekly 返回每周 weekday 在 loc 时区的 offset 时刻刷新的周期，例如 Weekly(loc, time.Monday, 0) 表示每周一 0 点。
// offset 必须在 [0, 24h) 内；loc 为 nil 时使用 time.Local。
func Weekly(loc *time.Location, weekday time.Weekday, offset time.Duration) Period {
	return calendarPeriod{loc: location(loc), offset: offset, days: 7, weekday: weekday}
}

// calendarPeriod 按自然日计算周期，跨夏令时也按日历日推进。
type calendarPeriod struct {
	loc     *time.Location // 计算日期使用的时区。
	offset  time.Duration  // 当天零点之后的刷新时刻。
	days    int            // 周期天数：1 为每天，7 为每周。
	weekday time.Weekday   // 每周周期的起始星期。
}

// Bounds 返回 now 所在周期的起止时间。
func (p calendarPeriod) Bounds(now int64) (int64, int64) {
	current := time.Unix(now, 0).In(p.loc)
	year, month, day := current.Date()
	if p.days == 7 {
		day -= (int(current.Weekday()) - int(p.weekday) + 7) % 7
	}
	start := p.at(year, month, day)
	if now < start {
		day -= p.days
		start = p.at(year, month, day)
	}
	return start, p.at(year, month, day+p.days)
}

// at 返回指定日期刷新时刻的秒时间戳；time.Date 会自动规范化越界的日期。
func (p calendarPeriod) at(year int, month time.Month, day int) int64 {
	return time.Date(year, month, day, 0, 0, 0, 0, p.loc).Add(p.offset).Unix()
}

// location 返回计算使用的时区，nil 时使用本地时区。
func location(loc *time.Location) *time.Location {
	if loc == nil {
		return time.Local
	}
	return loc
}
