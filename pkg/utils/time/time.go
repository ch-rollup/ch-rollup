package time

import "time"

func SecondsFromDuration(dur time.Duration) int {
	return int(dur / time.Second)
}

func ByDurationBoundaries(t time.Time, d time.Duration) (start time.Time, end time.Time) {
	start = t.Truncate(d)

	return start, start.Add(d)
}

type Range struct {
	From, To time.Time
}

func SplitTimeRangeByInterval(timeRange Range, interval time.Duration) []Range {
	// TODO: refac

	from := timeRange.From
	to := timeRange.To

	if interval >= to.Sub(from) {
		return []Range{
			{
				From: from,
				To:   to,
			},
		}
	}

	var result []Range

	next := from

	for next.Before(to) {
		curFrom := next
		curNext := next.Add(interval)
		if curNext.After(to) {
			result = append(result, Range{
				From: curFrom,
				To:   to,
			})

			break
		}

		next = curNext

		result = append(result, Range{
			From: curFrom,
			To:   curNext,
		})
	}

	return result
}
