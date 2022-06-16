package date

import (
	"strconv"
	"strings"
	"time"

	"github.com/tj/go-naturaldate"
)

func Parse(s string, now time.Time) (time.Time, error) {
	if strings.HasSuffix(s, "d") {
		s = strings.TrimSuffix(s, "d")
		days, err := strconv.Atoi(s)
		if err != nil {
			return time.Time{}, err
		}

		if days > 0 {
			days = -days
		}
		return now.Add(time.Duration(days) * 24 * time.Hour), nil
	}

	duration, err := time.ParseDuration(s)
	if err == nil {
		if duration > 0 {
			duration *= -1
		}
		return now.Add(duration), nil
	}

	t, err := time.Parse(time.RFC3339Nano, s)
	if err == nil {
		return t, nil
	}

	return naturaldate.Parse(s, now)
}
