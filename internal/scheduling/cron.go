package scheduling

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ScheduleSpec encapsulates a parsed 5-field cron expression and IANA timezone (Blueprint §17).
type ScheduleSpec struct {
	RawCron  string
	Timezone string
	Location *time.Location
	Minutes  fieldMatcher
	Hours    fieldMatcher
	Days     fieldMatcher
	Months   fieldMatcher
	Weekdays fieldMatcher
	domStar  bool
	dowStar  bool
}

type fieldMatcher map[int]bool

// ParseSchedule parses a 5-field cron expression with an explicit IANA timezone.
// If timezone is empty, UTC is used as the blueprint default.
func ParseSchedule(cronExpr string, timezone string) (*ScheduleSpec, error) {
	fields := strings.Fields(strings.TrimSpace(cronExpr))
	if len(fields) != 5 {
		return nil, fmt.Errorf("invalid cron expression: expected exactly 5 fields, got %d", len(fields))
	}

	if timezone == "" {
		timezone = "UTC"
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return nil, fmt.Errorf("invalid IANA timezone %q: %w", timezone, err)
	}

	spec := &ScheduleSpec{
		RawCron:  cronExpr,
		Timezone: timezone,
		Location: loc,
		domStar:  fields[2] == "*",
		dowStar:  fields[4] == "*",
	}

	spec.Minutes, err = parseField(fields[0], 0, 59, nil)
	if err != nil {
		return nil, fmt.Errorf("invalid minute field %q: %w", fields[0], err)
	}

	spec.Hours, err = parseField(fields[1], 0, 23, nil)
	if err != nil {
		return nil, fmt.Errorf("invalid hour field %q: %w", fields[1], err)
	}

	spec.Days, err = parseField(fields[2], 1, 31, nil)
	if err != nil {
		return nil, fmt.Errorf("invalid day-of-month field %q: %w", fields[2], err)
	}

	spec.Months, err = parseField(fields[3], 1, 12, monthNames)
	if err != nil {
		return nil, fmt.Errorf("invalid month field %q: %w", fields[3], err)
	}

	spec.Weekdays, err = parseField(fields[4], 0, 7, weekdayNames)
	if err != nil {
		return nil, fmt.Errorf("invalid day-of-week field %q: %w", fields[4], err)
	}
	// Normalize Sunday: both 0 and 7 represent Sunday
	if spec.Weekdays[7] {
		spec.Weekdays[0] = true
		delete(spec.Weekdays, 7)
	}

	return spec, nil
}

var monthNames = map[string]int{
	"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
	"JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

var weekdayNames = map[string]int{
	"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6,
}

func parseField(field string, min, max int, nameMap map[string]int) (fieldMatcher, error) {
	matcher := make(fieldMatcher)
	for _, part := range strings.Split(field, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("empty sub-expression in %q", field)
		}

		step := 1
		rangeExpr := part
		if strings.Contains(part, "/") {
			subParts := strings.Split(part, "/")
			if len(subParts) != 2 {
				return nil, fmt.Errorf("invalid step expression %q", part)
			}
			rangeExpr = subParts[0]
			var err error
			step, err = strconv.Atoi(subParts[1])
			if err != nil || step <= 0 {
				return nil, fmt.Errorf("invalid step value in %q", part)
			}
		}

		var start, end int
		if rangeExpr == "*" {
			start = min
			end = max
		} else if strings.Contains(rangeExpr, "-") {
			subParts := strings.Split(rangeExpr, "-")
			if len(subParts) != 2 {
				return nil, fmt.Errorf("invalid range expression %q", rangeExpr)
			}
			var err error
			start, err = resolveValue(subParts[0], min, max, nameMap)
			if err != nil {
				return nil, err
			}
			end, err = resolveValue(subParts[1], min, max, nameMap)
			if err != nil {
				return nil, err
			}
			if start > end {
				return nil, fmt.Errorf("range start %d greater than end %d", start, end)
			}
		} else {
			val, err := resolveValue(rangeExpr, min, max, nameMap)
			if err != nil {
				return nil, err
			}
			start = val
			if strings.Contains(part, "/") {
				end = max
			} else {
				end = val
			}
		}

		for i := start; i <= end; i += step {
			matcher[i] = true
		}
	}

	if len(matcher) == 0 {
		return nil, fmt.Errorf("no valid values resolved for %q", field)
	}
	return matcher, nil
}

func resolveValue(s string, min, max int, nameMap map[string]int) (int, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	if nameMap != nil {
		if val, ok := nameMap[s]; ok {
			return val, nil
		}
	}
	val, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q", s)
	}
	if val < min || val > max {
		return 0, fmt.Errorf("value %d out of bounds [%d, %d]", val, min, max)
	}
	return val, nil
}

// MatchesLocal returns whether the provided local wall time matches the cron specification.
func (s *ScheduleSpec) MatchesLocal(t time.Time) bool {
	if !s.Minutes[t.Minute()] {
		return false
	}
	if !s.Hours[t.Hour()] {
		return false
	}
	if !s.Months[int(t.Month())] {
		return false
	}

	domMatch := s.Days[t.Day()]
	dowMatch := s.Weekdays[int(t.Weekday())]

	if !s.domStar && !s.dowStar {
		// Standard cron: if both DOM and DOW are specified, match if either matches
		if !domMatch && !dowMatch {
			return false
		}
	} else if !s.domStar {
		if !domMatch {
			return false
		}
	} else if !s.dowStar {
		if !dowMatch {
			return false
		}
	}

	return true
}
