package protocol

import "strings"

// Topic rules (MQTT 3.1.1 §4.7, enforced strictly instead of silently
// accepting bad input):
//   - levels are separated by '/'; the empty topic/filter is invalid;
//   - '#' may only appear as the final level and matches the remainder
//     (including the leading separator, so "a/#" matches "a" as well);
//   - '+' matches exactly one level and must occupy a whole level;
//   - '+' / '#' are forbidden in PUBLISH topic names;
//   - NUL bytes are never allowed.

const (
	levelSep   = "/"
	singleWild = "+"
	multiWild  = "#"
)

// ValidateTopicName validates a PUBLISH topic name (filters forbidden).
func ValidateTopicName(topic string) error {
	if topic == "" {
		return NewError(ErrInvalidTopic, "empty topic name")
	}
	if strings.ContainsRune(topic, 0) {
		return NewError(ErrInvalidTopic, "topic contains NUL byte: %q", topic)
	}
	for i, level := range strings.Split(topic, levelSep) {
		if level == singleWild || level == multiWild {
			return NewError(ErrInvalidTopic, "wildcard %q not allowed in topic name at level %d", level, i)
		}
	}
	return nil
}

// ValidateFilter validates a subscription topic filter.
func ValidateFilter(filter string) error {
	if filter == "" {
		return NewError(ErrInvalidSubscription, "empty topic filter")
	}
	if strings.ContainsRune(filter, 0) {
		return NewError(ErrInvalidSubscription, "filter contains NUL byte: %q", filter)
	}
	levels := strings.Split(filter, levelSep)
	for i, level := range levels {
		switch {
		case level == multiWild:
			if i != len(levels)-1 {
				return NewError(ErrInvalidSubscription, "'#' must be the last level: %q", filter)
			}
		case strings.Contains(level, singleWild) && level != singleWild:
			return NewError(ErrInvalidSubscription, "'+' must occupy a whole level: %q", filter)
		case strings.Contains(level, multiWild):
			return NewError(ErrInvalidSubscription, "'#' must occupy a whole level: %q", filter)
		}
	}
	return nil
}

// TopicMatch reports whether a concrete topic name matches a filter,
// supporting MQTT '+' (single level) and '#' (multi level) wildcards.
// The caller is expected to have validated both arguments; an invalid
// filter simply never matches.
func TopicMatch(filter, topic string) bool {
	if filter == "" || topic == "" {
		return false
	}
	f := strings.Split(filter, levelSep)
	t := strings.Split(topic, levelSep)
	for i, fLevel := range f {
		if fLevel == multiWild {
			return true // matches zero or more remaining levels
		}
		if i >= len(t) {
			return false
		}
		if fLevel != singleWild && fLevel != t[i] {
			return false
		}
	}
	return len(f) == len(t)
}
