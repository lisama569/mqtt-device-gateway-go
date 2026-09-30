// Package topic validates topics and performs wildcard matching.
//
// Topics and filters are '/'-separated levels. Empty levels (leading or
// trailing '/') are legal. In filters, '+' matches exactly one level and '#'
// matches zero or more remaining levels; both wildcards are only legal as a
// whole level and '#' may only be the final level.
package topic

import "errors"

// ErrInvalidTopic is returned for empty/malformed topic names or filters.
var ErrInvalidTopic = errors.New("topic: invalid topic or filter")

// ValidatePublishTopic checks a publish target name. Wildcards are illegal:
// a published message must address a concrete topic.
func ValidatePublishTopic(topic string) error {
	if topic == "" {
		return ErrInvalidTopic
	}
	for _, r := range topic {
		if r == '+' || r == '#' {
			return ErrInvalidTopic
		}
	}
	return nil
}

// ValidateFilter checks a subscription filter.
//
// Rules: non-empty; '+' and '#' must occupy a whole level (no "a+" or "a#b");
// '#' may only appear as the last level.
func ValidateFilter(filter string) error {
	if filter == "" {
		return ErrInvalidTopic
	}
	levels := splitLevels(filter)
	for i, lvl := range levels {
		isLast := i == len(levels)-1
		switch {
		case lvl == "#":
			if !isLast {
				return ErrInvalidTopic // multi-level wildcard not at tail
			}
		case lvl == "+":
			// single-level wildcard: always valid
		default:
			for _, r := range lvl {
				if r == '+' || r == '#' {
					return ErrInvalidTopic // wildcard embedded inside level
				}
			}
		}
	}
	return nil
}

// Match reports whether the concrete topic name matches the filter.
// Empty topic never matches; callers validate both beforehand.
func Match(filter, topic string) bool {
	if topic == "" {
		return false
	}
	f := splitLevels(filter)
	t := splitLevels(topic)
	return matchLevels(f, t)
}

func matchLevels(filter, topic []string) bool {
	for i, fl := range filter {
		if fl == "#" {
			return true // remaining topic levels (zero or more) all match
		}
		if i >= len(topic) {
			return false // filter has more concrete levels than the topic
		}
		if fl != "+" && fl != topic[i] {
			return false
		}
	}
	return len(filter) == len(topic)
}

func splitLevels(s string) []string {
	levels := []string{}
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			levels = append(levels, s[start:i])
			start = i + 1
		}
	}
	levels = append(levels, s[start:])
	return levels
}
