package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestTopicMatch(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"a/b/c", "a/b/c", true},
		{"a/b/c", "a/b/d", false},
		{"a/+/c", "a/b/c", true},
		{"a/+/c", "a/x/c", true},
		{"a/+/c", "a/b/x", false},
		{"a/+/c", "a/c", false},
		{"a/#", "a", true}, // '#' matches zero trailing levels
		{"a/#", "a/b", true},
		{"a/#", "a/b/c", true},
		{"#", "anything/at/all", true},
		{"#", "x", true},
		{"+/+", "a/b", true},
		{"+/+", "a/b/c", false},
		{"sport/tennis/player1", "sport/tennis/player1", true},
		{"sport/tennis/player1/#", "sport/tennis/player1/rank/wimbledon", true},
		{"sport/tennis/player1/#", "sport/tennis/player1", true},
		{"sport/tennis/player1/#", "sport/tennis/player2", false},
		{"a/b", "", false},
		{"", "a/b", false},
	}
	for _, tc := range cases {
		if got := TopicMatch(tc.filter, tc.topic); got != tc.want {
			t.Errorf("TopicMatch(%q,%q)=%v want %v", tc.filter, tc.topic, got, tc.want)
		}
	}
}

func TestInvalidFiltersAndTopics(t *testing.T) {
	badFilters := []string{
		"",
		"a/#/b",  // '#' not last
		"a/#b",   // '#' inside level
		"a/+b",   // '+' inside level
		"a/\x00", // NUL
	}
	for _, f := range badFilters {
		if err := ValidateFilter(f); err == nil {
			t.Errorf("ValidateFilter(%q) = nil, want rejection", f)
		}
	}
	goodFilters := []string{"#", "+/b", "a/+/c/#", "a/b"}
	for _, f := range goodFilters {
		if err := ValidateFilter(f); err != nil {
			t.Errorf("ValidateFilter(%q) = %v, want acceptance", f, err)
		}
	}
	badTopics := []string{"", "a/+/c", "a/#", "\x00/x"}
	for _, top := range badTopics {
		if err := ValidateTopicName(top); err == nil {
			t.Errorf("ValidateTopicName(%q) = nil, want rejection", top)
		}
	}
}

func TestFrameRoundTrip(t *testing.T) {
	pkt := &Packet{Type: TypePublish, Topic: "a/b", QoS: 1, PacketID: 7, Payload: []byte("hi")}
	var buf bytes.Buffer
	if err := WriteFrame(&buf, pkt); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if got.Topic != "a/b" || got.PacketID != 7 || string(got.Payload) != "hi" {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

func TestReadFrameMalformed(t *testing.T) {
	// zero-length frame
	var buf bytes.Buffer
	buf.Write([]byte{0, 0, 0, 0})
	if _, err := ReadFrame(&buf); err == nil {
		t.Fatal("zero-length frame accepted")
	}

	// invalid JSON body
	buf.Reset()
	buf.Write([]byte{0, 0, 0, 3})
	buf.WriteString("{x}")
	_, err := ReadFrame(&buf)
	if err == nil || !strings.Contains(err.Error(), string(ErrMalformedPacket)) {
		t.Fatalf("want MALFORMED_PACKET, got %v", err)
	}
}

func TestReadFrameOversized(t *testing.T) {
	var buf bytes.Buffer
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], MaxFrameSize+1)
	buf.Write(prefix[:])
	_, err := ReadFrame(&buf)
	var pe *Error
	if !errors.As(err, &pe) || pe.Code != ErrOversizedPacket {
		t.Fatalf("want ErrOversizedPacket, got %v", err)
	}
}
