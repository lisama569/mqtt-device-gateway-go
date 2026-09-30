package proto

import (
	"bytes"
	"errors"
	"testing"
)

func roundTrip(t *testing.T, in *Packet) *Packet {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteFrame(&buf, in); err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}

func TestRoundTripConnect(t *testing.T) {
	in := &Packet{Type: CONNECT, Connect: &ConnectPacket{
		ProtocolName: ProtocolName, Version: SupportedVersion,
		ClientID: "dev-1", CleanSession: false, KeepAlive: 30,
		Will: &Will{Topic: "w/d", QoS: 1, Payload: []byte("bye")},
	}}
	out := roundTrip(t, in)
	c := out.Connect
	if c.ClientID != "dev-1" || c.CleanSession != false || c.KeepAlive != 30 ||
		c.Will == nil || c.Will.Topic != "w/d" || string(c.Will.Payload) != "bye" {
		t.Fatalf("connect round trip mismatch: %+v", c)
	}
}

func TestRoundTripPublishAndSubscribe(t *testing.T) {
	pub := roundTrip(t, &Packet{Type: PUBLISH, Pub: &PublishPacket{
		Topic: "a/b", Payload: []byte("hi"), QoS: 1, PacketID: 42, Dup: true,
	}})
	if pub.Pub.PacketID != 42 || !pub.Pub.Dup || pub.Pub.Topic != "a/b" {
		t.Fatal("publish round trip mismatch")
	}
	sub := roundTrip(t, &Packet{Type: SUBSCRIBE, Sub: &SubscribePacket{
		PacketID: 7, Filters: []SubFilter{{"a/+", 0}, {"b/#", 1}},
	}})
	if len(sub.Sub.Filters) != 2 || sub.Sub.Filters[1].Filter != "b/#" {
		t.Fatal("subscribe round trip mismatch")
	}
}

func TestMalformedRejected(t *testing.T) {
	// PUBACK with zero id
	for _, bad := range []*Packet{
		{Type: PUBACK, PacketID: 0},
		{Type: SUBSCRIBE, Sub: &SubscribePacket{PacketID: 0}},
	} {
		var buf bytes.Buffer
		if err := WriteFrame(&buf, bad); err == nil {
			t.Fatalf("expected encode error for %+v", bad)
		}
	}
}

func TestOversized(t *testing.T) {
	big := bytes.Repeat([]byte{'x'}, MaxFramePayload+1)
	var buf bytes.Buffer
	err := WriteFrame(&buf, &Packet{Type: PUBLISH, Pub: &PublishPacket{
		Topic: "t", Payload: big, QoS: 0,
	}})
	if !errors.Is(err, ErrOversized) {
		t.Fatalf("expected ErrOversized, got %v", err)
	}
}

func TestReasonCodeString(t *testing.T) {
	if RCQoSNotSupported.String() != "qos-not-supported" {
		t.Fatal("reason code string mismatch")
	}
}
