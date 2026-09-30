// Package proto defines the wire protocol of the local IoT gateway.
//
// Frame layout (little-endian):
//
//	[4 bytes total length][1 byte packet type][payload ...]
//
// total length includes the type byte but excludes the 4-byte length field.
// Payloads use little-endian scalars and 2-byte-length-prefixed strings/bytes.
package proto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// ProtocolName is the magic exchanged in CONNECT.
const ProtocolName = "LGW1" // Local GateWay protocol v1

// SupportedVersion is the only accepted protocol version.
const SupportedVersion byte = 1

// MaxFramePayload bounds one frame's payload (excluding the 3-byte header);
// oversized frames are a protocol error and drop the connection.
const MaxFramePayload = 1 << 20 // 1 MiB

// PacketType enumerates the frame types.
type PacketType byte

const (
	CONNECT     PacketType = 1
	CONNACK     PacketType = 2
	PUBLISH     PacketType = 3
	PUBACK      PacketType = 4
	SUBSCRIBE   PacketType = 5
	SUBACK      PacketType = 6
	UNSUBSCRIBE PacketType = 7
	UNSUBACK    PacketType = 8
	PINGREQ     PacketType = 9
	PINGRESP    PacketType = 10
	DISCONNECT  PacketType = 11
)

// ReasonCode is the explicit, never-silently-downgraded result.
type ReasonCode byte

const (
	RCAccepted         ReasonCode = 0
	RCUnsupportedProto ReasonCode = 1
	RCBadIdentifier    ReasonCode = 2
	RCPayloadTooLarge  ReasonCode = 3
	RCQoSNotSupported  ReasonCode = 4
	RCBadTopicFilter   ReasonCode = 5
	RCNotConnected     ReasonCode = 6
	RCTakeover         ReasonCode = 7
	RCServerError      ReasonCode = 8
)

// Packet is the decoded representation of one frame.
type Packet struct {
	Type PacketType

	// CONNECT
	Connect *ConnectPacket

	// PUBLISH (inbound and outbound)
	Pub *PublishPacket

	// SUBSCRIBE / UNSUBSCRIBE
	Sub   *SubscribePacket
	Unsub *UnsubscribePacket

	// PUBACK
	PacketID uint16

	// CONNACK / SUBACK / UNSUBACK
	Code ReasonCode
	// SUBACK: per-filter grant; QoSNotSupported (0x80-ish) rejects a filter.
	Granted []byte
}

// ConnectPacket carries the device identifier and session/keepalive/will setup.
type ConnectPacket struct {
	ProtocolName string
	Version      byte
	ClientID     string
	CleanSession bool
	KeepAlive    uint16
	Will         *Will
}

// Will is the testament published exactly once on abnormal disconnect.
type Will struct {
	Topic   string
	QoS     byte
	Payload []byte
}

// PublishPacket is an application message in either direction.
type PublishPacket struct {
	Topic    string
	Payload  []byte
	QoS      byte
	PacketID uint16 // 0 for QoS0
	Dup      bool
}

// SubscribePacket carries one or more filters with requested QoS.
type SubscribePacket struct {
	PacketID uint16
	Filters  []SubFilter
}

// SubFilter is one filter/QoS pair in a SUBSCRIBE.
type SubFilter struct {
	Filter string
	QoS    byte
}

// UnsubscribePacket removes filters.
type UnsubscribePacket struct {
	PacketID uint16
	Filters  []string
}

// Frame-level decode errors.
var (
	ErrShortFrame       = errors.New("proto: incomplete frame")
	ErrOversized        = errors.New("proto: frame exceeds maximum size")
	ErrUnknownPacket    = errors.New("proto: unknown packet type")
	ErrMalformedPayload = errors.New("proto: malformed packet payload")
)

const lengthHeaderSize = 4
const frameHeaderSize = 5

// ReadFrame reads exactly one length-prefixed frame from r.
func ReadFrame(r io.Reader) (*Packet, error) {
	var head [lengthHeaderSize]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, err // clean EOF or dropped connection
	}
	total := int(binary.LittleEndian.Uint32(head[:]))
	if total < 1 {
		return nil, ErrMalformedPayload
	}
	if total-1 > MaxFramePayload {
		// Drain is pointless: drop connection after consuming nothing more.
		return nil, fmt.Errorf("%w: %d bytes", ErrOversized, total-1)
	}
	buf := make([]byte, total)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, ErrShortFrame
	}
	p, err := decode(buf)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// WriteFrame encodes and writes one frame to w.
func WriteFrame(w io.Writer, p *Packet) error {
	payload, err := encodePayload(p)
	if err != nil {
		return err
	}
	if len(payload) > MaxFramePayload {
		return fmt.Errorf("%w: %d bytes", ErrOversized, len(payload))
	}
	out := make([]byte, frameHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(out, uint32(1+len(payload)))
	out[4] = byte(p.Type)
	copy(out[5:], payload)
	if err := writeAll(w, out); err != nil {
		return err
	}
	return nil
}

func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

type reader struct {
	b []byte
	i int
}

func (r *reader) byte_() (byte, error) {
	if r.i >= len(r.b) {
		return 0, ErrMalformedPayload
	}
	v := r.b[r.i]
	r.i++
	return v, nil
}

func (r *reader) u16() (uint16, error) {
	if r.i+2 > len(r.b) {
		return 0, ErrMalformedPayload
	}
	v := binary.LittleEndian.Uint16(r.b[r.i:])
	r.i += 2
	return v, nil
}

func (r *reader) str() (string, error) {
	v, err := r.bytes()
	return string(v), err
}

func (r *reader) bytes() ([]byte, error) {
	n, err := r.u16()
	if err != nil {
		return nil, err
	}
	if r.i+int(n) > len(r.b) {
		return nil, ErrMalformedPayload
	}
	v := r.b[r.i : r.i+int(n)]
	r.i += int(n)
	return v, nil
}

func (r *reader) done() bool { return r.i == len(r.b) }

type writer struct{ b []byte }

func (w *writer) byte_(v byte)   { w.b = append(w.b, v) }
func (w *writer) u16(v uint16)   { w.b = binary.LittleEndian.AppendUint16(w.b, v) }
func (w *writer) str(s string)   { w.bytes([]byte(s)) }
func (w *writer) bytes(v []byte) { w.u16(uint16(len(v))); w.b = append(w.b, v...) }

func decode(buf []byte) (*Packet, error) {
	p := &Packet{Type: PacketType(buf[0])}
	r := &reader{b: buf[1:]}
	switch p.Type {
	case CONNECT:
		cp := &ConnectPacket{}
		var err error
		if cp.ProtocolName, err = r.str(); err != nil {
			return nil, err
		}
		if cp.Version, err = r.byte_(); err != nil {
			return nil, err
		}
		flags, err := r.byte_()
		if err != nil {
			return nil, err
		}
		cp.CleanSession = flags&0x01 != 0
		hasWill := flags&0x02 != 0
		if cp.KeepAlive, err = r.u16(); err != nil {
			return nil, err
		}
		if cp.ClientID, err = r.str(); err != nil {
			return nil, err
		}
		if hasWill {
			w := &Will{}
			if w.Topic, err = r.str(); err != nil {
				return nil, err
			}
			if w.QoS, err = r.byte_(); err != nil {
				return nil, err
			}
			if w.Payload, err = r.bytes(); err != nil {
				return nil, err
			}
			cp.Will = w
		}
		if !r.done() {
			return nil, ErrMalformedPayload
		}
		p.Connect = cp
	case CONNACK:
		code, err := r.byte_()
		if err != nil || !r.done() {
			return nil, ErrMalformedPayload
		}
		p.Code = ReasonCode(code)
	case PUBLISH:
		pub := &PublishPacket{}
		var err error
		flags, err := r.byte_()
		if err != nil {
			return nil, err
		}
		pub.QoS = flags & 0x03
		pub.Dup = flags&0x08 != 0
		if pub.PacketID, err = r.u16(); err != nil {
			return nil, err
		}
		if pub.Topic, err = r.str(); err != nil {
			return nil, err
		}
		if pub.Payload, err = r.bytes(); err != nil {
			return nil, err
		}
		if (pub.QoS == 0 && pub.PacketID != 0) || (pub.QoS == 1 && pub.PacketID == 0) {
			return nil, ErrMalformedPayload
		}
		if !r.done() {
			return nil, ErrMalformedPayload
		}
		p.Pub = pub
	case PUBACK:
		id, err := r.u16()
		if err != nil || id == 0 || !r.done() {
			return nil, ErrMalformedPayload
		}
		p.PacketID = id
	case SUBSCRIBE:
		sp := &SubscribePacket{}
		var err error
		if sp.PacketID, err = r.u16(); err != nil || sp.PacketID == 0 {
			return nil, ErrMalformedPayload
		}
		count, err := r.byte_()
		if err != nil {
			return nil, err
		}
		for i := 0; i < int(count); i++ {
			f, err := r.str()
			if err != nil {
				return nil, err
			}
			q, err := r.byte_()
			if err != nil {
				return nil, err
			}
			sp.Filters = append(sp.Filters, SubFilter{Filter: f, QoS: q})
		}
		if !r.done() {
			return nil, ErrMalformedPayload
		}
		p.Sub = sp
		p.PacketID = sp.PacketID
	case SUBACK:
		var err error
		if p.PacketID, err = r.u16(); err != nil || p.PacketID == 0 {
			return nil, ErrMalformedPayload
		}
		count, err := r.byte_()
		if err != nil {
			return nil, err
		}
		for i := 0; i < int(count); i++ {
			g, err := r.byte_()
			if err != nil {
				return nil, err
			}
			p.Granted = append(p.Granted, g)
		}
		if !r.done() {
			return nil, ErrMalformedPayload
		}
	case UNSUBSCRIBE:
		up := &UnsubscribePacket{}
		var err error
		if up.PacketID, err = r.u16(); err != nil || up.PacketID == 0 {
			return nil, ErrMalformedPayload
		}
		count, err := r.byte_()
		if err != nil {
			return nil, err
		}
		for i := 0; i < int(count); i++ {
			f, err := r.str()
			if err != nil {
				return nil, err
			}
			up.Filters = append(up.Filters, f)
		}
		if !r.done() {
			return nil, ErrMalformedPayload
		}
		p.Unsub = up
		p.PacketID = up.PacketID
	case UNSUBACK:
		var err error
		if p.PacketID, err = r.u16(); err != nil || p.PacketID == 0 {
			return nil, ErrMalformedPayload
		}
		if !r.done() {
			return nil, ErrMalformedPayload
		}
	case PINGREQ, PINGRESP, DISCONNECT:
		if !r.done() {
			return nil, ErrMalformedPayload
		}
	default:
		return nil, fmt.Errorf("%w: type=%d", ErrUnknownPacket, p.Type)
	}
	return p, nil
}

func encodePayload(p *Packet) ([]byte, error) {
	w := &writer{}
	switch p.Type {
	case CONNECT:
		c := p.Connect
		if c == nil {
			return nil, ErrMalformedPayload
		}
		w.str(c.ProtocolName)
		w.byte_(c.Version)
		var flags byte
		if c.CleanSession {
			flags |= 0x01
		}
		if c.Will != nil {
			flags |= 0x02
		}
		w.byte_(flags)
		w.u16(c.KeepAlive)
		w.str(c.ClientID)
		if c.Will != nil {
			w.str(c.Will.Topic)
			w.byte_(c.Will.QoS)
			w.bytes(c.Will.Payload)
		}
	case CONNACK:
		w.byte_(byte(p.Code))
	case PUBLISH:
		pub := p.Pub
		if pub == nil {
			return nil, ErrMalformedPayload
		}
		var flags byte
		flags |= pub.QoS & 0x03
		if pub.Dup {
			flags |= 0x08
		}
		w.byte_(flags)
		w.u16(pub.PacketID)
		w.str(pub.Topic)
		w.bytes(pub.Payload)
	case PUBACK:
		if p.PacketID == 0 {
			return nil, ErrMalformedPayload
		}
		w.u16(p.PacketID)
	case SUBSCRIBE:
		s := p.Sub
		if s == nil || s.PacketID == 0 || len(s.Filters) > 255 {
			return nil, ErrMalformedPayload
		}
		w.u16(s.PacketID)
		w.byte_(byte(len(s.Filters)))
		for _, f := range s.Filters {
			w.str(f.Filter)
			w.byte_(f.QoS)
		}
	case SUBACK:
		if p.PacketID == 0 || len(p.Granted) > 255 {
			return nil, ErrMalformedPayload
		}
		w.u16(p.PacketID)
		w.byte_(byte(len(p.Granted)))
		for _, g := range p.Granted {
			w.byte_(g)
		}
	case UNSUBSCRIBE:
		u := p.Unsub
		if u == nil || u.PacketID == 0 || len(u.Filters) > 255 {
			return nil, ErrMalformedPayload
		}
		w.u16(u.PacketID)
		w.byte_(byte(len(u.Filters)))
		for _, f := range u.Filters {
			w.str(f)
		}
	case UNSUBACK:
		if p.PacketID == 0 {
			return nil, ErrMalformedPayload
		}
		w.u16(p.PacketID)
	case PINGREQ, PINGRESP, DISCONNECT:
		// empty payload
	default:
		return nil, fmt.Errorf("%w: type=%d", ErrUnknownPacket, p.Type)
	}
	return w.b, nil
}

// String renders a reason code for logs.
func (c ReasonCode) String() string {
	switch c {
	case RCAccepted:
		return "accepted"
	case RCUnsupportedProto:
		return "unsupported-protocol"
	case RCBadIdentifier:
		return "bad-identifier"
	case RCPayloadTooLarge:
		return "payload-too-large"
	case RCQoSNotSupported:
		return "qos-not-supported"
	case RCBadTopicFilter:
		return "bad-topic-filter"
	case RCNotConnected:
		return "not-connected"
	case RCTakeover:
		return "session-takeover"
	case RCServerError:
		return "server-error"
	default:
		return fmt.Sprintf("unknown(%d)", byte(c))
	}
}
