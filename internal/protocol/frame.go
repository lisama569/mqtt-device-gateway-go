package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// MaxFrameSize bounds a single frame (4 MiB). Larger frames are rejected
// as ErrOversizedPacket without reading unbounded data into memory.
const MaxFrameSize = 4 * 1024 * 1024

// lengthPrefixSize is the big-endian uint32 length prefix of every frame.
const lengthPrefixSize = 4

// ReadFrame reads one length-prefixed JSON packet from r.
func ReadFrame(r io.Reader) (*Packet, error) {
	var prefix [lengthPrefixSize]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(prefix[:])
	if n == 0 {
		return nil, NewError(ErrMalformedPacket, "zero-length frame")
	}
	if n > MaxFrameSize {
		// Do not drain the oversized payload: the connection must be dropped,
		// so the caller tears it down after classifying the failure.
		return nil, NewError(ErrOversizedPacket, "frame length %d exceeds limit %d", n, MaxFrameSize)
	}

	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, NewError(ErrMalformedPacket, "truncated frame body: %v", err)
	}

	var pkt Packet
	if err := json.Unmarshal(buf, &pkt); err != nil {
		return nil, NewError(ErrMalformedPacket, "invalid JSON frame: %v", err)
	}
	if pkt.Type == "" {
		return nil, NewError(ErrMalformedPacket, "missing packet type")
	}
	return &pkt, nil
}

// WriteFrame encodes p and writes one length-prefixed frame to w.
func WriteFrame(w io.Writer, p *Packet) error {
	body, err := json.Marshal(p)
	if err != nil {
		return NewError(ErrMalformedPacket, "encode packet: %v", err)
	}
	if len(body) > MaxFrameSize {
		return NewError(ErrOversizedPacket, "encoded frame length %d exceeds limit %d", len(body), MaxFrameSize)
	}
	var prefix [lengthPrefixSize]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(body)))
	if _, err := w.Write(prefix[:]); err != nil {
		return err
	}
	if _, err := w.Write(body); err != nil {
		return err
	}
	return nil
}

// String renders a short, log-friendly packet description including the
// device-independent identifiers used in every decision log line.
func (p *Packet) String() string {
	if p == nil {
		return "<nil packet>"
	}
	switch p.Type {
	case TypePublish:
		return fmt.Sprintf("PUBLISH(topic=%s qos=%d id=%d dup=%t retain=%t bytes=%d)",
			p.Topic, p.QoS, p.PacketID, p.Dup, p.Retain, len(p.Payload))
	case TypePuback:
		return fmt.Sprintf("PUBACK(id=%d)", p.PacketID)
	case TypeSubscribe:
		return fmt.Sprintf("SUBSCRIBE(id=%d filters=%d)", p.PacketID, len(p.Filters))
	case TypeSuback:
		return fmt.Sprintf("SUBACK(id=%d results=%d)", p.PacketID, len(p.Results))
	case TypeUnsubscribe:
		return fmt.Sprintf("UNSUBSCRIBE(id=%d filters=%d)", p.PacketID, len(p.Filters))
	case TypeUnsuback:
		return fmt.Sprintf("UNSUBACK(id=%d)", p.PacketID)
	case TypeConnect:
		return fmt.Sprintf("CONNECT(client=%s version=%d clean=%t keepalive=%ds will=%t)",
			p.ClientID, p.Version, p.CleanStart, p.KeepAlive, p.Will != nil)
	case TypeConnack:
		return fmt.Sprintf("CONNACK(reason=%d)", p.Reason)
	default:
		return string(p.Type)
	}
}
