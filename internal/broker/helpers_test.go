package broker_test

import (
	"context"
	"encoding/binary"
	"net"
	"testing"

	"github.com/lisama569/mqtt-device-gateway-go/internal/client"
	"github.com/lisama569/mqtt-device-gateway-go/internal/proto"
)

func clientDialHelper(ctx context.Context, addr, id string) (*client.Client, error) {
	return client.Dial(ctx, addr, client.Options{ClientID: id, CleanSession: true})
}

func halfClose(c net.Conn) error {
	if tc, ok := c.(*net.TCPConn); ok {
		return tc.CloseWrite()
	}
	return nil
}

func dialRaw(addr string) (net.Conn, error) { return net.Dial("tcp", addr) }

func protoWriteConnect(c net.Conn, id string) error {
	return proto.WriteFrame(c, &proto.Packet{Type: proto.CONNECT, Connect: &proto.ConnectPacket{
		ProtocolName: proto.ProtocolName, Version: proto.SupportedVersion,
		ClientID: id, CleanSession: true,
	}})
}

func writeBytes(t *testing.T, c net.Conn, b []byte) {
	t.Helper()
	if _, err := c.Write(b); err != nil {
		t.Fatal(err)
	}
}

// encodePublishRaw builds a raw QoS1 PUBLISH frame with explicit packet id.
func encodePublishRaw(topicName string, payload []byte, id uint16, dup bool) []byte {
	body := []byte{}
	var flags byte = 1 // qos1
	if dup {
		flags |= 0x08
	}
	body = append(body, flags)
	body = binary.LittleEndian.AppendUint16(body, id)
	body = binary.LittleEndian.AppendUint16(body, uint16(len(topicName)))
	body = append(body, topicName...)
	body = binary.LittleEndian.AppendUint16(body, uint16(len(payload)))
	body = append(body, payload...)

	out := make([]byte, 5)
	binary.LittleEndian.PutUint32(out, uint32(1+len(body)))
	out[4] = byte(proto.PUBLISH)
	return append(out, body...)
}
