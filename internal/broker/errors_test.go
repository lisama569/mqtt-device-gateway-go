package broker_test

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/lisama569/mqtt-device-gateway-go/internal/proto"
)

func rawDial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func writeRaw(t *testing.T, c net.Conn, ptype byte, payload []byte) {
	t.Helper()
	hdr := make([]byte, 5)
	binary.LittleEndian.PutUint32(hdr, uint32(1+len(payload)))
	hdr[4] = ptype
	if _, err := c.Write(append(hdr, payload...)); err != nil {
		t.Fatal(err)
	}
}

// 无法识别的协议版本必须被明确拒绝（CONNACK RCUnsupportedProto）。
func TestUnsupportedProtocolVersion(t *testing.T) {
	b := testBroker(t)
	c := rawDial(t, b.Addr())
	p := &proto.Packet{Type: proto.CONNECT, Connect: &proto.ConnectPacket{
		ProtocolName: proto.ProtocolName, Version: 99, ClientID: "dev-x",
	}}
	if err := proto.WriteFrame(c, p); err != nil {
		t.Fatal(err)
	}
	ack, err := proto.ReadFrame(c)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Type != proto.CONNACK || ack.Code != proto.RCUnsupportedProto {
		t.Fatalf("got type=%d code=%s, want CONNACK unsupported-proto", ack.Type, ack.Code)
	}
}

// 空设备标识必须被拒绝。
func TestEmptyClientIDRejected(t *testing.T) {
	b := testBroker(t)
	c := rawDial(t, b.Addr())
	_ = proto.WriteFrame(c, &proto.Packet{Type: proto.CONNECT, Connect: &proto.ConnectPacket{
		ProtocolName: proto.ProtocolName, Version: proto.SupportedVersion, ClientID: "",
	}})
	ack, _ := proto.ReadFrame(c)
	if ack.Code != proto.RCBadIdentifier {
		t.Fatalf("code=%s", ack.Code)
	}
}

// 首帧不是 CONNECT：连接被直接关闭。
func TestFirstFrameNotConnect(t *testing.T) {
	b := testBroker(t)
	c := rawDial(t, b.Addr())
	writeRaw(t, c, byte(proto.PINGREQ), nil)
	_, err := proto.ReadFrame(c)
	if err == nil {
		t.Fatal("expected connection close")
	}
}

// 畸形报文（声明长度远大于实际内容）：连接被关闭，分类为协议错误。
func TestMalformedFrameClosesConnection(t *testing.T) {
	b := testBroker(t)
	ctx := context.Background()
	good, err := clientDialHelper(ctx, b.Addr(), "good-dev")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = good.Disconnect() })

	bad := rawDial(t, b.Addr())
	// 合法 CONNECT
	_ = proto.WriteFrame(bad, &proto.Packet{Type: proto.CONNECT, Connect: &proto.ConnectPacket{
		ProtocolName: proto.ProtocolName, Version: proto.SupportedVersion,
		ClientID: "bad-dev", CleanSession: true,
	}})
	ack, _ := proto.ReadFrame(bad)
	if ack.Code != proto.RCAccepted {
		t.Fatalf("connect code=%s", ack.Code)
	}
	// 声明 300 字节但只发 3 字节，随后半关闭写端 → 服务端快速读到 EOF
	hdr := make([]byte, 4)
	binary.LittleEndian.PutUint32(hdr, 300)
	buf := append(hdr, byte(proto.PINGREQ), 0, 0)
	_, _ = bad.Write(buf)
	_ = halfClose(bad)

	// bad 设备应被断开（服务端关闭连接）
	one := make([]byte, 16)
	_ = bad.SetReadDeadline(time.Now().Add(testTimeout))
	if _, err := bad.Read(one); err == nil {
		t.Fatal("malformed client should be disconnected")
	}

	// good 设备必须不受影响：仍能订阅收发
	if _, err := good.Subscribe(ctx, map[string]byte{"iso/+": 1}); err != nil {
		t.Fatalf("good device subscribe after neighbor failure: %v", err)
	}
}

// 超大帧必须被拒绝并断开：声明长度超过网关上限即触发，且无需读完整个报文体。
func TestOversizedFrameRejected(t *testing.T) {
	b := testBroker(t)
	c := rawDial(t, b.Addr())
	// 仅发送长度头，声明的负载超过上限；服务端应立即判定并关闭。
	hdr := make([]byte, 4)
	binary.LittleEndian.PutUint32(hdr, uint32(proto.MaxFramePayload+100))
	if _, err := c.Write(hdr); err != nil {
		t.Fatal(err)
	}
	one := make([]byte, 16)
	_ = c.SetReadDeadline(time.Now().Add(testTimeout))
	if _, err := c.Read(one); err == nil {
		t.Fatal("oversized client should be disconnected")
	}
}

// 非法订阅过滤条件必须逐项拒绝，不能静默接受。
func TestBadSubscriptionRejected(t *testing.T) {
	b := testBroker(t)
	ctx := context.Background()
	c, err := clientDialHelper(ctx, b.Addr(), "sub-bad")
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Subscribe(ctx, map[string]byte{"a/#/c": 1})
	if err != nil {
		t.Fatal(err)
	}
	idx, bad := res.AnyRejected()
	if !bad {
		t.Fatal("malformed filter must be rejected in SUBACK")
	}
	if res.Granted[idx]&0x7f != byte(proto.RCBadTopicFilter) {
		t.Fatalf("granted=%#x want bad-topic-filter", res.Granted[idx])
	}
}

// QoS2 等不支持的等级必须明确拒绝，不能降级。
func TestQoS2Rejected(t *testing.T) {
	b := testBroker(t)
	ctx := context.Background()
	c, err := clientDialHelper(ctx, b.Addr(), "qos2-dev")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(ctx, "t/x", []byte("x"), 2); err == nil {
		t.Fatal("client should reject qos2 locally")
	}
}
