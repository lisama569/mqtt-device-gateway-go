// Package protocol defines the wire protocol of the local IoT gateway:
// length-prefixed JSON frames carrying MQTT-like control packets.
package protocol

// PacketType identifies a control packet.
type PacketType string

// Supported packet types (an MQTT 3.1.1-style subset).
const (
	TypeConnect     PacketType = "CONNECT"
	TypeConnack     PacketType = "CONNACK"
	TypePublish     PacketType = "PUBLISH"
	TypePuback      PacketType = "PUBACK"
	TypeSubscribe   PacketType = "SUBSCRIBE"
	TypeSuback      PacketType = "SUBACK"
	TypeUnsubscribe PacketType = "UNSUBSCRIBE"
	TypeUnsuback    PacketType = "UNSUBACK"
	TypePingreq     PacketType = "PINGREQ"
	TypePingresp    PacketType = "PINGRESP"
	TypeDisconnect  PacketType = "DISCONNECT"
)

// ProtocolName is the protocol identifier carried in CONNECT.
const ProtocolName = "LIG" // Local IoT Gateway

// SupportedVersion is the only accepted protocol version.
const SupportedVersion uint8 = 1

// Connack / Suback reason codes.
const (
	ReasonAccepted           uint8 = 0
	ReasonUnsupportedVersion uint8 = 1
	ReasonIdentifierRejected uint8 = 2
	ReasonMalformed          uint8 = 3
	ReasonQoSNotSupported    uint8 = 4
	ReasonServerUnavailable  uint8 = 5
	ReasonSubFilterRejected  uint8 = 0x80
)

// SubFilter is a single subscription request: a topic filter plus max QoS.
type SubFilter struct {
	Filter string `json:"filter"`
	QoS    uint8  `json:"qos"`
}

// SubResult is the per-filter result in a SUBACK packet.
// Reason is the granted QoS (0 or 1) or ReasonSubFilterRejected.
type SubResult struct {
	Reason uint8 `json:"reason"`
}

// WillMessage is the Last-Will attached to a CONNECT.
type WillMessage struct {
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
	QoS     uint8  `json:"qos"`
}

// Packet is the on-wire representation of every control packet.
// Only fields relevant to Type are populated.
type Packet struct {
	Type     PacketType `json:"type"`
	Protocol string     `json:"protocol,omitempty"` // CONNECT
	Version  uint8      `json:"version,omitempty"`  // CONNECT

	CleanStart bool   `json:"cleanStart,omitempty"` // CONNECT
	KeepAlive  uint16 `json:"keepAlive,omitempty"`  // CONNECT, seconds
	ClientID   string `json:"clientID,omitempty"`
	Reason     uint8  `json:"reason,omitempty"` // CONNACK/SUBACK per-filter

	Topic    string `json:"topic,omitempty"`
	Payload  []byte `json:"payload,omitempty"`
	QoS      uint8  `json:"qos,omitempty"`
	Retain   bool   `json:"retain,omitempty"`
	Dup      bool   `json:"dup,omitempty"`
	PacketID uint16 `json:"packetID,omitempty"`

	Filters []SubFilter  `json:"filters,omitempty"` // SUBSCRIBE
	Results []SubResult  `json:"results,omitempty"` // SUBACK
	Will    *WillMessage `json:"will,omitempty"`    // CONNECT
}
