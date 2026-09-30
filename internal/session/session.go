package session

import "sync"

// MaxInflight bounds unacknowledged QoS 1 outbound packets per session.
const MaxInflight = 16

// MaxOfflineQueue bounds queued messages for an offline persistent session.
const MaxOfflineQueue = 1000

// InboundDupWindow bounds remembered inbound QoS1 packet identifiers used
// to suppress duplicate deliveries. Identifiers leaving the ring can be
// reused by a well-behaved client; reuse inside the window is a duplicate.
const InboundDupWindow = 64

// QueuedMessage is an outbound QoS 1 message waiting for delivery.
type QueuedMessage struct {
	Topic   string
	Payload []byte
	QoS     uint8
	Retain  bool
}

// Inflight is an outbound QoS 1 packet awaiting PUBACK.
type Inflight struct {
	Msg      *QueuedMessage
	PacketID uint16
	Dup      bool
}

// Session is the per-client state machine. All fields are guarded by mu.
// The same Session instance is never reachable from two live connections:
// the broker swaps connection ownership under the registry lock.
type Session struct {
	mu sync.Mutex

	clientID   string
	persistent bool

	subscriptions map[string]uint8 // filter -> max qos

	inflight map[uint16]*Inflight
	nextID   uint16

	// pending holds QoS 1 messages waiting for an inflight slot;
	// offline holds messages produced while no connection owns the session.
	pending []*QueuedMessage
	offline []*QueuedMessage

	// overflowDropped counts messages removed by the oldest-first
	// offline-queue overflow policy.
	overflowDropped int

	// inbound QoS1 de-duplication: a bounded ring of recently seen ids.
	seenRing  [InboundDupWindow]uint16
	seenPos   int
	seenFull  bool
	seenIndex map[uint16]int
}

// NewSession creates a session. Persistent sessions survive disconnects.
func NewSession(clientID string, persistent bool) *Session {
	return &Session{
		clientID:      clientID,
		persistent:    persistent,
		subscriptions: make(map[string]uint8),
		inflight:      make(map[uint16]*Inflight),
		nextID:        1,
		seenIndex:     make(map[uint16]int),
	}
}

// ClientID returns the device identifier.
func (s *Session) ClientID() string { return s.clientID }

// Persistent reports whether the session survives disconnects.
func (s *Session) Persistent() bool { return s.persistent }

// SetSubscription adds or replaces a subscription.
func (s *Session) SetSubscription(filter string, qos uint8) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions[filter] = qos
}

// RemoveSubscription removes a subscription; reports whether it existed.
func (s *Session) RemoveSubscription(filter string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subscriptions[filter]; !ok {
		return false
	}
	delete(s.subscriptions, filter)
	return true
}

// Subscriptions returns a snapshot copy of the subscription table.
func (s *Session) Subscriptions() map[string]uint8 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]uint8, len(s.subscriptions))
	for f, q := range s.subscriptions {
		out[f] = q
	}
	return out
}

// SeenInbound reports and records an inbound QoS1 packet identifier.
// The first sighting returns false; a repeat inside the dedup window
// returns true (duplicate: acknowledge again, do not re-deliver).
func (s *Session) SeenInbound(packetID uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.seenIndex[packetID]; dup {
		return true
	}
	// Evict the identifier currently occupying the ring slot.
	if s.seenFull {
		delete(s.seenIndex, s.seenRing[s.seenPos])
	}
	s.seenRing[s.seenPos] = packetID
	s.seenIndex[packetID] = s.seenPos
	s.seenPos = (s.seenPos + 1) % InboundDupWindow
	if s.seenPos == 0 {
		s.seenFull = true
	}
	return false
}

// EnqueueOffline appends a message for an offline persistent session.
// When the bounded queue is full the OLDEST message is dropped
// (oldest-first policy) and returned so the caller can log the decision.
func (s *Session) EnqueueOffline(m *QueuedMessage) *QueuedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	var dropped *QueuedMessage
	if len(s.offline) >= MaxOfflineQueue {
		dropped = s.offline[0]
		s.offline = s.offline[1:]
		s.overflowDropped++
	}
	s.offline = append(s.offline, m)
	return dropped
}

// OverflowDropped returns how many messages the overflow policy removed.
func (s *Session) OverflowDropped() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.overflowDropped
}

// DrainOffline moves queued offline messages into the pending queue in
// their original arrival order and returns the number moved.
func (s *Session) DrainOffline() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.offline)
	s.pending = append(s.pending, s.offline...)
	s.offline = s.offline[:0]
	return n
}

// OfflineCount returns the number of queued offline messages.
func (s *Session) OfflineCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.offline)
}

// PushPending appends a message to the pending queue.
func (s *Session) PushPending(m *QueuedMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = append(s.pending, m)
}

// allocIDLocked returns the next free packet identifier, skipping ids
// still held in the inflight window. Wrapping past 65535 restarts at 1
// (0 is never a valid identifier).
func (s *Session) allocIDLocked() uint16 {
	id := s.nextID
	for attempt := 0; attempt < 65535; attempt++ {
		if _, busy := s.inflight[id]; !busy {
			s.nextID = id + 1
			if s.nextID == 0 { // 0 is never a valid identifier
				s.nextID = 1
			}
			return id
		}
		id++
		if id == 0 {
			id = 1
		}
	}
	return 0 // whole 1..65535 range in flight; window cap prevents this
}

// AllocPacketID binds the head pending message to a fresh packet
// identifier when an inflight slot is free. It returns false when the
// window is full, when no identifier is available, or when the pending
// queue is empty.
func (s *Session) AllocPacketID() (*Inflight, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.inflight) >= MaxInflight || len(s.pending) == 0 {
		return nil, false
	}
	id := s.allocIDLocked()
	if id == 0 {
		return nil, false
	}
	m := s.pending[0]
	s.pending = s.pending[1:]
	in := &Inflight{Msg: m, PacketID: id}
	s.inflight[id] = in
	return in, true
}

// PendingCount returns the number of messages waiting for a window slot.
func (s *Session) PendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

// InflightSnapshot returns copies of all inflight entries. On resumption
// callers must resend them with Dup set to true.
func (s *Session) InflightSnapshot() []*Inflight {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Inflight, 0, len(s.inflight))
	for _, in := range s.inflight {
		cp := *in
		cp.Dup = true
		out = append(out, &cp)
	}
	return out
}

// AckPacketID completes an inflight packet (PUBACK). It returns false if
// the identifier is unknown (spurious or duplicate acknowledgement);
// the inflight entry is then left untouched rather than double-removed.
func (s *Session) AckPacketID(packetID uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.inflight[packetID]; !ok {
		return false
	}
	delete(s.inflight, packetID)
	return true
}

// InflightCount returns the number of unacknowledged outbound packets.
func (s *Session) InflightCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.inflight)
}
