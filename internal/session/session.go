// Package session owns per-device session state: subscriptions, the offline
// QoS1 queue, the in-flight window and packet-id allocation.
//
// All methods are safe for concurrent use. The broker never holds two live
// connections for one clientID, but the session may be touched by the routing
// path while the connection goroutine is processing an ack.
package session

import "sync"

// QueuedMessage is an offline or in-flight QoS1 message.
type QueuedMessage struct {
	PacketID uint16
	Topic    string
	Payload  []byte
	QoS      byte
	Dup      bool
}

// Store maps clientID -> persistent session.
type Store struct {
	mu       sync.Mutex
	sessions map[string]*Session
}

// NewStore creates an empty session store.
func NewStore() *Store { return &Store{sessions: map[string]*Session{}} }

// GetOrCreate returns the session for clientID and whether it was created
// fresh. Clean sessions are always new and are NOT stored by this method:
// the caller owns them and passes the same *Session back on disconnect.
func (s *Store) GetOrCreate(clientID string, clean bool) (*Session, bool) {
	if clean {
		return NewSession(clientID, true), true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[clientID]
	if !ok {
		sess = NewSession(clientID, false)
		s.sessions[clientID] = sess
	}
	return sess, !ok
}

// Delete removes a stored persistent session.
func (s *Store) Delete(clientID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, clientID)
}

// Len returns the number of stored persistent sessions (tests).
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}

// PersistentClientIDs snapshots the stored client identifiers for routing.
func (s *Store) PersistentClientIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.sessions))
	for id := range s.sessions {
		out = append(out, id)
	}
	return out
}

// Session is one device's durable (or transient) state.
type Session struct {
	mu sync.Mutex

	clientID string
	clean    bool
	subs     map[string]byte // filter -> granted qos
	queue    []QueuedMessage // not-yet-sent QoS1 messages (FIFO, ordered)
	inflight map[uint16]QueuedMessage
	usedID   map[uint16]bool // window bitmap: inflight + queued
	nextID   uint16
}

// NewSession constructs a session.
func NewSession(clientID string, clean bool) *Session {
	return &Session{
		clientID: clientID,
		clean:    clean,
		subs:     map[string]byte{},
		inflight: map[uint16]QueuedMessage{},
		usedID:   map[uint16]bool{},
		nextID:   0,
	}
}

// ClientID reports the device identifier.
func (s *Session) ClientID() string { return s.clientID }

// Clean reports whether the session is transient.
func (s *Session) Clean() bool { return s.clean }

// Subscriptions returns a snapshot copy of the filters with granted QoS.
func (s *Session) Subscriptions() map[string]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]byte, len(s.subs))
	for f, q := range s.subs {
		out[f] = q
	}
	return out
}

// AddSubscription adds or replaces a subscription filter.
func (s *Session) AddSubscription(filter string, qos byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs[filter] = qos
}

// RemoveSubscription removes a filter; reports whether it existed.
func (s *Session) RemoveSubscription(filter string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subs[filter]; !ok {
		return false
	}
	delete(s.subs, filter)
	return true
}

// Enqueue appends a QoS1 message to the FIFO offline queue and allocates its
// packet id. Returns false when the 65535-entry packet-id window is full.
// Order of enqueue is the exact replay order after reconnect (no duplicates,
// no loss until the storage limit given by the caller is reached).
func (s *Session) Enqueue(topic string, payload []byte, qos byte) (uint16, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.allocateIDLocked()
	if !ok {
		return 0, false
	}
	s.queue = append(s.queue, QueuedMessage{
		PacketID: id, Topic: topic, Payload: append([]byte(nil), payload...), QoS: qos,
	})
	return id, true
}

// QueuedLen is the number of waiting messages (tests).
func (s *Session) QueuedLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

// InflightLen is the number of sent-but-unacked messages (tests).
func (s *Session) InflightLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.inflight)
}

// Pending returns a snapshot of queued (not yet sent) messages in FIFO order.
func (s *Session) Pending() []QueuedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]QueuedMessage, len(s.queue))
	copy(out, s.queue)
	return out
}

// Inflight returns a snapshot of sent-but-unacked messages.
func (s *Session) Inflight() []QueuedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]QueuedMessage, 0, len(s.inflight))
	for _, m := range s.inflight {
		out = append(out, m)
	}
	return out
}

// PopForSend moves the head queued message to in-flight state and returns it.
// On a fresh connection Dup is false; for resend after reconnect call
// ResendInflight. Returns false when the queue is empty.
func (s *Session) PopForSend() (QueuedMessage, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 {
		return QueuedMessage{}, false
	}
	m := s.queue[0]
	s.queue = s.queue[1:]
	s.inflight[m.PacketID] = m
	return m, true
}

// ResendInflight returns all in-flight messages with Dup=true in ascending
// packet-id order (deterministic replay of the unacked window), marking them.
func (s *Session) ResendInflight() []QueuedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]uint16, 0, len(s.inflight))
	for id := range s.inflight {
		ids = append(ids, id)
	}
	// simple insertion-independent order: sort via helper
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
	out := make([]QueuedMessage, 0, len(ids))
	for _, id := range ids {
		m := s.inflight[id]
		m.Dup = true
		s.inflight[id] = m
		out = append(out, m)
	}
	return out
}

// AckPending acknowledges a packet id: removes it from in-flight and frees
// the id. A duplicate ack returns false and changes nothing (idempotent).
func (s *Session) AckPending(id uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.inflight[id]; !ok {
		return false
	}
	delete(s.inflight, id)
	delete(s.usedID, id)
	return true
}

// AllocateID reserves a free packet id out of 1..65535. Used for client-driven
// flows; returns false when the window is exhausted.
func (s *Session) AllocateID() (uint16, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.allocateIDLocked()
}

// ReleaseID frees a packet id without touching in-flight state.
func (s *Session) ReleaseID(id uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.usedID, id)
}

func (s *Session) allocateIDLocked() (uint16, bool) {
	if len(s.usedID) == 65535 {
		return 0, false
	}
	for {
		s.nextID++
		if s.nextID == 0 {
			s.nextID = 1
		}
		if !s.usedID[s.nextID] {
			s.usedID[s.nextID] = true
			return s.nextID, true
		}
	}
}
