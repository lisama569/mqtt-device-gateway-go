package session

import "sync"

// Store is the registry of known sessions keyed by client identifier.
type Store struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

// NewStore creates an empty session store.
func NewStore() *Store {
	return &Store{sessions: make(map[string]*Session)}
}

// Lookup returns the session for clientID, if any.
func (st *Store) Lookup(clientID string) (*Session, bool) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	s, ok := st.sessions[clientID]
	return s, ok
}

// CreateOrResume implements the CONNECT session handling:
//   - cleanStart=true: any session previously stored for clientID is
//     discarded (its unacked/queued state is abandoned) and a fresh
//     temporary session is returned with created=true.
//   - cleanStart=false: an existing persistent session is resumed with
//     created=false; if none exists a new persistent session is created.
func (st *Store) CreateOrResume(clientID string, cleanStart bool) (s *Session, created bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if cleanStart {
		fresh := NewSession(clientID, false)
		st.sessions[clientID] = fresh
		return fresh, true
	}
	if existing, ok := st.sessions[clientID]; ok && existing.Persistent() {
		return existing, false
	}
	// Either no stored session, or only a leftover temporary session
	// (it was still registered because its transport had not finished
	// cleanup): a non-clean CONNECT binds a brand-new persistent session.
	s = NewSession(clientID, true)
	st.sessions[clientID] = s
	return s, true
}

// Delete removes the session stored for clientID only when it is the same
// instance. Identity comparison prevents a new connection's session from
// being deleted by a late cleanup from a taken-over old connection.
func (st *Store) Delete(clientID string, s *Session) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	cur, ok := st.sessions[clientID]
	if !ok || cur != s {
		return false
	}
	delete(st.sessions, clientID)
	return true
}

// Put explicitly installs a session (used by tests).
func (st *Store) Put(s *Session) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.sessions[s.ClientID()] = s
}

// Len returns the number of registered sessions.
func (st *Store) Len() int {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return len(st.sessions)
}

// All returns a snapshot copy of every registered session.
func (st *Store) All() []*Session {
	st.mu.RLock()
	defer st.mu.RUnlock()
	out := make([]*Session, 0, len(st.sessions))
	for _, s := range st.sessions {
		out = append(out, s)
	}
	return out
}
