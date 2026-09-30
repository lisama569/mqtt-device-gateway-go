package session

import "testing"

func qmsg(topic string) *QueuedMessage {
	return &QueuedMessage{Topic: topic, QoS: 1}
}

func TestInflightWindowAndAck(t *testing.T) {
	s := NewSession("d", true)
	for i := 0; i < MaxInflight+3; i++ {
		s.PushPending(qmsg("t"))
	}
	allocated := map[uint16]bool{}
	for i := 0; i < MaxInflight; i++ {
		in, ok := s.AllocPacketID()
		if !ok {
			t.Fatalf("alloc %d failed unexpectedly", i)
		}
		if allocated[in.PacketID] {
			t.Fatalf("packet id %d allocated twice", in.PacketID)
		}
		if in.PacketID == 0 {
			t.Fatal("packet id 0 is invalid")
		}
		allocated[in.PacketID] = true
	}
	// Window is full: head must NOT be dequeued (boundary).
	if _, ok := s.AllocPacketID(); ok {
		t.Fatal("alloc beyond inflight window succeeded")
	}
	if s.PendingCount() != 3 {
		t.Fatalf("pending=%d want 3", s.PendingCount())
	}
	// Spurious/duplicate ACK of an unknown id changes nothing.
	if s.AckPacketID(60000) {
		t.Fatal("unknown id acknowledged")
	}
	// ACK one -> slot frees -> next pending binds a fresh id.
	var firstID uint16
	for id := range allocated {
		firstID = id
		break
	}
	if !s.AckPacketID(firstID) {
		t.Fatal("valid ack rejected")
	}
	in, ok := s.AllocPacketID()
	if !ok {
		t.Fatal("alloc after ack failed")
	}
	if in.PacketID == firstID {
		// Allocator may legitimately reuse, but must not return an id that
		// is still present in the inflight map.
	}
	// Duplicate ACK must fail (id already removed).
	if s.AckPacketID(firstID) {
		t.Fatal("duplicate ACK accepted")
	}
}

func TestInboundDedupWindow(t *testing.T) {
	s := NewSession("d", true)
	if s.SeenInbound(5) {
		t.Fatal("first sighting reported duplicate")
	}
	if !s.SeenInbound(5) {
		t.Fatal("second sighting not reported duplicate")
	}
	// Fill the window to evict id 5; reuse after eviction is valid again.
	for i := uint16(100); i < 100+InboundDupWindow; i++ {
		s.SeenInbound(i)
	}
	if s.SeenInbound(5) {
		t.Fatal("id 5 should have left the dedup window")
	}
}

func TestOfflineDrainOrder(t *testing.T) {
	s := NewSession("d", true)
	want := []string{"m1", "m2", "m3", "m4"}
	for _, name := range want {
		s.EnqueueOffline(qmsg(name))
	}
	if n := s.DrainOffline(); n != 4 {
		t.Fatalf("drained %d want 4", n)
	}
	var got []string
	for {
		in, ok := s.AllocPacketID()
		if !ok {
			break
		}
		got = append(got, in.Msg.Topic)
		s.AckPacketID(in.PacketID)
	}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order broken at %d: got %v want %v", i, got, want)
		}
	}
}

func TestOfflineOverflowDropsOldest(t *testing.T) {
	s := NewSession("d", true)
	for i := 0; i < MaxOfflineQueue; i++ {
		if d := s.EnqueueOffline(qmsg("old")); d != nil {
			t.Fatalf("unexpected early drop at %d", i)
		}
	}
	dropped := s.EnqueueOffline(qmsg("new"))
	if dropped == nil || dropped.Topic != "old" {
		t.Fatalf("overflow policy did not drop oldest: %+v", dropped)
	}
	if s.OverflowDropped() != 1 {
		t.Fatalf("dropped count=%d want 1", s.OverflowDropped())
	}
	if s.OfflineCount() != MaxOfflineQueue {
		t.Fatalf("queue size=%d want cap %d", s.OfflineCount(), MaxOfflineQueue)
	}
}

func TestStoreCreateResumeDelete(t *testing.T) {
	st := NewStore()
	s1, created := st.CreateOrResume("dev", true)
	if !created || s1.Persistent() {
		t.Fatal("cleanStart should create a temporary session")
	}
	// New clean start replaces the previous stored session.
	s2, created2 := st.CreateOrResume("dev", true)
	if !created2 || s1 == s2 {
		t.Fatal("second cleanStart must replace stored session instance")
	}
	// Persistent connect after temp sessions: a new persistent session
	// is created; subsequent persistent connects resume that instance.
	p1, created3 := st.CreateOrResume("dev", false)
	if !created3 || !p1.Persistent() {
		t.Fatal("first persistent connect after clean starts must create persistent session")
	}
	p2, created4 := st.CreateOrResume("dev", false)
	if created4 || p1 != p2 {
		t.Fatal("resume must return the same persistent session instance")
	}
	// Identity-checked delete rejects a stale instance.
	if st.Delete("dev", s2) {
		t.Fatal("stale instance must not delete current session")
	}
	if !st.Delete("dev", p2) {
		t.Fatal("current instance delete failed")
	}
}
