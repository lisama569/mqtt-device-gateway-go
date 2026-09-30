package session

import "testing"

func TestEnqueuePopAckOrder(t *testing.T) {
	s := NewSession("d", false)
	ids := []uint16{}
	for i := 0; i < 3; i++ {
		id, ok := s.Enqueue("t", []byte{byte(i)}, 1)
		if !ok {
			t.Fatal("enqueue failed")
		}
		ids = append(ids, id)
	}
	if s.QueuedLen() != 3 {
		t.Fatalf("queued=%d", s.QueuedLen())
	}
	for i, want := range ids {
		m, ok := s.PopForSend()
		if !ok || m.PacketID != want || m.Payload[0] != byte(i) {
			t.Fatalf("pop %d out of order: %+v %v", i, m, ok)
		}
	}
	if s.InflightLen() != 3 {
		t.Fatalf("inflight=%d", s.InflightLen())
	}
	// duplicate ack is idempotent
	if !s.AckPending(ids[0]) {
		t.Fatal("first ack should succeed")
	}
	if s.AckPending(ids[0]) {
		t.Fatal("duplicate ack must report false and change nothing")
	}
}

func TestResendMarksDupAndOrdered(t *testing.T) {
	s := NewSession("d", false)
	var ids []uint16
	for i := 0; i < 5; i++ {
		id, _ := s.Enqueue("t", nil, 1)
		ids = append(ids, id)
		s.PopForSend()
	}
	got := s.ResendInflight()
	if len(got) != 5 {
		t.Fatalf("resend count=%d", len(got))
	}
	var prev uint16
	for i, m := range got {
		if !m.Dup {
			t.Fatalf("msg %d missing dup flag", i)
		}
		if i > 0 && m.PacketID <= prev {
			t.Fatal("resend not ascending")
		}
		prev = m.PacketID
	}
}

func TestWindowExhaustion(t *testing.T) {
	s := NewSession("d", false)
	for i := 0; i < 65535; i++ {
		if _, ok := s.Enqueue("t", nil, 1); !ok {
			t.Fatalf("window exhausted at %d", i)
		}
	}
	if _, ok := s.Enqueue("t", nil, 1); ok {
		t.Fatal("65536th enqueue must fail")
	}
	// free one and re-allocate
	m, _ := s.PopForSend()
	s.AckPending(m.PacketID)
	if _, ok := s.Enqueue("t", nil, 1); !ok {
		t.Fatal("enqueue after ack should succeed")
	}
}

func TestStorePersistent(t *testing.T) {
	st := NewStore()
	s1, created1 := st.GetOrCreate("dev", false)
	if !created1 {
		t.Fatal("first should be created")
	}
	s2, created2 := st.GetOrCreate("dev", false)
	if created2 || s1 != s2 {
		t.Fatal("persistent session must be reused")
	}
	if st.Len() != 1 {
		t.Fatalf("store len=%d", st.Len())
	}
	st.Delete("dev")
	if st.Len() != 0 {
		t.Fatal("delete failed")
	}
	// clean sessions never enter the store
	_, _ = st.GetOrCreate("tmp", true)
	if st.Len() != 0 {
		t.Fatal("clean session leaked into store")
	}
}

func TestSubscriptions(t *testing.T) {
	s := NewSession("d", false)
	s.AddSubscription("a/+", 1)
	s.AddSubscription("b/#", 0)
	if len(s.Subscriptions()) != 2 {
		t.Fatal("sub count")
	}
	if !s.RemoveSubscription("a/+") || s.RemoveSubscription("a/+") {
		t.Fatal("remove semantics wrong")
	}
}
