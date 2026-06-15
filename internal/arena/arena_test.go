package arena

import (
	"sync"
	"testing"
)

func TestSlabGetExhaustion(t *testing.T) {
	s := NewSlab[int](3)
	for i := 0; i < 3; i++ {
		if p := s.Get(); p == nil {
			t.Fatalf("Get %d returned nil before capacity reached", i)
		}
	}
	if p := s.Get(); p != nil {
		t.Error("Get past capacity should return nil")
	}
	if s.Len() != 3 || s.Cap() != 3 {
		t.Errorf("Len=%d Cap=%d, want 3,3", s.Len(), s.Cap())
	}
}

func TestSlabGetNExhaustion(t *testing.T) {
	s := NewSlab[int](5)
	a := s.GetN(3)
	if len(a) != 3 || cap(a) != 3 {
		t.Fatalf("GetN(3) len=%d cap=%d, want 3,3", len(a), cap(a))
	}
	if b := s.GetN(3); b != nil {
		t.Error("GetN(3) should fail with only 2 slots left")
	}
	if b := s.GetN(2); len(b) != 2 {
		t.Errorf("GetN(2) len=%d, want 2", len(b))
	}
}

func TestSlabConcurrentDisjoint(t *testing.T) {
	const n = 10000
	s := NewSlab[int](n)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				p := s.Get()
				if p == nil {
					return
				}
				*p = id // each goroutine stamps its slots; disjoint => no race
			}
		}(g)
	}
	wg.Wait()
	if s.Len() != n {
		t.Errorf("allocated %d slots, want %d", s.Len(), n)
	}
}

func TestSlabReset(t *testing.T) {
	s := NewSlab[int](2)
	s.Get()
	s.Reset()
	if s.Cap() != 0 {
		t.Errorf("Cap after Reset = %d, want 0", s.Cap())
	}
}
