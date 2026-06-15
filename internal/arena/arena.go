// Package arena provides a fixed-size, lock-free bump allocator.
//
// A Slab[T] preallocates a backing array of capacity elements of type T and
// hands them out one (or n) at a time by atomically advancing a counter. When
// the capacity is exhausted, allocation returns nil — the caller treats this as
// "full" and rotates to a fresh Slab.
//
// Why a typed slice rather than a raw []byte arena: the backing store is an
// ordinary Go slice, so the garbage collector scans it and follows any pointers
// stored in allocated elements (string headers, inter-node pointers, etc.).
// This means:
//   - Referenced heap data (e.g. string backing bytes) is kept alive correctly,
//     with no copying into the arena required.
//   - Reset is safe to call concurrently with readers: dropping the Slab's own
//     reference does not free anything still reachable. A reader holding a
//     pointer into the backing array keeps that array alive until it is done.
//     There is no manual free and therefore no use-after-free hazard.
//
// Allocation is lock-free: it is a single atomic add. Allocations from one Slab
// by many goroutines receive disjoint regions and never block one another.
package arena

import "sync/atomic"

// Slab is a fixed-capacity, lock-free bump allocator for values of type T.
type Slab[T any] struct {
	next  atomic.Int64 // next free index
	items []T
}

// NewSlab returns a Slab that can allocate up to capacity elements of T.
func NewSlab[T any](capacity int) *Slab[T] {
	if capacity < 0 {
		capacity = 0
	}
	return &Slab[T]{items: make([]T, capacity)}
}

// Get reserves a single element and returns a pointer to it, or nil if the slab
// is full. The returned element holds T's zero value.
//
// Uses a CAS loop so a failed reservation never consumes capacity; this keeps it
// lock-free while remaining well-behaved when allocations of different sizes
// race near the end of the pool.
func (s *Slab[T]) Get() *T {
	for {
		old := s.next.Load()
		if old >= int64(len(s.items)) {
			return nil
		}
		if s.next.CompareAndSwap(old, old+1) {
			return &s.items[old]
		}
	}
}

// GetN reserves n contiguous elements and returns them as a slice of length and
// capacity n, or nil if the slab cannot satisfy the request. Elements hold T's
// zero value. A failed reservation does not consume capacity.
func (s *Slab[T]) GetN(n int) []T {
	if n <= 0 {
		return nil
	}
	for {
		old := s.next.Load()
		end := old + int64(n)
		if end > int64(len(s.items)) {
			return nil
		}
		if s.next.CompareAndSwap(old, end) {
			return s.items[old:end:end]
		}
	}
}

// Len reports how many elements have been allocated.
func (s *Slab[T]) Len() int {
	return int(s.next.Load())
}

// Cap reports the total capacity.
func (s *Slab[T]) Cap() int { return len(s.items) }

// Reset drops the backing array so the garbage collector can reclaim it once no
// other references (e.g. pointers held by in-flight readers) remain. The Slab
// must not be allocated from afterwards. Safe to call concurrently with readers
// that hold pointers obtained from earlier allocations.
func (s *Slab[T]) Reset() {
	s.items = nil
}
