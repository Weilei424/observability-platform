// Package syncx holds synchronization primitives the standard library lacks.
package syncx

import (
	"context"
	"sync"
)

// Mutex is a mutual-exclusion lock whose acquisition can be bounded by a
// context: a flush with a deadline must not wait past it behind another
// flush that holds the lock. The zero value is an unlocked Mutex.
type Mutex struct {
	once sync.Once
	ch   chan struct{}
}

func (m *Mutex) sem() chan struct{} {
	m.once.Do(func() { m.ch = make(chan struct{}, 1) })
	return m.ch
}

// Lock acquires m, waiting as long as it takes.
func (m *Mutex) Lock() { m.sem() <- struct{}{} }

// LockContext acquires m, or gives up when ctx is done first and returns its
// error without holding m.
func (m *Mutex) LockContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case m.sem() <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Unlock releases m. Unlocking an unlocked Mutex panics, as with sync.Mutex.
func (m *Mutex) Unlock() {
	select {
	case <-m.sem():
	default:
		panic("syncx: unlock of unlocked mutex")
	}
}
