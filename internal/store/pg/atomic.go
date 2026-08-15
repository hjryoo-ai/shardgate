package pg

import "sync/atomic"

// atomic64 는 이 패키지 안에서만 쓰는 얇은 카운터다.
type atomic64 struct{ v atomic.Int64 }

func (a *atomic64) add(n int64) { a.v.Add(n) }
func (a *atomic64) load() int64 { return a.v.Load() }
