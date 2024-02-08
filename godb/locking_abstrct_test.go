package godb

import (
	"sync"
	"testing"
	"time"
)

type bfp_demo struct {
	mgr *LockManager
	mu  sync.Mutex
	i   int
}

func NewBfpDemo() *bfp_demo {
	return &bfp_demo{NewLockManager(), *new(sync.Mutex), 0}
}

func (bfp *bfp_demo) GetPage(tid TransactionID) {
	bfp.mu.Lock()
	defer bfp.mu.Unlock()

	ok := false
	for !ok {
		ok = bfp.mgr.AcquireLock(tid, 1, ReadPerm)
		if ok {
			break
		}
		// ok is false
		bfp.mu.Unlock()
		time.Sleep(100)
		bfp.mu.Lock() // try lock
	}
	bfp.i++

	println(*tid, " Win ", bfp.i)
}

func TestAlgo(t *testing.T) {
	bfp := NewBfpDemo()
	cnt := 300
	for i := 0; i < cnt; i++ {
		go func() {
			bfp.GetPage(&i)
		}()
	}

	time.Sleep(3000)

	if bfp.i != cnt {
		t.Errorf("i {%d} != cnt {%d}", bfp.i, cnt)
	}
}
