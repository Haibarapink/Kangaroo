package godb

import "testing"

func TestLockManager_AcquireLock(t *testing.T) {
	mgr := NewLockManager()
	read_tid1 := 1
	read_tid2 := 2
	write_tid3 := 3

	pageNo1 := 99
	pageNo2 := 98
	mgr.AcquireLock(&read_tid1, pageNo1, ReadPerm)
	mgr.AcquireLock(&read_tid2, pageNo1, ReadPerm)
	req, ok := mgr.reqMap[pageNo1]
	if !ok {
		t.Errorf("can't find req")
	}

	if req.RdCnt != 2 {

	}

}
