package godb

import "testing"

func TestLockManager(t *testing.T) {
	mgr := NewLockManager()
	read_tid1 := 1
	read_tid2 := 2
	write_tid3 := 3

	pageNo1 := 99
	mgr.AcquireLock(&read_tid1, pageNo1, ReadPerm)
	mgr.AcquireLock(&read_tid2, pageNo1, ReadPerm)
	req, ok := mgr.reqMap[pageNo1]
	if !ok {
		t.Errorf("can't find req")
	}

	if req.RdCnt != 2 {
		t.Errorf("req.RdCnt != 2")
	}

	ok = mgr.AcquireLock(&write_tid3, pageNo1, WritePerm)
	if ok {
		t.Errorf("acuqiring write-page success after read transation")
	}

	mgr.ReleaseLock(&read_tid1, pageNo1)
	mgr.ReleaseLock(&read_tid2, pageNo1)

	req, ok = mgr.reqMap[pageNo1]
	if ok {
		t.Errorf("req shouldn't  contain")
	}
	ok = mgr.AcquireLock(&write_tid3, pageNo1, WritePerm)
	req, ok = mgr.reqMap[pageNo1]
	if !ok {
		t.Errorf("req should contain")
	}
	if len(req.Tid) != 1 || req.Perm != WritePerm || req.RdCnt != 0 {
		t.Errorf("check failure")
	}
}

func TestLockManagerReacquireLock(t *testing.T) {
	mgr := NewLockManager()
	read_tid1 := 1
	read_tid2 := 2
	write_tid3 := 3

	pageNo1 := 99
	mgr.AcquireLock(&read_tid1, pageNo1, ReadPerm)
	mgr.AcquireLock(&read_tid2, pageNo1, ReadPerm)
	req, ok := mgr.reqMap[pageNo1]
	if !ok {
		t.Errorf("can't find req")
	}

	if req.RdCnt != 2 {
		t.Errorf("req.RdCnt != 2")
	}

	ok = mgr.AcquireLock(&write_tid3, pageNo1, WritePerm)
	if ok {
		t.Errorf("acuqiring write-page success after read transation")
	}

	mgr.ReleaseLock(&read_tid1, pageNo1)
	mgr.ReleaseLock(&read_tid2, pageNo1)

	req, ok = mgr.reqMap[pageNo1]
	if ok {
		t.Errorf("req shouldn't  contain")
	}
	ok = mgr.AcquireLock(&write_tid3, pageNo1, WritePerm)
	req, ok = mgr.reqMap[pageNo1]
	if !ok {
		t.Errorf("req should contain")
	}
	if len(req.Tid) != 1 || req.Perm != WritePerm || req.RdCnt != 0 {
		t.Errorf("check failure")
	}

	ok = mgr.AcquireLock(&write_tid3, pageNo1, ReadPerm)
	if !ok {
		t.Errorf("reacquiring should success")
	}
	ok = mgr.AcquireLock(&write_tid3, pageNo1, WritePerm)
	if !ok {
		t.Errorf("reacquiring should success")
	}
	ok = mgr.AcquireLock(&read_tid1, pageNo1, ReadPerm)
	if ok {
		t.Errorf("acquiring should fail")
	}
	// clean all lock
	mgr.ReleaseLock(&write_tid3, pageNo1)

	ok = mgr.AcquireLock(&write_tid3, pageNo1, ReadPerm)
	if !ok {
		t.Errorf("bug")
	}
	ok = mgr.AcquireLock(&read_tid1, pageNo1, ReadPerm)
	if !ok {
		t.Errorf("bug")
	}
	// try updating lock
	ok = mgr.AcquireLock(&write_tid3, pageNo1, WritePerm)
	if ok {
		t.Errorf("updating lock bug")
	}
}
