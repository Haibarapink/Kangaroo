package godb

import (
	"testing"
)

func toHeapPage(p Page) *heapPage {
	return p.(*heapPage)
}

// my test
func TestEvcit(t *testing.T) {
	_, t1, _, hf, bp, tid := makeTestVars()
	for hf.NumPages() != 10 {
		hf.insertTuple(&t1, tid)
	}
	pg1, _ := bp.GetPage(hf, 0, tid, WritePerm)
	pg2, _ := bp.GetPage(hf, 1, tid, WritePerm)
	pg3, _ := bp.GetPage(hf, 2, tid, WritePerm)
	hp1 := toHeapPage(*pg1)
	hp2 := toHeapPage(*pg2)
	hp3 := toHeapPage(*pg3)

	hp1.setDirty(true)
	hp2.setDirty(true)
	hp3.setDirty(true)

	_, err := bp.GetPage(hf, 3, tid, ReadPerm)
	if err == nil {
		t.Errorf("can't get any page")
	}

	pg4, err := bp.GetPage(hf, 3, tid, ReadPerm)
	if err != nil || pg4 == nil {
		t.Errorf("should get page 3")
	}
}

// my test
func TestCommitRight(t *testing.T) {
	_, _, t2, hf, bp, _ := makeTestVars()
	tid := NewTID()
	bp.BeginTransaction(tid)
	hf.insertTuple(&t2, tid)
	hf.insertTuple(&t2, tid)
	bp.CommitTransaction(tid)
	iter, err := hf.Iterator(tid)
	if err != nil {
		panic("err should not exist")
	}
	tuple, err := iter()
	if err != nil {
		panic("err should not exist")
	}
	tuple, err = iter()
	if tuple == nil {
		panic("there should be more tuple")
	}
	tuple, err = iter()
	if tuple != nil {
		panic("no more tuple")
	}
}

func TestLockingDataStruct(t *testing.T) {
	_, t1, t2, hf, bp, _ := makeTestVars()
	tid := NewTID()
	bp.BeginTransaction(tid)
	hf.insertTuple(&t2, tid)
	bp.CommitTransaction(tid)

	tid2 := NewTID()
	bp.BeginTransaction(tid2)
	err := hf.insertTuple(&t1, tid2)
	if err != nil {
		t.Errorf("%s", err.Error())
	}
	bp.AbortTransaction(tid2)
	iter, err := hf.Iterator(tid2)
	if err != nil {
		t.Errorf("%s", err.Error())
	}
	cnt := 0
	for {
		tuple, err := iter()
		if tuple == nil && err == nil {
			break
		}
		if err != nil {
			t.Errorf("%s", err.Error())
		}
		cnt++
	}
	if cnt != 1 {
		t.Errorf("count {%d} is not %d", cnt, 1)
	}
}

func TestGetPage(t *testing.T) {
	_, t1, t2, hf, bp, _ := makeTestVars()
	tid := NewTID()
	for i := 0; i < 300; i++ {
		bp.BeginTransaction(tid)
		err := hf.insertTuple(&t1, tid)
		if err != nil {
			t.Fatalf("%v", err)
		}
		err = hf.insertTuple(&t2, tid)
		if err != nil {
			t.Fatalf("%v", err)
		}
		//// hack to force dirty pages to disk
		//// because CommitTransaction may not be implemented
		//// yet if this is called in lab 1 or 2
		//for i := 0; i < hf.NumPages(); i++ {
		//	pg, err := bp.GetPage(hf, i, tid, ReadPerm)
		//	if pg == nil || err != nil {
		//		t.Fatal("page nil or error", err)
		//	}
		//	if (*pg).isDirty() {
		//		(*(*pg).getFile()).flushPage(pg)
		//		(*pg).setDirty(false)
		//	}
		//}
		// commit transaction
		bp.CommitTransaction(tid)
	}
	bp.BeginTransaction(tid)
	//expect 6 pages
	for i := 0; i < 6; i++ {
		pg, err := bp.GetPage(hf, i, tid, ReadPerm)
		if pg == nil || err != nil {
			t.Fatalf("failed to get page %d (err = %v)", i, err)
		}
	}
	_, err := bp.GetPage(hf, 7, tid, ReadPerm)
	if err == nil {
		t.Fatalf("No error when getting page 7 from a file with 6 pages.")
	}
}
