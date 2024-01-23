package godb

import (
	"testing"
)

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
		// hack to force dirty pages to disk
		// because CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2
		for i := 0; i < hf.NumPages(); i++ {
			pg, err := bp.GetPage(hf, i, tid, ReadPerm)
			if pg == nil || err != nil {
				t.Fatal("page nil or error", err)
			}
			if (*pg).isDirty() {
				(*(*pg).getFile()).flushPage(pg)
				(*pg).setDirty(false)
			}
		}
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
