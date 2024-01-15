package godb

import (
	"testing"
)

func TestDelete(t *testing.T) {
	_, t1, t2, hf, bp, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	bp.CommitTransaction(tid)
	var f FieldType = FieldType{"age", "", IntType}
	filt, err := NewIntFilter(&ConstExpr{IntField{25}, IntType}, OpGt, &FieldExpr{f}, hf)
	if err != nil {
		t.Errorf(err.Error())
	}
	dop := NewDeleteOp(hf, filt)
	if dop == nil {
		t.Fatalf("delete op was nil")
	}
	tid = NewTID()
	bp.BeginTransaction(tid)
	iter, _ := dop.Iterator(tid)
	if iter == nil {
		t.Fatalf("iter was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	if tup == nil {
		t.Errorf("insert did not return tuple")
		return
	}
	intField, ok := tup.Fields[0].(IntField)
	if !ok || len(tup.Fields) != 1 || intField.Value != 1 {
		t.Errorf("invalid output tuple")
		return
	}
	bp.CommitTransaction(tid)

	tid = NewTID()
	bp.BeginTransaction(tid)

	iter, _ = hf.Iterator(tid)

	cnt := 0
	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		cnt++
	}
	if cnt != 1 {
		t.Errorf("unexpected number of results after deletion")
	}

}
