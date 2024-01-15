package godb

import (
	"fmt"
	"testing"
)

func TestIntFilter(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	var f FieldType = FieldType{"age", "", IntType}
	filt, err := NewIntFilter(&ConstExpr{IntField{25}, IntType}, OpGt, &FieldExpr{f}, hf)
	if err != nil {
		t.Errorf(err.Error())
	}
	iter, err := filt.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}

	cnt := 0
	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		fmt.Printf("filter passed tup %d: %v\n", cnt, tup)
		cnt++
	}
	if cnt != 1 {
		t.Errorf("unexpected number of results")
	}
}

func TestStringFilter(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	var f FieldType = FieldType{"name", "", StringType}
	filt, err := NewStringFilter(&ConstExpr{StringField{"sam"}, StringType}, OpEq, &FieldExpr{f}, hf)
	if err != nil {
		t.Errorf(err.Error())
	}
	iter, err := filt.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}

	cnt := 0
	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		fmt.Printf("filter passed tup %d: %v\n", cnt, tup)
		cnt++
	}
	if cnt != 1 {
		t.Errorf("unexpected number of results")
	}
}
