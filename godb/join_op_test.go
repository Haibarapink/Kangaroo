package godb

import (
	"os"
	"testing"
	"time"
)

const JoinTestFile string = "JoinTestFile.dat"

func TestJoin(t *testing.T) {
	td, t1, t2, hf, bp, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	hf.insertTuple(&t2, tid)

	os.Remove(JoinTestFile)
	hf2, _ := NewHeapFile(JoinTestFile, &td, bp)
	hf2.insertTuple(&t1, tid)
	hf2.insertTuple(&t2, tid)
	hf2.insertTuple(&t2, tid)

	outT1 := joinTuples(&t1, &t1)
	outT2 := joinTuples(&t2, &t2)

	leftField := FieldExpr{td.Fields[1]}
	join, err := NewIntJoin(hf, &leftField, hf2, &leftField, 100)
	if err != nil {
		t.Errorf("unexpected error initializing join")
		return
	}
	iter, err := join.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("iter was nil")
	}
	cnt := 0
	cntOut1 := 0
	cntOut2 := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		if t.equals(outT1) {
			cntOut1++
		} else if t.equals(outT2) {
			cntOut2++
		}
		//fmt.Printf("got tuple %v: %v\n", cnt, t)
		cnt++
	}
	if cnt != 5 {
		t.Errorf("unexpected number of join results (%d, expected 5)", cnt)
	}
	if cntOut1 != 1 {
		t.Errorf("unexpected number of t1 results (%d, expected 1)", cntOut1)
	}
	if cntOut2 != 4 {
		t.Errorf("unexpected number of t2 results (%d, expected 4)", cntOut2)
	}

}

const BigJoinFile1 string = "jointest1.dat"
const BigJoinFile2 string = "jointest2.dat"

//This test joins two large heap files (each containing ntups tuples). A simple
//nested loops join will take a LONG time to complete this join, so we've added
//a timeout that will cause the join to fail after 10 seconds.
//
//Note that this test is optional;  passing it will give extra credit, as
//describe in the lab 2 assignment.

func TestBigJoinOptional(t *testing.T) {

	timeout := time.After(20 * time.Second)

	done := make(chan bool)

	go func() {
		ntups := 314159
		bp := NewBufferPool(100)
		td := TupleDesc{[]FieldType{{"name", "", IntType}}}
		os.Remove(BigJoinFile1)
		os.Remove(BigJoinFile2)
		hf1, err := NewHeapFile(BigJoinFile1, &td, bp)
		if err != nil {
			t.Errorf("unexpected error heap file")
			done <- true
			return
		}
		hf2, err := NewHeapFile(BigJoinFile2, &td, bp)
		if err != nil {
			t.Errorf("unexpected error heap file")
			done <- true
			return
		}
		var tid TransactionID

		for i := 0; i < ntups; i++ {
			if i%5000 == 0 {
				if tid != nil {
					// hack to force dirty pages to disk
					// because CommitTransaction may not be implemented
					// yet if this is called in lab 1 or 2
					for j := 0; j < hf1.NumPages(); j++ {
						pg, err := bp.GetPage(hf1, j, tid, ReadPerm)
						if pg == nil || err != nil {
							t.Fatal("page nil or error", err)
						}
						if (*pg).isDirty() {
							(*hf1).flushPage(pg)
							(*pg).setDirty(false)
						}

					}
					for j := 0; j < hf2.NumPages(); j++ {
						pg, err := bp.GetPage(hf2, j, tid, ReadPerm)
						if pg == nil || err != nil {
							t.Fatal("page nil or error", err)
						}
						if (*pg).isDirty() {
							(*hf2).flushPage(pg)
							(*pg).setDirty(false)
						}

					}

					// commit transaction
					bp.CommitTransaction(tid)
				}
				tid = NewTID()
				bp.BeginTransaction(tid)
			}

			tup := Tuple{td, []DBValue{IntField{int64(i)}}, nil}
			err := hf1.insertTuple(&tup, tid)
			if err != nil {
				t.Errorf(err.Error())
				return
			}

			err = hf2.insertTuple(&tup, tid)
			if err != nil {
				t.Errorf(err.Error())
				return
			}

		}
		bp.CommitTransaction(tid)
		tid = NewTID()
		bp.BeginTransaction(tid)
		leftField := FieldExpr{td.Fields[0]}
		join, err := NewIntJoin(hf1, &leftField, hf2, &leftField, 100000)
		if err != nil {
			t.Errorf("unexpected error initializing join")
			return
		}
		iter, err := join.Iterator(tid)
		if err != nil {
			t.Errorf(err.Error())
			return

		}
		if iter == nil {
			t.Errorf("iter was nil")
			done <- true
			return
		}
		cnt := 0
		for {
			tup, err := iter()
			if err != nil {
				t.Errorf(err.Error())
				return
			}
			if tup == nil {
				break
			}
			cnt++
		}
		if cnt != ntups {
			t.Errorf("unexpected number of join results (%d, expected %d)", cnt, ntups)
		}
		done <- true

	}()

	select {
	case <-timeout:
		t.Fatal("Test didn't finish in time")
	case <-done:
	}

}
