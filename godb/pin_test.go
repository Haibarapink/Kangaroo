package godb

import (
	"testing"
)

func TestIteratorPinCorrect(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	for _, v := range hf.bufPool.pin {
		if v != 0 {
			t.Errorf("v should be 0")
		}
	}

	if hf.NumPages() != 1 {
		t.Errorf("page number is 1")
	}

	iter, _ := hf.Iterator(tid)
	iter()
	for _, v := range hf.bufPool.pin {
		if v != 1 {
			t.Errorf("v should be 1")
		}
	}
	iter()
	for _, v := range hf.bufPool.pin {
		if v != 1 {
			t.Errorf("v should be 1")
		}
	}
	iter()
	for _, v := range hf.bufPool.pin {
		if v != 0 {
			t.Errorf("v should be 0")
		}
	}
}

func TestIteratorPinCorrectWithManyPages(t *testing.T) {
	_, t1, _, hf, _, tid := makeTestVars()
	for hf.NumPages() != 20 {
		hf.insertTuple(&t1, tid)
	}
	for _, v := range hf.bufPool.pin {
		if v != 0 {
			t.Errorf("v should be 0")
		}
	}

	iter, _ := hf.Iterator(tid)
	for {
		tp, _ := iter()
		if tp == nil {
			break
		}
	}

	for _, v := range hf.bufPool.pin {
		if v != 0 {
			t.Errorf("v should be 0")
		}
	}
}

func TestDeletePinCorrect(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	for _, v := range hf.bufPool.pin {
		if v != 0 {
			t.Errorf("v should be 0")
		}
	}

	if hf.NumPages() != 1 {
		t.Errorf("page number is 1")
	}

	iter, _ := hf.Iterator(tid)
	tp, _ := iter()
	hf.deleteTuple(tp, tid)
	for _, v := range hf.bufPool.pin {
		if v != 1 {
			t.Errorf("v should be 0")
		}
	}

	iter2, _ := hf.Iterator(tid)
	iter2()
	for _, v := range hf.bufPool.pin {
		if v != 2 {
			t.Errorf("v should be 0")
		}
	}
}
