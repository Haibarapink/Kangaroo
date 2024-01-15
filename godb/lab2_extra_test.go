package godb

import (
	"os"
	"testing"
)

// Note that we will not use these tests for grading of lab2 but we believe
// they may be helpful in debugging your lab1 and lab2 implementations. We
// have tried to take into account the suggestions from Piazza — thank you
// all for your constructive input!

func makeJoinOrderingVars() (*HeapFile, *HeapFile, Tuple, Tuple, *BufferPool) {
	var td1 = TupleDesc{Fields: []FieldType{
		{Fname: "a", Ftype: StringType},
		{Fname: "b", Ftype: IntType},
	}}
	var td2 = TupleDesc{Fields: []FieldType{
		{Fname: "c", Ftype: StringType},
		{Fname: "d", Ftype: IntType},
	}}

	var t1 = Tuple{
		Desc: td1,
		Fields: []DBValue{
			StringField{"sam"},
			IntField{25},
		}}

	var t2 = Tuple{
		Desc: td2,
		Fields: []DBValue{
			StringField{"george jones"},
			IntField{25},
		}}

	bp := NewBufferPool(3)
	os.Remove(TestingFile)
	hf1, err := NewHeapFile(TestingFile, &td1, bp)
	if err != nil {
		print("ERROR MAKING TEST VARS, BLARGH")
		panic(err)
	}

	os.Remove(TestingFile2)
	hf2, err := NewHeapFile(TestingFile2, &td2, bp)
	if err != nil {
		print("ERROR MAKING TEST VARS, BLARGH")
		panic(err)
	}

	return hf1, hf2, t1, t2, bp
}

func makeOrderByOrderingVars() (*HeapFile, Tuple, TupleDesc, *BufferPool) {
	var td = TupleDesc{Fields: []FieldType{
		{Fname: "a", Ftype: StringType},
		{Fname: "b", Ftype: IntType},
		{Fname: "c", Ftype: IntType},
	}}

	var t = Tuple{
		Desc: td,
		Fields: []DBValue{
			StringField{"sam"},
			IntField{25},
			IntField{5},
		}}

	bp := NewBufferPool(3)
	os.Remove(TestingFile)
	hf, err := NewHeapFile(TestingFile, &td, bp)
	if err != nil {
		print("ERROR MAKING TEST VARS, BLARGH")
		panic(err)
	}

	return hf, t, td, bp
}

func TestSetDirty(t *testing.T) {
	_, t1, _, hf, bp, _ := makeTestVars()
	tid := NewTID()
	bp.BeginTransaction(tid)
	for i := 0; i < 308; i++ {
		err := hf.insertTuple(&t1, tid)
		if err != nil && (i == 306 || i == 307) {
			return
		} else if err != nil {
			t.Fatalf("%v", err)
		}
	}
	bp.CommitTransaction(tid)
	t.Fatalf("Expected error due to all pages in BufferPool being dirty")
}

func TestDirtyBit(t *testing.T) {
	_, t1, _, hf, bp, _ := makeTestVars()

	tid := NewTID()
	bp.BeginTransaction(tid)
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t1, tid)
	page, _ := bp.GetPage(hf, 0, tid, ReadPerm)
	if !(*page).isDirty() {
		t.Fatalf("Expected page to be dirty")
	}
}

func TestJoinFieldOrder(t *testing.T) {
	hf1, hf2, t1, t2, bp := makeJoinOrderingVars()

	tid := NewTID()
	bp.BeginTransaction(tid)

	hf1.insertTuple(&t1, tid)
	hf2.insertTuple(&t2, tid)

	leftField := FieldExpr{t1.Desc.Fields[1]}
	rightField := FieldExpr{t2.Desc.Fields[1]}

	join, err := NewIntJoin(hf1, &leftField, hf2, &rightField, 100)
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

	var tdExpected = TupleDesc{Fields: []FieldType{
		{Fname: "a", Ftype: StringType},
		{Fname: "b", Ftype: IntType},
		{Fname: "c", Ftype: StringType},
		{Fname: "d", Ftype: IntType},
	}}

	tj, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if !tdExpected.equals(&tj.Desc) {
		t.Fatalf("Unexpected descriptor of joined tuple")
	}
}

func TestOrderByFieldsOrder(t *testing.T) {
	hf, tup, td, bp := makeOrderByOrderingVars()

	tid := NewTID()
	bp.BeginTransaction(tid)
	hf.insertTuple(&tup, tid)

	bs := make([]bool, 2)
	for i := range bs {
		bs[i] = false
	}

	exprs := []Expr{&FieldExpr{td.Fields[0]}, &FieldExpr{td.Fields[2]}}

	oby, err := NewOrderBy(exprs, hf, bs)
	if err != nil {
		t.Fatalf(err.Error())
	}

	iter, _ := oby.Iterator(tid)
	if iter == nil {
		t.Fatalf("iter was nil")
	}

	var expectedDesc = TupleDesc{Fields: []FieldType{
		{Fname: "a", Ftype: StringType},
		{Fname: "b", Ftype: IntType},
		{Fname: "c", Ftype: IntType},
	}}

	tupOut, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if !expectedDesc.equals(&tupOut.Desc) {
		t.Fatalf("Unexpected descriptor of ordered tuple")
	}
}

func TestProjectOrdering(t *testing.T) {
	hf, tup, td, bp := makeOrderByOrderingVars()

	tid := NewTID()
	bp.BeginTransaction(tid)
	hf.insertTuple(&tup, tid)

	var outNames = []string{"out1", "out2"}
	exprs := []Expr{&FieldExpr{td.Fields[2]}, &FieldExpr{td.Fields[0]}}

	proj, _ := NewProjectOp(exprs, outNames, false, hf)
	if proj == nil {
		t.Fatalf("project was nil")
	}
	iter, _ := proj.Iterator(tid)
	if iter == nil {
		t.Fatalf("iter was nil")
	}

	tupOut, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}

	var expectedDesc = TupleDesc{Fields: []FieldType{
		{Fname: "out1", Ftype: IntType},
		{Fname: "out2", Ftype: StringType},
	}}

	if !expectedDesc.equals(&tupOut.Desc) {
		t.Fatalf("Unexpected descriptor of projected tuple")
	}

}

func TestJoinTupleNil(t *testing.T) {
	_, t1, t2, _, _, _ := makeTestVars()
	tNew := joinTuples(&t1, nil)
	if !tNew.equals(&t1) {
		t.Fatalf("Unexpected output of joinTuple with nil")
	}
	tNew2 := joinTuples(nil, &t2)
	if !tNew2.equals(&t2) {
		t.Fatalf("Unexpected output of joinTuple with nil")
	}
}

func TestJoinTuplesDesc(t *testing.T) {
	_, t1, t2, _, _, _ := makeTestVars()
	tNew := joinTuples(&t1, &t2)
	if len(tNew.Desc.Fields) != 4 {
		t.Fatalf("Expected 4 fields in desc after join")
	}
	fields := []string{"name", "age", "name", "age"}
	for i, fname := range fields {
		if tNew.Desc.Fields[i].Fname != fname {
			t.Fatalf("expected %dth field to be named %s", i, fname)
		}
	}
}

func TestHeapFileSize(t *testing.T) {
	_, t1, _, hf, bp, _ := makeTestVars()

	tid := NewTID()
	bp.BeginTransaction(tid)
	hf.insertTuple(&t1, tid)
	page, err := bp.GetPage(hf, 0, tid, ReadPerm)
	if err != nil {
		t.Fatalf("unexpected error, getPage, %s", err.Error())
	}
	hf.flushPage(page)
	info, err := os.Stat(TestingFile)
	if err != nil {
		t.Fatalf("unexpected error, stat, %s", err.Error())
	}
	if info.Size() != int64(PageSize) {
		t.Fatalf("heap file page is not %d bytes;  NOTE:  This error may be OK, but many implementations that don't write full pages break.", PageSize)
	}

}

func TestProjectExtra(t *testing.T) {
	_, _, t1, _, _ := makeJoinOrderingVars()
	ft1 := FieldType{"a", "", StringType}
	ft2 := FieldType{"b", "", IntType}
	outTup, _ := t1.project([]FieldType{ft1})
	if (len(outTup.Fields)) != 1 {
		t.Fatalf("project returned %d fields, expected 1", len(outTup.Fields))
	}
	v, ok := outTup.Fields[0].(StringField)

	if !ok {
		t.Fatalf("project of name didn't return string")
	}
	if v.Value != "sam" {
		t.Fatalf("project didn't return sam")

	}
	outTup, _ = t1.project([]FieldType{ft2})
	if (len(outTup.Fields)) != 1 {
		t.Fatalf("project returned %d fields, expected 1", len(outTup.Fields))
	}
	v2, ok := outTup.Fields[0].(IntField)

	if !ok {
		t.Fatalf("project of name didn't return int")
	}
	if v2.Value != 25 {
		t.Fatalf("project didn't return 25")

	}

	outTup, _ = t1.project([]FieldType{ft2, ft1})
	if (len(outTup.Fields)) != 2 {
		t.Fatalf("project returned %d fields, expected 2", len(outTup.Fields))
	}
	v, ok = outTup.Fields[1].(StringField)
	if !ok {
		t.Fatalf("project of name didn't return string in second field")
	}
	if v.Value != "sam" {
		t.Fatalf("project didn't return sam")

	}

	v2, ok = outTup.Fields[0].(IntField)
	if !ok {
		t.Fatalf("project of name didn't return int in first field")
	}
	if v2.Value != 25 {
		t.Fatalf("project didn't return 25")

	}

}

func TestBufferLen(t *testing.T) {

	td, _, _, hf, _, _ := makeTestVars()
	page := newHeapPage(&td, 0, hf)
	free := page.getNumSlots()

	for i := 0; i < free-1; i++ {
		var addition = Tuple{
			Desc: td,
			Fields: []DBValue{
				StringField{"sam"},
				IntField{int64(i)},
			},
		}
		page.insertTuple(&addition)
	}

	buf, _ := page.toBuffer()

	if buf.Len() != PageSize {
		t.Fatalf("HeapPage.toBuffer returns buffer of unexpected size;  NOTE:  This error may be OK, but many implementations that don't write full pages break.")
	}

}

func TestHeapFileIteratorExtra(t *testing.T) {
	_, t1, _, hf, bp, _ := makeTestVars()
	tid := NewTID()
	bp.BeginTransaction(tid)

	it, err := hf.Iterator(tid)
	_, err = it()
	if err != nil {
		t.Fatalf("Empty heap file iterator should return nil,nil")
	}
	hf.insertTuple(&t1, tid)
	it, err = hf.Iterator(tid)
	pg, err := it()
	if err != nil {
		t.Fatalf("Iterating over heap file with one tuple returned error %s", err.Error())
	}
	if pg == nil {
		t.Fatalf("Should have gotten 1 page in heap file iterator")
	}
	pg, err = it()
	if pg != nil {
		t.Fatalf("More than 1 page in heap file iterator!")
	}
	if err != nil {
		t.Fatalf("Iterator returned error at end, expected nil, nil, got nil, %s", err.Error())
	}
}

func TestBufferPoolHoldsMultipleHeapFiles(t *testing.T) {
	td, t1, t2, hf, bp, tid := makeTestVars()
	os.Remove(TestingFile2)
	hf2, err := NewHeapFile(TestingFile2, &td, bp)
	if err != nil {
		print("ERROR MAKING TEST VARS, BLARGH")
		panic(err)
	}

	err1 := hf.insertTuple(&t1, tid)
	err2 := hf.insertTuple(&t1, tid)
	err3 := hf2.insertTuple(&t2, tid)

	if err1 != nil || err2 != nil || err3 != nil {
		t.Errorf("The BufferPool should be able to handle multiple files")
	}
	// bp contains 2 dirty pages at this point

	hf2TupCntPerPage := 0
	for hf2.NumPages() <= 1 {
		if err := hf2.insertTuple(&t2, tid); err != nil {
			t.Errorf("%v", err)
		}
		hf2TupCntPerPage++
	}
	// bp contains 3 dirty pages at this point

	for i := 0; i < hf2TupCntPerPage-1; i++ {
		if err := hf2.insertTuple(&t2, tid); err != nil {
			t.Errorf("%v", err)
		}
	}

	// bp contains 3 dirty pages at this point, including 2 full pages of hf2
	_ = hf2.insertTuple(&t2, tid)
	if err := hf2.insertTuple(&t2, tid); err == nil {
		t.Errorf("should cause bufferpool dirty page overflow here")
	}
}

func TestTupleProjectExtra(t *testing.T) {
	var td = TupleDesc{Fields: []FieldType{
		{Fname: "name1", TableQualifier: "tq1", Ftype: StringType},
		{Fname: "name2", TableQualifier: "tq2", Ftype: StringType},
		{Fname: "name1", TableQualifier: "tq2", Ftype: StringType},
	}}

	var t1 = Tuple{
		Desc: td,
		Fields: []DBValue{
			StringField{"SFname1tq1"},
			StringField{"SFname2tq2"},
			StringField{"SFname1tq2"},
		}}

	t2, err := t1.project([]FieldType{
		{Fname: "name1", TableQualifier: "tq1", Ftype: StringType},
		{Fname: "name2", TableQualifier: "", Ftype: StringType},
		{Fname: "name1", TableQualifier: "tq1", Ftype: StringType},
		{Fname: "name2", TableQualifier: "tq2", Ftype: StringType},
		{Fname: "name1", TableQualifier: "tq2", Ftype: StringType},
	})


	if err != nil {
		t.Errorf("%v", err)
	}

if t2.Fields[0].(StringField).Value != "SFname1tq1" {
		t.Errorf("wrong match 0")
	}

	if t2.Fields[1].(StringField).Value != "SFname2tq2" {
		t.Errorf("wrong match 1")
	}

	if t2.Fields[2].(StringField).Value != "SFname1tq1" {
		t.Errorf("wrong match 2")
	}
	if t2.Fields[3].(StringField).Value != "SFname2tq2" {
		t.Errorf("wrong match 3")
	}
	if t2.Fields[4].(StringField).Value != "SFname1tq2" {
		t.Errorf("wrong match 4")
	}

}

func TestTupleJoinDesc(t *testing.T) {

	var td1 = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
	}}

	var td2 = TupleDesc{Fields: []FieldType{
		{Fname: "age2", Ftype: IntType},
		{Fname: "name2", Ftype: StringType},
	}}

	var t1 = Tuple{
		Desc: td1,
		Fields: []DBValue{
			StringField{"sam"},
			IntField{25},
		}}

	var t2 = Tuple{
		Desc: td2,
		Fields: []DBValue{
			IntField{999},
			StringField{"george jones"},
		}}

	tNew := joinTuples(&t1, &t2)
	if len(tNew.Desc.Fields) != 4 {
		t.Fatalf("unexpected number of desc fields after join")
	}

	var tdAns = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
		{Fname: "age2", Ftype: IntType},
		{Fname: "name2", Ftype: StringType},
	}}

	if !tNew.Desc.equals(&tdAns) {
		t.Fatalf("unexpected desc after join")
	}
}
