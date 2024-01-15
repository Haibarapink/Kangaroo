package godb

import (
	"testing"
)

func TestSimpleSumAgg(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars()

	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	sa := SumAggState[int64]{}
	expr := FieldExpr{t1.Desc.Fields[1]}
	sa.Init("sum", &expr, intAggGetter)
	agg := NewAggregator([]AggState{&sa}, hf)
	iter, err := agg.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("Expected non-null tuple")
	}
	sum := tup.Fields[0].(IntField).Value
	if sum != 1024 {
		t.Errorf("unexpected sum")
	}
}

func TestMinStringAgg(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	sa := MinAggState[string]{}
	expr := FieldExpr{t1.Desc.Fields[0]}
	sa.Init("min", &expr, stringAggGetter)
	agg := NewAggregator([]AggState{&sa}, hf)
	iter, err := agg.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("Expected non-null tuple")
	}
	min := tup.Fields[0].(StringField).Value
	if min != "george jones" {
		t.Errorf("incorrect min")
	}
}

func TestSimpleCountAgg(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	sa := CountAggState{}
	expr := FieldExpr{t1.Desc.Fields[0]}
	sa.Init("count", &expr, nil)
	agg := NewAggregator([]AggState{&sa}, hf)
	iter, err := agg.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("Expected non-null tuple")
	}
	cnt := tup.Fields[0].(IntField).Value
	if cnt != 2 {
		t.Errorf("unexpected count")
	}
}

func TestMultiAgg(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	ca := CountAggState{}
	expr := FieldExpr{t1.Desc.Fields[0]}
	ca.Init("count", &expr, nil)
	sa := SumAggState[int64]{}
	expr = FieldExpr{t1.Desc.Fields[1]}
	sa.Init("sum", &expr, intAggGetter)

	agg := NewAggregator([]AggState{&ca, &sa}, hf)
	iter, err := agg.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("Expected non-null tuple")
	}
	cnt := tup.Fields[0].(IntField).Value
	if cnt != 2 {
		t.Errorf("unexpected count")
	}
	sum := tup.Fields[1].(IntField).Value
	if sum != 1024 {
		t.Errorf("unexpected sum")
	}

}

func TestGbyCountAgg(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	hf.insertTuple(&t2, tid)
	hf.insertTuple(&t2, tid)

	gbyFields := []Expr{&FieldExpr{hf.Descriptor().Fields[0]}}
	sa := CountAggState{}
	expr := FieldExpr{t1.Desc.Fields[0]}
	sa.Init("count", &expr, nil)

	agg := NewGroupedAggregator([]AggState{&sa}, gbyFields, hf)
	iter, _ := agg.Iterator(tid)
	fields := []FieldType{
		{"name", "", StringType},
		{"count", "", IntType},
	}
	outt1 := Tuple{TupleDesc{fields},
		[]DBValue{
			StringField{"sam"},
			IntField{1},
		},
		nil,
	}
	outt2 := Tuple{
		TupleDesc{fields},
		[]DBValue{
			StringField{"george jones"},
			IntField{3},
		},
		nil,
	}
	ts := []*Tuple{&outt1, &outt2}
	match := CheckIfOutputMatches(iter, ts)
	if !match {
		t.Fail()
	}
}

func TestGbySumAgg(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	//gbyFields := hf.td.Fields[0:1]
	gbyFields := []Expr{&FieldExpr{hf.Descriptor().Fields[0]}}

	sa := SumAggState[int64]{}
	expr := FieldExpr{t1.Desc.Fields[1]}
	sa.Init("sum", &expr, intAggGetter)

	agg := NewGroupedAggregator([]AggState{&sa}, gbyFields, hf)
	iter, _ := agg.Iterator(tid)

	fields := []FieldType{
		{"name", "", StringType},
		{"sum", "", IntType},
	}
	outt1 := Tuple{TupleDesc{fields},
		[]DBValue{
			StringField{"sam"},
			IntField{50},
		}, nil,
	}
	outt2 := Tuple{
		TupleDesc{fields},
		[]DBValue{
			StringField{"george jones"},
			IntField{1998},
		}, nil,
	}
	ts := []*Tuple{&outt1, &outt2}
	match := CheckIfOutputMatches(iter, ts)
	if !match {
		t.Fail()
	}
}

func TestFilterCountAgg(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)

	var f FieldType = FieldType{"age", "", IntType}
	filt, err := NewIntFilter(&ConstExpr{IntField{25}, IntType}, OpGt, &FieldExpr{f}, hf)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if filt == nil {
		t.Fatalf("Filter returned nil")
	}

	sa := CountAggState{}
	expr := FieldExpr{t1.Desc.Fields[0]}
	sa.Init("count", &expr, nil)
	agg := NewAggregator([]AggState{&sa}, filt)
	iter, err := agg.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("Expected non-null tuple")
	}
	cnt := tup.Fields[0].(IntField).Value
	if cnt != 1 {
		t.Errorf("unexpected count")
	}
}

func TestRepeatedIteration(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	sa := CountAggState{}
	expr := FieldExpr{t1.Desc.Fields[0]}
	sa.Init("count", &expr, nil)
	agg := NewAggregator([]AggState{&sa}, hf)
	iter, err := agg.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("Expected non-null tuple")
	}
	cnt := tup.Fields[0].(IntField).Value
	if cnt != 2 {
		t.Errorf("unexpected count")
	}
	iter, err = agg.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	tup, err = iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("Expected non-null tuple")
	}
	cnt2 := tup.Fields[0].(IntField).Value
	if cnt != cnt2 {
		t.Errorf("count changed on repeated iteration")
	}

}
