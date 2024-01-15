package godb

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func CheckIfOutputMatches(f func() (*Tuple, error), ts []*Tuple) bool {
	n := 0
	for {
		t1, _ := f()
		if t1 == nil {
			break
		}
		//		fmt.Printf("%v\n", t1)
		got := false
		for _, t2 := range ts {
			if t1.equals(t2) {
				got = true
				break
			}
		}
		if !got {
			return false
		}
		n++
	}
	if n == len(ts) {
		return true
	} else {
		return false
	}
}
func TestSimple(t *testing.T) {
	var b bytes.Buffer
	binary.Write(&b, binary.LittleEndian, int32(1))
	binary.Write(&b, binary.LittleEndian, int32(2))
	// read
	var i int32
	binary.Read(&b, binary.LittleEndian, &i)
	if i != 1 {
		t.Errorf("Expected 1, got %d", i)
	}
	binary.Read(&b, binary.LittleEndian, &i)
	if i != 2 {
		t.Errorf("Expected 2, got %d", i)
	}
	// string
	b.WriteString("hello")
	// rea
	// read bytes first
	var n = make([]byte, 5)

	var s string
	binary.Read(&b, binary.LittleEndian, &n)
	s = string(n)
	if s != "hello" {
		t.Errorf("Expected hello, got %s", s)
	}

}

// Unit test for Tuple.writeTo() and Tuple.readTupleFrom()
func TestTupleSerialization(t *testing.T) {
	td, t1, _, _, _, _ := makeTestVars()
	b := new(bytes.Buffer)
	t1.writeTo(b)
	t3, err := readTupleFrom(b, &td)

	if err != nil {
		t.Fatalf("Error loading tuple from saved buffer.")
	}
	if !t3.equals(&t1) {
		t.Errorf("Serialization / deserialization doesn't result in identical tuple.")
	}

}

// Unit test for Tuple.compareField()
func TestTupleExpr(t *testing.T) {
	td, t1, t2, _, _, _ := makeTestVars()
	ft := td.Fields[0]
	f := FieldExpr{ft}
	result, err := t1.compareField(&t2, &f) // compare "sam" to "george jones"
	if err != nil {
		t.Fatalf(err.Error())
	}
	if result != OrderedGreaterThan {
		t.Errorf("comparison of fields did not return expected result")
	}
}

// Unit test for Tuple.project()
func TestTupleProject(t *testing.T) {
	_, t1, _, _, _, _ := makeTestVars()
	tNew, err := t1.project([]FieldType{t1.Desc.Fields[0]})
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tNew == nil {
		t.Fatalf("new tuple was nil")
	}
	if len(tNew.Fields) != 1 {
		t.Fatalf("unexpected number of fields after project")
	}
	f, ok := tNew.Fields[0].(StringField)
	if !ok || f.Value != "sam" {
		t.Errorf("unexpected value after project")
	}
}

// Unit test for Tuple.joinTuples()
func TestTupleJoin(t *testing.T) {
	_, t1, t2, _, _, _ := makeTestVars()
	tNew := joinTuples(&t1, &t2)
	if len(tNew.Fields) != 4 {
		t.Fatalf("unexpected number of fields after join")
	}
	f, ok := tNew.Fields[0].(StringField)
	if !ok || f.Value != "sam" {
		t.Fatalf("unexpected value after join")
	}
	f, ok = tNew.Fields[2].(StringField)
	if !ok || f.Value != "george jones" {
		t.Errorf("unexpected value after join")
	}

}

func TDAssertEquals(t *testing.T, expected, actual TupleDesc) {
	if !(expected.equals(&actual)) {
		t.Errorf("Expected EQUAL, found NOT EQUAL")
	}
}

func TDAssertNotEquals(t *testing.T, expected, actual TupleDesc) {
	if expected.equals(&actual) {
		t.Errorf("Expected EQUAL, found NOT EQUAL")
	}
}

func TAssertEquals(t *testing.T, expected, actual Tuple) {
	if !(expected.equals(&actual)) {
		t.Errorf("Expected EQUAL, found NOT EQUAL")
	}
}

func TAssertNotEquals(t *testing.T, expected, actual Tuple) {
	if expected.equals(&actual) {
		t.Errorf("Expected EQUAL, found NOT EQUAL")
	}
}

func TestTupleDescEquals(t *testing.T) {
	singleInt := TupleDesc{Fields: []FieldType{{Ftype: IntType}}}
	singleInt2 := TupleDesc{Fields: []FieldType{{Ftype: IntType}}}
	intString := TupleDesc{Fields: []FieldType{{Ftype: IntType}, {Ftype: StringType}}}
	intString2 := TupleDesc{Fields: []FieldType{{Ftype: IntType}, {Ftype: StringType}}}

	TDAssertEquals(t, singleInt, singleInt)
	TDAssertEquals(t, singleInt, singleInt2)
	TDAssertEquals(t, singleInt2, singleInt)
	TDAssertEquals(t, intString, intString)

	TDAssertNotEquals(t, singleInt, intString)
	TDAssertNotEquals(t, singleInt2, intString)
	TDAssertNotEquals(t, intString, singleInt)
	TDAssertNotEquals(t, intString, singleInt2)
	TDAssertEquals(t, intString, intString2)
	TDAssertEquals(t, intString2, intString)

	stringInt := TupleDesc{Fields: []FieldType{{Ftype: StringType}, {Ftype: IntType}}}
	_, t1, _, _, _, _ := makeTestVars()
	TDAssertNotEquals(t, t1.Desc, stringInt) // diff in only Fname
}

// Unit test for TupleDesc.copy()
func TestTupleDescCopy(t *testing.T) {
	singleInt := TupleDesc{Fields: []FieldType{{Ftype: IntType}}}
	intString := TupleDesc{Fields: []FieldType{{Ftype: IntType}, {Ftype: StringType}}}

	TDAssertEquals(t, singleInt, *singleInt.copy())
	TDAssertEquals(t, intString, *intString.copy())
	TDAssertEquals(t, *intString.copy(), *intString.copy())
	TDAssertNotEquals(t, *intString.copy(), *singleInt.copy())

	// tests deep copy
	tdCpy := intString.copy()
	tdCpy2 := tdCpy.copy()
	if tdCpy == nil || len(tdCpy.Fields) == 0 {
		t.Fatalf("tdCpy is nil or fields are empty")
	}
	if tdCpy2 == nil || len(tdCpy2.Fields) == 0 {
		t.Fatalf("tdCpy2 is nil or fields are empty")
	}
	tdCpy.Fields[0] = intString.Fields[1]
	TDAssertNotEquals(t, *tdCpy, *tdCpy2)
	tdCpy.Fields[0] = intString.Fields[0]
	TDAssertEquals(t, *tdCpy, *tdCpy2)
}

// Unit test for TupleDesc.merge()
func TestTupleDescMerge(t *testing.T) {
	singleInt := TupleDesc{Fields: []FieldType{{Ftype: IntType}}}
	stringInt := TupleDesc{Fields: []FieldType{{Ftype: StringType}, {Ftype: IntType}}}
	td1, td2 := stringInt, stringInt.copy()

	tdNew := td1.merge(&singleInt).merge(td2)
	final := TupleDesc{Fields: []FieldType{{Ftype: StringType}, {Ftype: IntType}, {Ftype: IntType}, {Ftype: StringType}, {Ftype: IntType}}}

	TDAssertEquals(t, final, *tdNew)
	TDAssertNotEquals(t, td1, *tdNew)
}

// Unit test for Tuple.equals()
func TestTupleEquals(t *testing.T) {
	_, t1, t2, _, _, _ := makeTestVars()
	_, t1Dup, _, _, _, _ := makeTestVars()

	var stringTup = Tuple{
		Desc: TupleDesc{Fields: []FieldType{{Ftype: StringType}}},
		Fields: []DBValue{
			StringField{"sam"},
		},
	}

	TAssertEquals(t, t1, t1)
	TAssertEquals(t, t1, t1Dup)

	TAssertNotEquals(t, t1, t2)
	TAssertNotEquals(t, t1, stringTup)
	TAssertNotEquals(t, stringTup, t2)
}
