package godb

//This file defines methods for working with tuples, including defining
// the types DBType, FieldType, TupleDesc, DBValue, and Tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/mitchellh/hashstructure/v2"
)

// DBType is the type of a tuple field, in GoDB, e.g., IntType or StringType
type DBType int

const (
	IntType     DBType = iota
	StringType  DBType = iota
	UnknownType DBType = iota //used internally, during parsing, because sometimes the type is unknown
)

var typeNames map[DBType]string = map[DBType]string{IntType: "int", StringType: "string"}

// FieldType is the type of a field in a tuple, e.g., its name, table, and [godb.DBType].
// TableQualifier may or may not be an emtpy string, depending on whether the table
// was specified in the query
type FieldType struct {
	Fname          string
	TableQualifier string
	Ftype          DBType
}

// Pink's implementation of equals
func (f *FieldType) equals(f2 FieldType) bool {
	return f.Fname == f2.Fname && f.TableQualifier == f2.TableQualifier && f.Ftype == f2.Ftype
}

// TupleDesc is "type" of the tuple, e.g., the field names and types
type TupleDesc struct {
	Fields []FieldType
}

func (d *TupleDesc) Size() int {
	var size int = 0
	for _, f := range d.Fields {
		switch f.Ftype {
		case IntType:
			size += 8
		case StringType:
			size += StringLength
		}
	}
	return size
}

// Compare two tuple descs, and return true iff
// all of their field objects are equal and they
// are the same length
func (d1 *TupleDesc) equals(d2 *TupleDesc) bool {
	if len(d1.Fields) != len(d2.Fields) {
		return false
	}
	for i, f := range d1.Fields {
		if !f.equals(d2.Fields[i]) {
			return false
		}
	}
	return true

}

// Given a FieldType f and a TupleDesc desc, find the best
// matching field in desc for f.  A match is defined as
// having the same Ftype and the same name, preferring a match
// with the same TableQualifier if f has a TableQualifier
// We have provided this implementation because it's details are
// idiosyncratic to the behavior of the parser, which we are not
// asking you to write
func findFieldInTd(field FieldType, desc *TupleDesc) (int, error) {
	best := -1
	for i, f := range desc.Fields {
		if f.Fname == field.Fname && (f.Ftype == field.Ftype || field.Ftype == UnknownType) {
			if field.TableQualifier == "" && best != -1 {
				return 0, GoDBError{AmbiguousNameError, fmt.Sprintf("select name %s is ambiguous", f.Fname)}
			}
			if f.TableQualifier == field.TableQualifier || best == -1 {
				best = i
			}
		}
	}
	if best != -1 {
		return best, nil
	}
	return -1, GoDBError{IncompatibleTypesError, fmt.Sprintf("field %s.%s not found", field.TableQualifier, field.Fname)}

}

// Make a copy of a tuple desc.  Note that in go, assignment of a slice to
// another slice object does not make a copy of the contents of the slice.
// Look at the built-in function "copy".
func (td *TupleDesc) copy() *TupleDesc {
	var res TupleDesc = TupleDesc{}
	res.Fields = make([]FieldType, len(td.Fields))
	copy(res.Fields, td.Fields)
	return &res
}

// Assign the TableQualifier of every field in the TupleDesc to be the
// supplied alias.  We have provided this function as it is only used
// by the parser.
func (td *TupleDesc) setTableAlias(alias string) {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	for i := range fields {
		fields[i].TableQualifier = alias
	}
	td.Fields = fields
}

// Merge two TupleDescs together.  The resulting TupleDesc
// should consist of the fields of desc2
// appended onto the fields of desc.
func (desc *TupleDesc) merge(desc2 *TupleDesc) *TupleDesc {
	var res TupleDesc = TupleDesc{}
	res.Fields = make([]FieldType, len(desc.Fields)+len(desc2.Fields))
	copy(res.Fields, desc.Fields)
	copy(res.Fields[len(desc.Fields):], desc2.Fields)
	return &res
}

// ================== Tuple Methods ======================

// Interface used for tuple field values
// Since it implements no methods, any object can be used
// but having an interface for this improves code readability
// where tuple values are used
type DBValue interface {
}

// Integer field value
type IntField struct {
	Value int64
}

// String field value
type StringField struct {
	Value string
}

// Tuple represents the contents of a tuple read from a database
// It includes the tuple descriptor, and the value of the fields
type Tuple struct {
	Desc   TupleDesc
	Fields []DBValue
	Rid    recordID //used to track the page and position this page was read from
}

type recordID interface {
}

type Rid struct {
	PageNo int
	SlotNo int
}

// Serialize the contents of the tuple into a byte array Since all tuples are of
// fixed size, this method should simply write the fields in sequential order
// into the supplied buffer.
//
// See the function [binary.Write].  Objects should be serialized in little
// endian oder.
//
// Strings can be converted to byte arrays by casting to []byte. Note that all
// strings need to be padded to StringLength bytes (set in types.go). For
// example if StringLength is set to 5, the string 'mit' should be written as
// 'm', 'i', 't', 0, 0
//
// May return an error if the buffer has insufficient capacity to store the
// tuple.
func makeFixedBytes(s string, n int) []byte {
	var fixed = make([]byte, n)
	for i := 0; i < len(s); i++ {
		if i < len(s) {
			fixed[i] = s[i]
		} else {
			fixed[i] = 0
		}
	}
	return fixed
}

func (t *Tuple) writeTo(b *bytes.Buffer) error {
	var err error
	for i := 0; i < len(t.Fields); i++ {
		switch f := t.Fields[i].(type) {
		case IntField:
			err = binary.Write(b, binary.LittleEndian, f.Value)
		case StringField:
			fixed := makeFixedBytes(f.Value, StringLength)
			b.Write(fixed)
		}
	}
	return err
}

func (t *Tuple) Print() {
	for i := 0; i < len(t.Fields); i++ {
		switch f := t.Fields[i].(type) {
		case IntField:
			fmt.Printf(" %d ", f.Value)
		case StringField:
			fmt.Printf(" %s ", f.Value)
		}
	}
}

func (t *Tuple) ToString() string {
	var b bytes.Buffer
	for i := 0; i < len(t.Fields); i++ {
		switch f := t.Fields[i].(type) {
		case IntField:
			fmt.Fprintf(&b, " %d ", f.Value)
		case StringField:
			fmt.Fprintf(&b, " %s ", f.Value)
		}
	}
	return b.String()
}

// Read the contents of a tuple with the specified [TupleDesc] from the
// specified buffer, returning a Tuple.
//
// See [binary.Read]. Objects should be deserialized in little endian oder.
//
// All strings are stored as StringLength byte objects.
//
// Strings with length < StringLength will be padded with zeros, and these
// trailing zeros should be removed from the strings.  A []byte can be cast
// directly to string.
//
// May return an error if the buffer has insufficent data to deserialize the
// tuple.
func readString(b *bytes.Buffer, len int) (string, error) {
	var s string
	var data = make([]byte, len)
	err := binary.Read(b, binary.LittleEndian, &data)
	if err != nil {
		return "", err
	}
	var i int
	for i = 0; i < len; i++ {
		if data[i] == 0 {
			break
		}
	}
	s = string(data[0:i])
	return s, nil
}

func readTupleFrom(b *bytes.Buffer, desc *TupleDesc) (*Tuple, error) {
	var tp Tuple = Tuple{}
	tp.Desc = *desc.copy()
	for i := 0; i < len(desc.Fields); i++ {
		switch desc.Fields[i].Ftype {
		case IntType:
			var num int64
			err := binary.Read(b, binary.LittleEndian, &num)
			if err != nil {
				return nil, err
			}
			tp.Fields = append(tp.Fields, IntField{Value: num})
		case StringType:
			str, err := readString(b, StringLength)
			if err != nil {
				return nil, err
			}
			tp.Fields = append(tp.Fields, StringField{Value: str})
		}
	}
	return &tp, nil //replace me

}

// Compare two tuples for equality.  Equality means that the TupleDescs are equal
// and all of the fields are equal.  TupleDescs should be compared with
// the [TupleDesc.equals] method, but fields can be compared directly with equality
// operators.
func (t1 *Tuple) equals(t2 *Tuple) bool {
	if !t1.Desc.equals(&t2.Desc) {
		return false
	}
	for i := 0; i < len(t1.Fields); i++ {
		switch f1 := t1.Fields[i].(type) {
		case IntField:
			f2, ok := t2.Fields[i].(IntField)
			if !ok {
				return false
			}
			if f1.Value != f2.Value {
				return false
			}
		case StringField:
			f2, ok := t2.Fields[i].(StringField)
			if !ok {
				return false
			}
			if f1.Value != f2.Value {
				return false
			}
		}
	}
	return true
}

// Merge two tuples together, producing a new tuple with the fields of t2 appended to t1.
func joinTuples(t1 *Tuple, t2 *Tuple) *Tuple {
	if t1 == nil || t2 == nil {
		var t Tuple
		if t1 == nil {
			t = *t2
		} else {
			t = *t1
		}
		t.Rid = nil
		return &t
	}

	var res Tuple = Tuple{}
	res.Desc = *t1.Desc.merge(&t2.Desc)
	res.Fields = make([]DBValue, len(t1.Fields)+len(t2.Fields))
	copy(res.Fields, t1.Fields)
	copy(res.Fields[len(t1.Fields):], t2.Fields)
	return &res
}

type orderByState int

const (
	OrderedLessThan    orderByState = iota
	OrderedEqual       orderByState = iota
	OrderedGreaterThan orderByState = iota
)

// Apply the supplied expression to both t and t2, and compare the results,
// returning an orderByState value.
//
// Takes an arbitrary expressions rather than a field, because, e.g., for an
// ORDER BY SQL may ORDER BY arbitrary expressions, e.g., substr(name, 1, 2)
//
// Note that in most cases Expr will be a [godb.FieldExpr], which simply
// extracts a named field from a supplied tuple.
//
// Calling the [Expr.EvalExpr] method on a tuple will return the value of the
// expression on the supplied tuple.
func (t *Tuple) compareField(t2 *Tuple, field Expr) (orderByState, error) {
	v1, err := field.EvalExpr(t)
	if err != nil {
		return OrderedEqual, err
	}

	v2, err := field.EvalExpr(t2)
	if err != nil {
		return OrderedEqual, err
	}

	switch v1.(type) {
	case IntField:
		if v1.(IntField).Value < v2.(IntField).Value {
			return OrderedLessThan, nil
		}
		if v1.(IntField).Value > v2.(IntField).Value {
			return OrderedGreaterThan, nil
		}
		return OrderedEqual, nil
	case StringField:
		if v1.(StringField).Value < v2.(StringField).Value {
			return OrderedLessThan, nil
		}
		if v1.(StringField).Value > v2.(StringField).Value {
			return OrderedGreaterThan, nil
		}
		return OrderedEqual, nil
	default:
		return OrderedEqual, GoDBError{ParseError, "unknown type in compareField"}
	}
}

// Project out the supplied fields from the tuple. Should return a new Tuple
// with just the fields named in fields.
//
// Should not require a match on TableQualifier, but should prefer fields that
// do match on TableQualifier (e.g., a field  t1.name in fields should match an
// entry t2.name in t, but only if there is not an entry t1.name in t)
func (t *Tuple) project(fields []FieldType) (*Tuple, error) {
	var res Tuple = Tuple{}
	for _, f := range fields {
		fieldIndex, err := findFieldInTd(f, &t.Desc)
		if err != nil {
			return nil, err
		}
		res.Fields = append(res.Fields, t.Fields[fieldIndex])
	}
	return &res, nil //replace me
}

// Compute a key for the tuple to be used in a map structure
func (t *Tuple) tupleKey() any {
	//todo efficiency here is poor - hashstructure is probably slow
	hash, _ := hashstructure.Hash(t, hashstructure.FormatV2, nil)
	return hash
}

var winWidth int = 120

func fmtCol(v string, ncols int) string {
	colWid := winWidth / ncols
	nextLen := len(v) + 3
	remLen := colWid - nextLen
	if remLen > 0 {
		spacesRight := remLen / 2
		spacesLeft := remLen - spacesRight
		return strings.Repeat(" ", spacesLeft) + v + strings.Repeat(" ", spacesRight) + " |"
	} else {
		return " " + v[0:colWid-4] + " |"
	}
}

// Return a string representing the header of a table for a tuple with the
// supplied TupleDesc.
//
// Aligned indicates if the tuple should be foramtted in a tabular format
func (d *TupleDesc) HeaderString(aligned bool) string {
	outstr := ""
	for i, f := range d.Fields {
		tableName := ""
		if f.TableQualifier != "" {
			tableName = f.TableQualifier + "."
		}

		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(tableName+f.Fname, len(d.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, tableName+f.Fname)
		}
	}
	return outstr
}

// Return a string representing the tuple
// Aligned indicates if the tuple should be formatted in a tabular format
func (t *Tuple) PrettyPrintString(aligned bool) string {
	outstr := ""
	for i, f := range t.Fields {
		str := ""
		switch f := f.(type) {
		case IntField:
			str = fmt.Sprintf("%d", f.Value)
		case StringField:
			str = f.Value
		}
		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(str, len(t.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, str)
		}
	}
	return outstr

}
