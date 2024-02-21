package godb

// TODO: some code goes here
type InsertOp struct {
	insertFile DBFile
	child      Operator
}

// Construtor.  The insert operator insert the records in the child
// Operator into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	var res InsertOp
	res.insertFile = insertFile
	res.child = child
	return &res
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	var res TupleDesc
	res.Fields = append(res.Fields, FieldType{"count", "", IntType})
	return &res
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	desc := iop.Descriptor()
	count := 0
	childIter, err := iop.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	for {
		t, err := childIter()
		if err != nil {
			return nil, err
		}
		if t == nil {
			break
		}

		err = iop.insertFile.insertTuple(t, tid)
		if err != nil {
			return nil, err
		}
		count++
	}

	called := false
	return func() (*Tuple, error) {
		if called == true {
			return nil, nil
		}
		called = true
		var res Tuple
		res.Desc = *desc
		res.Fields = append(res.Fields, IntField{int64(count)})

		return &res, nil
	}, nil

}
