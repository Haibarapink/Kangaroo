package godb

type DeleteOp struct {
	// TODO: some code goes here
	deleteFile DBFile
	child      Operator
}

// Construtor.  The delete operator deletes the records in the child
// Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	var res DeleteOp
	res.deleteFile = deleteFile
	res.child = child
	return &res
}

// The delete TupleDesc is a one column descriptor with an integer field named "count"
func (i *DeleteOp) Descriptor() *TupleDesc {
	var f FieldType = FieldType{"count", "", IntType}
	var desc TupleDesc = TupleDesc{[]FieldType{f}}
	return &desc
}

// Return an iterator function that deletes all of the tuples from the child
// iterator from the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were deleted.  Tuples should be deleted using the [DBFile.deleteTuple]
// method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	deletedCount := 0
	iter, err := dop.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	for {
		tup, err := iter()
		if err != nil {
			return nil, err
		}
		if tup == nil {
			break
		}
		err = dop.deleteFile.deleteTuple(tup, tid)
		if err != nil {
			return nil, err
		}
		deletedCount++
	}

	return func() (*Tuple, error) {
		desc := dop.Descriptor()
		var t Tuple = Tuple{*desc, []DBValue{IntField{int64(deletedCount)}}, nil}
		return &t, nil
	}, nil

}
