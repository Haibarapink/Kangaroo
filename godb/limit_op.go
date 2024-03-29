package godb

type LimitOp struct {
	child     Operator //required fields for parser
	limitTups Expr
	//add additional fields here, if needed
}

// Limit constructor -- should save how many tuples to return and the child op.
// lim is how many tuples to return and child is the child op.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	var l LimitOp
	l.child = child
	l.limitTups = lim
	return &l
}

// Return a TupleDescriptor for this limit
func (l *LimitOp) Descriptor() *TupleDesc {
	return l.child.Descriptor()
}

// Limit operator implementation. This function should iterate over the
// results of the child iterator, and limit the result set to the first
// [lim] tuples it sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	cnt := 0
	lim := l.limitTups.(*ConstExpr).val.(IntField).Value
	iter, err := l.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	return func() (*Tuple, error) {
		if int64(cnt) == lim {
			return nil, nil
		}
		cnt++
		return iter()
	}, nil
}
