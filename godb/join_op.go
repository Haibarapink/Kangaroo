package godb

type EqualityJoin[T comparable] struct {
	// Expressions that when applied to tuples from the left or right operators,
	// respectively, return the value of the left or right side of the join
	leftField, rightField Expr

	left, right *Operator //operators for the two inputs of the join

	// Function that when applied to a DBValue returns the join value; will be
	// one of intFilterGetter or stringFilterGetter
	getter func(DBValue) T

	// The maximum number of records of intermediate state that the join should use
	// (only required for optional exercise)
	maxBufferSize int
}

// Constructor for a  join of integer expressions
// Returns an error if either the left or right expression is not an integer
func NewIntJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin[int64], error) {
	if leftField.GetExprType().Ftype != rightField.GetExprType().Ftype {
		return nil, GoDBError{TypeMismatchError, "can't join fields of different types"}
	}
	switch leftField.GetExprType().Ftype {
	case StringType:
		return nil, GoDBError{TypeMismatchError, "join field is not an int"}
	case IntType:
		return &EqualityJoin[int64]{leftField, rightField, &left, &right, intFilterGetter, maxBufferSize}, nil
	}
	return nil, GoDBError{TypeMismatchError, "unknown type"}
}

// Constructor for a  join of string expressions
// Returns an error if either the left or right expression is not a string
func NewStringJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin[string], error) {

	if leftField.GetExprType().Ftype != rightField.GetExprType().Ftype {
		return nil, GoDBError{TypeMismatchError, "can't join fields of different types"}
	}
	switch leftField.GetExprType().Ftype {
	case StringType:
		return &EqualityJoin[string]{leftField, rightField, &left, &right, stringFilterGetter, maxBufferSize}, nil
	case IntType:
		return nil, GoDBError{TypeMismatchError, "join field is not a string"}
	}
	return nil, GoDBError{TypeMismatchError, "unknown type"}
}

// Return a TupleDescriptor for this join. The returned descriptor should contain
// the union of the fields in the descriptors of the left and right operators.
// HINT: use the merge function you implemented for TupleDesc in lab1
func (hj *EqualityJoin[T]) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return nil
}

// Join operator implementation.  This function should iterate over the results
// of the join. The join should be the result of joining joinOp.left and
// joinOp.right, applying the joinOp.leftField and joinOp.rightField expressions
// to the tuples of the left and right iterators respectively, and joining them
// using an equality predicate.
// HINT: When implementing the simple nested loop join, you should keep in mind that
// you only iterate through the left iterator once (outer loop) but iterate through the right iterator
// once for every tuple in the the left iterator (inner loop).
// HINT: You can use joinTuples function you implemented in lab1 to join two tuples.
//
// OPTIONAL EXERCISE:  the operator implementation should not use more than
// maxBufferSize records, and should pass the testBigJoin test without timing
// out.  To pass this test, you will need to use something other than a nested
// loops join.
func (joinOp *EqualityJoin[T]) Iterator(tid TransactionID) (func() (*Tuple, error), error) {

	// TODO: some code goes here
	return nil, nil
}
