package godb

import "sort"

type OrderBy struct {
	orderBy []Expr // OrderBy should include these two fields (used by parser)
	child   Operator
	//add additional fields here
	ascending []bool
}

// Order by constructor -- should save the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extacted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields
// list should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	var ob OrderBy
	ob.child = child
	ob.orderBy = orderByFields
	ob.ascending = ascending
	return &ob, nil
}

func (o *OrderBy) Descriptor() *TupleDesc {
	return o.child.Descriptor()
}

// Return a function that iterators through the results of the child iterator in
// ascending/descending order, as specified in the construtor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort pacakge and the [sort.Sort] method for this purpose.  To
// use this you will need to implement three methods:  Len, Swap, and Less that
// the sort algorithm will invoke to preduce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at: https://pkg.go.dev/sort

type TupleSorted struct {
	Tup           *Tuple
	Asc           *[]bool
	CmpFieldsExpr *[]Expr
}

type TupleSortedSlice []TupleSorted

func (t TupleSortedSlice) Len() int {
	return len(t)
}

func (t TupleSortedSlice) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t TupleSortedSlice) Less(a, b int) bool {
	tupI := t[a].Tup
	tupJ := t[b].Tup
	asc := t[a].Asc
	cmpFieldsExpr := t[a].CmpFieldsExpr
	for i, cmpExpr := range *cmpFieldsExpr {
		left, _ := cmpExpr.EvalExpr(tupI)
		right, _ := cmpExpr.EvalExpr(tupJ)
		var leftGreater bool
		var equal bool
		switch curType := left.(type) {
		case IntField:
			leftGreater = evalPred(curType.Value, right.(IntField).Value, OpGt)
			equal = evalPred(curType.Value, right.(IntField).Value, OpEq)
		case StringField:
			leftGreater = evalPred(curType.Value, right.(StringField).Value, OpGt)
			equal = evalPred(curType.Value, right.(StringField).Value, OpEq)
		}
		if equal {
			continue
		}
		if leftGreater {
			return !(*asc)[i]
		} else if !leftGreater {
			return (*asc)[i]
		}
	}
	// if we get here, then the tuples are equal
	return false
}

func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iter, err := o.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	var sortedTuples TupleSortedSlice
	for {
		tup, err := iter()
		if err != nil {
			return nil, err
		}
		if tup == nil {
			break
		}
		sortedTuples = append(sortedTuples, TupleSorted{tup, &o.ascending, &o.orderBy})
	}

	sort.Sort(sortedTuples)
	index := 0
	return func() (*Tuple, error) {
		if index >= len(sortedTuples) {
			return nil, nil
		}
		tup := sortedTuples[index].Tup
		index++
		return tup, nil
	}, nil
}
