package godb

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator

	distinct bool

	// distinct tuples seen so farm, and key is from tuple.key()
	seenTuples map[any]bool
}

// Project constructor -- should save the list of selected field, child, and the child op.
// Here, selectFields is a list of expressions that represents the fields to be selected,
// outputNames are names by which the selected fields are named (should be same length as
// selectFields; throws error if not), distinct is for noting whether the projection reports
// only distinct results, and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	var p Project
	p.child = child
	p.selectFields = selectFields
	p.outputNames = outputNames
	p.distinct = distinct
	p.seenTuples = make(map[any]bool)
	return &p, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should contain
// fields for each field in the constructor selectFields list with outputNames
// as specified in the constructor.
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	var desc TupleDesc
	for i := 0; i < len(p.selectFields); i++ {
		if len(p.outputNames) > i {
			var field FieldType
			field.Fname = p.outputNames[i]
			field.Ftype = p.selectFields[i].GetExprType().Ftype
			field.TableQualifier = p.selectFields[i].GetExprType().TableQualifier
			desc.Fields = append(desc.Fields, field)
		} else {
			desc.Fields = append(desc.Fields, p.selectFields[i].GetExprType())
		}
	}
	return &desc
}

// Project operator implementation.  This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed.
// To implement this you will need to record in some data structure with the
// distinct tuples seen so far.  Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	childIter, err := p.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	return func() (*Tuple, error) {
		for {
			tp, err := childIter()
			if err != nil {
				return nil, err
			}
			if tp == nil {
				return nil, nil
			}

			var newTuple Tuple
			for i := 0; i < len(p.selectFields); i++ {
				val, err := p.selectFields[i].EvalExpr(tp)
				if err != nil {
					return nil, err
				}
				newTuple.Fields = append(newTuple.Fields, val)
			}

			if p.distinct {
				key := newTuple.tupleKey()
				if p.seenTuples[key] {
					continue
				}
				p.seenTuples[key] = true
			}

			newTuple.Desc = *p.Descriptor()
			return &newTuple, nil
		}
	}, nil
}
