package godb

import "golang.org/x/exp/constraints"

type Number interface {
	constraints.Integer | constraints.Float
}

// interface for an aggregation state
type AggState interface {

	// Initializes an aggregation state. Is supplied with an alias,
	// an expr to evaluate an input tuple into a DBValue, and a getter
	// to extract from the DBValue its int or string field's value.
	Init(alias string, expr Expr, getter func(DBValue) any) error

	// Makes an copy of the aggregation state.
	Copy() AggState

	// Adds an tuple to the aggregation state.
	AddTuple(*Tuple)

	// Returns the final result of the aggregation as a tuple.
	Finalize() *Tuple

	// Gets the tuple description of the tuple that Finalize() returns.
	GetTupleDesc() *TupleDesc
}

// Implements the aggregation state for COUNT
type CountAggState struct {
	alias string
	expr  Expr
	count int
}

func (a *CountAggState) Copy() AggState {
	return &CountAggState{a.alias, a.expr, a.count}
}

func (a *CountAggState) Init(alias string, expr Expr, getter func(DBValue) any) error {
	a.count = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *CountAggState) AddTuple(t *Tuple) {
	a.count++
}

func (a *CountAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.count)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

func (a *CountAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

// Implements the aggregation state for SUM
type SumAggState[T Number] struct {
	// TODO: some code goes here
	// TODO add fields that can help implement the aggregation state
	CountInt   int64
	CountFloat float64

	alias  string
	expr   Expr
	getter func(DBValue) any
}

func (a *SumAggState[T]) Copy() AggState {
	newAggState := &SumAggState[T]{}
	newAggState.CountInt = a.CountInt
	newAggState.CountFloat = a.CountFloat
	newAggState.alias = a.alias
	newAggState.expr = a.expr
	newAggState.getter = a.getter

	return newAggState
}

func intAggGetter(v DBValue) any {
	return v.(IntField).Value
}

func stringAggGetter(v DBValue) any {
	return v.(StringField).Value
}

func (a *SumAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any) error {
	a.CountInt = 0
	a.CountFloat = 0
	a.expr = expr
	a.alias = alias
	a.getter = getter
	return nil
}

func (a *SumAggState[T]) AddTuple(t *Tuple) {
	if t == nil {
		panic("t is nil")
	}
	v, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}
	val := a.getter(v)
	switch typeOfVal := val.(type) {
	case int64:
		a.CountInt += typeOfVal
	case float64:
		a.CountFloat += typeOfVal
	default:
		panic("invalid type")
	}
}

func (a *SumAggState[T]) GetTupleDesc() *TupleDesc {
	var ft FieldType = FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *SumAggState[T]) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{a.CountInt}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for AVG
// Note that we always AddTuple() at least once before Finalize()
// so no worries for divide-by-zero
type AvgAggState[T Number] struct {
	// TODO: some code goes here
	// TODO add fields that can help implement the aggregation state
	count     int64
	sumInt    float64
	avgResInt int64

	sumFloat    int64
	avgResFloat float64

	alias  string
	expr   Expr
	getter func(DBValue) any
}

func (a *AvgAggState[T]) Copy() AggState {
	copy := AvgAggState[T]{}
	copy.count = a.count
	copy.sumInt = a.sumInt
	copy.sumFloat = a.sumFloat

	copy.alias = a.alias
	copy.expr = a.expr
	copy.getter = a.getter

	return &copy
}

func (a *AvgAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any) error {
	a.count = 0
	a.sumInt = 0
	a.sumFloat = 0
	a.expr = expr
	a.alias = alias
	a.getter = getter
	return nil
}

func (a *AvgAggState[T]) AddTuple(t *Tuple) {
	v, err := a.expr.EvalExpr(t)
	if err != nil {
		panic(err)
	}
	val := a.getter(v)
	switch typeOfVal := val.(type) {
	case int64:
		a.sumInt += float64(typeOfVal)
	case float64:
		a.sumFloat += int64(typeOfVal)
	default:
		panic("invalid type")
	}
	a.count++
}

func (a *AvgAggState[T]) GetTupleDesc() *TupleDesc {
	var ft FieldType = FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *AvgAggState[T]) Finalize() *Tuple {
	td := a.GetTupleDesc()
	a.avgResInt = int64(a.sumInt / float64(a.count))
	var f IntField = IntField{a.avgResInt}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for MAX
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN max
type MaxAggState[T constraints.Ordered] struct {
	alias  string
	expr   Expr
	max    T
	null   bool // whether the agg state have not seen any tuple inputted yet
	getter func(DBValue) any
}

func (a *MaxAggState[T]) Copy() AggState {
	return &MaxAggState[T]{a.alias, a.expr, a.max, true, a.getter}
}

func (a *MaxAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any) error {
	a.expr = expr
	a.getter = getter
	a.alias = alias
	return nil
}

func (a *MaxAggState[T]) AddTuple(t *Tuple) {
	v, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}
	val := a.getter(v).(T)
	if a.null {
		a.max = val
		a.null = false
	} else if val > a.max {
		a.max = val
	}
}

func (a *MaxAggState[T]) GetTupleDesc() *TupleDesc {
	var ft FieldType
	switch any(a.max).(type) {
	case string:
		ft = FieldType{a.alias, "", StringType}
	default:
		ft = FieldType{a.alias, "", IntType}
	}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *MaxAggState[T]) Finalize() *Tuple {
	td := a.GetTupleDesc()
	var f any
	switch any(a.max).(type) {
	case string:
		f = StringField{any(a.max).(string)}
	default:
		f = IntField{any(a.max).(int64)}
	}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for MIN
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN min
type MinAggState[T constraints.Ordered] struct {
	alias  string
	expr   Expr
	min    T
	null   bool // whether the agg state have not seen any tuple inputted yet
	getter func(DBValue) any
}

func (a *MinAggState[T]) Copy() AggState {
	return &MinAggState[T]{a.alias, a.expr, a.min, true, a.getter}
}

func (a *MinAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any) error {
	a.expr = expr
	a.getter = getter
	a.alias = alias
	a.null = true
	return nil
}

func (a *MinAggState[T]) AddTuple(t *Tuple) {
	v, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}
	val := a.getter(v).(T)
	if a.null {
		a.min = val
		a.null = false
	} else if val < a.min {
		a.min = val
	}
}

func (a *MinAggState[T]) GetTupleDesc() *TupleDesc {
	var ft FieldType
	switch any(a.min).(type) {
	case string:
		ft = FieldType{a.alias, "", StringType}
	default:
		ft = FieldType{a.alias, "", IntType}
	}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *MinAggState[T]) Finalize() *Tuple {
	td := a.GetTupleDesc()
	var f any
	switch any(a.min).(type) {
	case string:
		f = StringField{any(a.min).(string)}
	default:
		f = IntField{any(a.min).(int64)}
	}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}
