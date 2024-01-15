package godb

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	"github.com/xwb1989/sqlparser"
)

type LogicalFilterNode struct {
	fieldExpr LogicalSelectNode
	constExpr LogicalSelectNode
	predOp    BoolOp
}

type LogicalJoinNode struct {
	left, right *LogicalSelectNode
	predOp      BoolOp
}

type SelectExprType int

const (
	ExprField SelectExprType = iota
	ExprConst SelectExprType = iota
	ExprFunc  SelectExprType = iota
	ExprStar  SelectExprType = iota
	ExprAggr  SelectExprType = iota
)

type LogicalSelectNode struct {
	exprType    SelectExprType
	table       string
	field       string
	funcOp      *string //may be nil, if no aggregate
	alias       string
	value       string
	args        []*LogicalSelectNode //for functions other than aggregates
	cachedField *FieldType
}

func NewFieldSelectNode(table string, field string, alias string) LogicalSelectNode {
	lsn := LogicalSelectNode{}
	lsn.exprType = ExprField
	lsn.table = table
	lsn.field = field
	lsn.alias = alias
	return lsn
}
func NewConstSelectNode(value string, alias string) LogicalSelectNode {
	lsn := LogicalSelectNode{}
	lsn.exprType = ExprConst
	lsn.value = value
	lsn.alias = alias
	return lsn
}
func NewStarSelectNode(table string) LogicalSelectNode {
	lsn := LogicalSelectNode{}
	lsn.exprType = ExprStar
	lsn.table = table
	lsn.field = "*"
	return lsn
}
func NewAggrSelectNode(op string, arg *LogicalSelectNode, alias string) LogicalSelectNode {
	lsn := LogicalSelectNode{}
	lsn.exprType = ExprAggr
	lsn.args = []*LogicalSelectNode{arg}
	//lsn.field = field
	lsn.funcOp = &op
	lsn.alias = alias
	return lsn
}

func NewFuncSelectNode(op string, args []*LogicalSelectNode, alias string) LogicalSelectNode {
	lsn := LogicalSelectNode{}
	lsn.exprType = ExprFunc
	lsn.funcOp = &op
	lsn.alias = alias
	lsn.args = args
	return lsn
}

func checkNameInTablesOrSubqueries(table string, field string, c *Catalog, subqueries []*LogicalPlan, ts []*LogicalTableNode) (string, error) {
	if table == "" && subqueries != nil {
		for _, q := range subqueries {
			qFs := q.getSubplanFields(c)
			for _, testField := range qFs {
				if testField.Fname == field {
					if table != "" {
						return "", GoDBError{AmbiguousNameError, fmt.Sprintf("multiple possible table names for field %s in select expression", field)}
					}
					table = testField.TableQualifier
				}
			}
		}
	}
	if table == "" && c != nil && ts != nil {
		catTs := c.findTablesWithColumn(field)
		for _, t := range catTs {
			for _, t2 := range ts {
				if t.name == t2.tableName {
					if table != "" {
						return "", GoDBError{AmbiguousNameError, fmt.Sprintf("multiple possible table names for field %s in select expression", field)}
					}
					table = t.name
				}
			}
		}
	}
	return table, nil
}

// need to figure out which table & field this expression references, if any
// if catalog is non null, will try to resolve table name from catalog
// otherwise, will not
func (lsn *LogicalSelectNode) getTableField(c *Catalog, subqueries []*LogicalPlan, ts []*LogicalTableNode) (string, string, error) {
	if lsn.exprType == ExprConst {
		return "", "", nil
	}
	if lsn.exprType == ExprFunc || lsn.exprType == ExprAggr {
		tabName := ""
		fieldName := ""
		for _, subLsn := range lsn.args {
			newTabName, newFieldName, err := subLsn.getTableField(c, subqueries, ts)
			if err != nil {
				return "", "", err
			}
			if tabName == "" && (newTabName != "" || newFieldName != "") {
				tabName = newTabName
				fieldName = newFieldName
				break
			} else if fieldName == "" && tabName == "" {
				fieldName = newFieldName
			} else if newTabName != "" {
				return "", "", GoDBError{AmbiguousNameError, fmt.Sprintf("multiple possible table names for field %s in select expression", fieldName)}
			}
		}
		tabName, err := checkNameInTablesOrSubqueries(tabName, fieldName, c, subqueries, ts)
		if err != nil {
			return "", "", err
		}
		return tabName, fieldName, nil
	}
	tabName := lsn.table
	tabName, err := checkNameInTablesOrSubqueries(tabName, lsn.field, c, subqueries, ts)
	if err != nil {
		return "", "", err
	}
	field := lsn.field
	if lsn.alias != "" {
		field = lsn.alias
	}
	return tabName, field, nil
}

type LogicalTableNode struct {
	tableName string
	alias     string
	file      *DBFile
}

type GroupBy struct {
	expr *LogicalSelectNode
}

type OrderByNode struct {
	expr      *LogicalSelectNode
	ascending bool
}

type LogicalPlan struct {
	filters       []*LogicalFilterNode
	joins         []*LogicalJoinNode
	selects       []*LogicalSelectNode
	aggs          []*LogicalSelectNode
	tables        []*LogicalTableNode
	subqueries    []*LogicalPlan
	groupByFields []*GroupBy
	orderByFields []*OrderByNode
	limit         *LogicalSelectNode
	distinct      bool
	alias         string
}

func (p *LogicalPlan) getSubplanFields(c *Catalog) []*FieldType {
	var nodes []*FieldType
	for _, s := range p.selects {
		_, field, _ := s.getTableField(c, p.subqueries, p.tables)
		nodes = append(nodes, &FieldType{field, p.alias, UnknownType})
	}
	return nodes
}

func parseWhere(c *Catalog, subqueries []*LogicalPlan, ts []*LogicalTableNode, expr sqlparser.Expr) ([]*LogicalFilterNode, []*LogicalJoinNode, error) {
	switch expr := expr.(type) {
	case *sqlparser.AndExpr:
		//print("got and")
		filterListLeft, joinListLeft, _ := parseWhere(c, subqueries, ts, expr.Left)
		filterListRight, joinListRight, _ := parseWhere(c, subqueries, ts, expr.Right)
		filterExprs := append(filterListLeft, filterListRight...)
		joinExprs := append(joinListLeft, joinListRight...)
		return filterExprs, joinExprs, nil
	case *sqlparser.ComparisonExpr:
		op := BoolOpMap[expr.Operator]
		//print(op)
		//print("got compare")

		left, err := parseExpr(c, expr.Left, "")
		if err != nil {
			return nil, nil, err
		}
		right, err := parseExpr(c, expr.Right, "")
		if err != nil {
			return nil, nil, err
		}
		//here we want to search the catalog for the table id, if it's not specified
		lTable, _, err := left.getTableField(c, subqueries, ts)
		if err != nil {
			return nil, nil, err
		}
		rTable, _, err := right.getTableField(c, subqueries, ts)
		if err != nil {
			return nil, nil, err
		}
		if lTable != "" && rTable != "" && lTable != rTable { //join

			if op != OpEq {
				return nil, nil, GoDBError{IllegalOperationError, "only equality joins are supported"}
			}
			join := LogicalJoinNode{left, right, op}
			lj := make([]*LogicalJoinNode, 1)
			lj[0] = &join
			return nil, lj, nil
		} else {
			filter := LogicalFilterNode{*left, *right, op}
			lf := make([]*LogicalFilterNode, 1)
			lf[0] = &filter
			return lf, nil, nil
		}
	default:
		return nil, nil, GoDBError{ParseError, "where expression with non value or column on RHS (disjunctions and nested where expressions are not supported)"}
	}
}

func parseFrom(c *Catalog, t sqlparser.TableExpr) ([]*LogicalTableNode, []*LogicalPlan, []*LogicalJoinNode, error) {
	switch tableEx := t.(type) {
	case *sqlparser.AliasedTableExpr:
		switch tableEx.Expr.(type) {
		case *sqlparser.Subquery:
			sq := (tableEx.Expr).(*sqlparser.Subquery)
			//print("got subquery")
			switch stmt := sq.Select.(type) {
			case *sqlparser.Select:
				subplan, err := parseStatement(c, stmt)
				if err != nil {
					return nil, nil, nil, err
				}
				subplan.alias = strings.ToLower(sqlparser.String(tableEx.As))
				subplans := make([]*LogicalPlan, 1)
				subplans[0] = subplan
				return nil, subplans, nil, nil
			}
		case sqlparser.SimpleTableExpr:
			tableName := strings.ToLower(sqlparser.GetTableName(tableEx.Expr).CompliantName())
			//fmt.Printf("got simple table, name %s\n", tableName)
			dbFile, err := c.GetTable(tableName)
			if err != nil {
				return nil, nil, nil, err
			}
			table := LogicalTableNode{tableName,
				strings.ToLower(sqlparser.String(tableEx.As)),
				&dbFile}
			table.alias = strings.ToLower(sqlparser.String(tableEx.As))
			tables := make([]*LogicalTableNode, 1)
			tables[0] = &table
			return tables, nil, nil, nil
		}
	case *sqlparser.ParenTableExpr:
		var (
			tables   []*LogicalTableNode
			subplans []*LogicalPlan
			joins    []*LogicalJoinNode
		)
		for _, e := range tableEx.Exprs {
			newTables, newSubplans, newJoins, err := parseFrom(c, e)
			if err != nil {
				return nil, nil, nil, err
			}
			tables = append(tables, newTables...)
			subplans = append(subplans, newSubplans...)
			joins = append(joins, newJoins...)
		}
		return tables, subplans, joins, nil
	case *sqlparser.JoinTableExpr:
		joinTable, _ := t.(*sqlparser.JoinTableExpr)
		leftTables, leftSubplans, leftJoins, err := parseFrom(c, joinTable.LeftExpr)
		if err != nil {
			return nil, nil, nil, err
		}
		rightTables, rightSubplans, rightJoins, err := parseFrom(c, joinTable.RightExpr)
		if err != nil {
			return nil, nil, nil, err
		}
		if joinTable.Join != "join" {
			return nil, nil, nil, GoDBError{ParseError, fmt.Sprintf("unsupported join type %s", joinTable.Join)}
		}
		tabList := append(leftTables, rightTables...)
		subPlanList := append(leftSubplans, rightSubplans...)
		_, joins, err := parseWhere(c, subPlanList, tabList, joinTable.Condition.On)
		if err != nil {
			return nil, nil, nil, err
		}
		return tabList, subPlanList, append(leftJoins, append(rightJoins, joins...)...), nil

	}
	return nil, nil, nil, GoDBError{ParseError, "unknown query type in parseFrom"}
}

func isAgg(funcName string) bool {
	aggs := []string{"count", "sum", "avg", "min", "max"}
	for _, s := range aggs {
		if s == funcName {
			return true
		}
	}
	return false
}

func parseExpr(c *Catalog, expr sqlparser.Expr, alias string) (*LogicalSelectNode, error) {
	switch expr := expr.(type) {
	case *sqlparser.FuncExpr:
		funName := strings.ToLower(sqlparser.String(expr.Name))
		if isAgg(funName) {
			if len(expr.Exprs) != 1 {
				return nil, GoDBError{ParseError, fmt.Sprintf("expected one argument to aggregate %s in select list", sqlparser.String(expr.Name))}
			}
			star, ok := expr.Exprs[0].(*sqlparser.StarExpr)
			if ok {
				if funName != "count" {
					return nil, GoDBError{ParseError, "got * in non-count aggregate"}
				}
				subField := NewFieldSelectNode(strings.ToLower(sqlparser.String(star.TableName)), "*", "")
				field := NewAggrSelectNode(funName, &subField, alias)
				return &field, nil
			}
			field, err := parseSelect(c, expr.Exprs[0])
			if err != nil {
				return nil, err
			}
			outer := NewAggrSelectNode(funName, field, alias)
			return &outer, nil
		} else {
			funName := strings.ToLower(sqlparser.String(expr.Name))
			exprList := make([]*LogicalSelectNode, len(expr.Exprs))
			for i, subExpr := range expr.Exprs {
				e, err := parseSelect(c, subExpr)
				if err != nil {
					return nil, err
				}
				exprList[i] = e
			}
			if funName[0] == '\'' || funName[0] == '`' {
				funName = funName[1 : len(funName)-1]
			}
			outer := NewFuncSelectNode(funName, exprList, alias)
			return &outer, nil
		}
	case *sqlparser.BinaryExpr:
		opname := expr.Operator
		left, err := parseExpr(c, expr.Left, "")
		if err != nil {
			return nil, err
		}
		right, err := parseExpr(c, expr.Right, "")
		if err != nil {
			return nil, err
		}
		exprList := make([]*LogicalSelectNode, 2)
		exprList[0] = left
		exprList[1] = right
		outer := NewFuncSelectNode(opname, exprList, alias)
		return &outer, nil
	case *sqlparser.ParenExpr:
		return parseExpr(c, expr.Expr, alias)
	case *sqlparser.ColName:
		field := NewFieldSelectNode(strings.ToLower(sqlparser.String(expr.Qualifier)), strings.ToLower(sqlparser.String(expr.Name)), alias)
		if len(field.table) > 1 && (field.table[0] == '\'' || field.table[0] == '`') {
			field.table = field.table[1 : len(field.table)-1]
		}
		if len(field.field) > 1 && (field.field[0] == '\'' || field.field[0] == '`') {
			field.field = field.field[1 : len(field.field)-1]
		}

		return &field, nil
	case *sqlparser.SQLVal:
		str := sqlparser.String(expr)
		if str[0] == '\'' {
			str = str[1 : len(str)-1]
			//str = str[-1]
		}
		field := NewConstSelectNode(str, alias)
		return &field, nil
	default:
		return nil, GoDBError{ParseError, fmt.Sprintf("unsupported expression type %s in select list", reflect.TypeOf(expr))}
	}

}
func parseSelect(c *Catalog, stmt sqlparser.SelectExpr) (*LogicalSelectNode, error) {
	star, ok := stmt.(*sqlparser.StarExpr)
	if ok {
		node := NewStarSelectNode(strings.ToLower(sqlparser.String(star.TableName)))
		return &node, nil
	}

	switch exprAlias := stmt.(type) {
	case (*sqlparser.AliasedExpr):
		alias := strings.ToLower(sqlparser.String(exprAlias.As))
		return parseExpr(c, exprAlias.Expr, alias)
	default:
		return nil, GoDBError{ParseError, fmt.Sprintf("unsupported expression type %s in select list", reflect.TypeOf(exprAlias))}

	}

}

func extractAggs(s *LogicalSelectNode) []*LogicalSelectNode {
	switch s.exprType {
	case ExprAggr:
		return []*LogicalSelectNode{s}
	case ExprFunc:
		var aggs []*LogicalSelectNode
		for _, subs := range s.args {
			aggs = append(aggs, extractAggs(subs)...)
		}
		return aggs
	}
	return nil
}

func parseStatement(c *Catalog, s *sqlparser.Select) (*LogicalPlan, error) {
	from := s.From
	var (
		tables   []*LogicalTableNode
		subplans []*LogicalPlan
		joins    []*LogicalJoinNode
		filters  []*LogicalFilterNode
		aggs     []*LogicalSelectNode
		selects  []*LogicalSelectNode
		groupBys []*GroupBy
		orderBys []*OrderByNode
	)

	for _, t := range from {
		newTables, newSubplans, newJoins, err := parseFrom(c, t)
		if err != nil {
			return nil, err
		}
		tables = append(tables, newTables...)
		subplans = append(subplans, newSubplans...)
		joins = append(joins, newJoins...)
	}
	where := s.Where
	if where != nil {
		//var newTs []*LogicalTableNode
		//for _, s := range subplans {
		//newTs = append(newTs, s.getTableNodes(c)...)
		/*			for _, f := range s.selects {
						field, tab, _ := f.getTableField(c, s.tables)
						fmt.Printf("%s.%s (%s)\n", field, s.alias, f.alias)
					}
		*/
		//}
		newFilters, newJoins, err := parseWhere(c, subplans, tables, where.Expr)
		if err != nil {
			return nil, err
		}
		joins = append(joins, newJoins...)
		filters = append(filters, newFilters...)
	}
	//extract select list
	for _, stmt := range s.SelectExprs {
		sel, err := parseSelect(c, stmt)
		if err != nil {
			return nil, err
		}
		selects = append(selects, sel)
		aggs = append(aggs, extractAggs(sel)...)
	}

	for _, gby := range s.GroupBy {
		expr, err := parseExpr(c, gby, "")
		if err != nil {
			return nil, err
		}
		groupBys = append(groupBys, &GroupBy{expr})
	}

	for _, oby := range s.OrderBy {
		expr, err := parseExpr(c, oby.Expr, "")
		if err != nil {
			return nil, err
		}
		orderBys = append(orderBys, &OrderByNode{expr, oby.Direction == sqlparser.AscScr})

	}

	lim := s.Limit
	var limExpr *LogicalSelectNode
	if lim != nil {
		var err error
		limExpr, err = parseExpr(c, lim.Rowcount, "")
		if err != nil {
			return nil, err
		}
	}

	p := LogicalPlan{filters, joins, selects, aggs, tables, subplans, groupBys, orderBys, limExpr, s.Distinct != "", ""}

	return &p, nil
}

func fieldToOp(tab string, field string, opMap map[string]*PlanNode) (*PlanNode, error) {
	node := opMap[tab]

	if node == nil && tab != "" {
		return nil, GoDBError{ParseError, fmt.Sprintf("no table in catalog matching '%s'", tab)}
	}
	if node == nil {
		for _, candNode := range opMap {
			if candNode == node {
				continue
			}
			for _, f := range candNode.desc.Fields {
				if f.Fname == field || field == "*" {
					if node == nil || tab == f.TableQualifier {
						node = candNode
					} else if node != nil && field != "*" {
						return nil, GoDBError{ParseError, fmt.Sprintf("field name '%s' is ambiguous", field)}
					}
				}
			}
		}
	}
	if node == nil {
		return nil, GoDBError{ParseError, fmt.Sprintf("no field in catalog matching '%s'", field)}
	}
	return node, nil
}

func fieldNameToField(table string, field string, node *PlanNode) (FieldType, error) {
	op := node.op
	var best FieldType
	gotField := false
	desc := node.desc
	for i, f := range node.desc.Fields {
		if f.Fname == field || field == "*" {
			if desc.Fields[i].TableQualifier == table || !gotField {
				best = op.Descriptor().Fields[i]
				gotField = true
			}
		}
	}
	if gotField {
		return best, nil
	}
	return FieldType{}, GoDBError{ParseError, fmt.Sprintf("no field in catalog matching '%s'", field)}
}

type PlanNode struct {
	op   Operator
	desc *TupleDesc
}

func (s *LogicalSelectNode) generateExpr(c *Catalog, inputDesc *TupleDesc, tableMap map[string]*PlanNode) (Expr, string, error) {
	switch s.exprType {
	case ExprAggr:
		fallthrough
	case ExprField:
		var field FieldType
		if inputDesc == nil {
			return nil, "", GoDBError{ParseError, "Tuple desc must be non-null for expression fields"}
		}
		if tableMap == nil {
			return nil, "", GoDBError{ParseError, "Table map must be non-null for expression fields"}
		}
		if s.cachedField != nil {
			field = *s.cachedField
		} else {
			fieldNo, err := findFieldInTd(FieldType{s.field, s.table, UnknownType}, inputDesc)
			// if it doesn't match a field in the descriptor,
			// look in the underlying tables
			if err != nil {
				selectNode, err := fieldToOp(s.table, s.field, tableMap)
				if err != nil {
					return nil, "", err
				}
				field, _ = fieldNameToField(s.table, s.field, selectNode)
				_, err = findFieldInTd(field, inputDesc)
				if err != nil {
					return nil, "", GoDBError{ParseError, fmt.Sprintf("cannot select field %s that is not child expression", s.field)}
				}
			} else {
				field = inputDesc.Fields[fieldNo]
			}
		}
		//fieldList = append(fieldList, field)
		var fieldName string
		if s.funcOp != nil {
			tName, fName, err := (*s).getTableField(nil, nil, nil)
			if err != nil {
				return nil, "", err
			}
			if tName != "" {
				tName = tName + "."
			}
			fieldName = fmt.Sprintf("%s(%s%s)", *s.funcOp, tName, fName)
		} else {
			fieldName = s.field
			/*if s.table != "" {
				fieldName = s.table + "." + fieldName
			}*/
		}
		if s.alias != "" {
			fieldName = s.alias
		}
		e := FieldExpr{field}
		return &e, fieldName, nil
	case ExprConst:

		var fval any
		constType := StringType
		intFval, e := strconv.Atoi(s.value)
		if e == nil {
			constType = IntType
			fval = IntField{int64(intFval)}
		} else {
			fval = StringField{s.value}
		}
		fieldName := s.value
		if s.alias != "" {
			fieldName = s.alias
		}
		ce := ConstExpr{fval, constType}
		return &ce, fieldName, nil
	case ExprFunc:
		fieldName := *s.funcOp
		if s.alias != "" {
			fieldName = s.alias
		}
		exprs := make([]*Expr, len(s.args))
		for i, lsn := range s.args {
			newExpr, _, err := lsn.generateExpr(c, inputDesc, tableMap)
			if err != nil {
				return nil, "", err
			}
			exprs[i] = &newExpr
		}

		fe := FuncExpr{*s.funcOp, exprs}
		return &fe, fieldName, nil
	}
	return nil, "", GoDBError{ParseError, "unhandled expression type in select list"}

}

const JoinBufferSize int = 10000000

func exprToStr(e Expr) string {
	switch ex := e.(type) {
	case *FieldExpr:
		tbl := ""
		if ex.selectField.TableQualifier != "" {
			tbl = ex.selectField.TableQualifier + "."
		}
		return fmt.Sprintf("%s%s", tbl, ex.selectField.Fname)
	case *ConstExpr:
		return fmt.Sprintf("%v", ex.val)
	case *FuncExpr:
		argStr := ""
		for _, arg := range ex.args {
			argStr += fmt.Sprintf("%s,", exprToStr(*arg))
		}
		return fmt.Sprintf("%s(%s)", ex.op, argStr)
	default:
		return fmt.Sprintf("%+v, ", e)
	}
}

func opToStr(op BoolOp) string {
	switch op {
	case OpEq:
		return "="
	case OpNeq:
		return "<>"
	case OpGe:
		return ">="
	case OpGt:
		return ">"
	case OpLe:
		return "<="
	case OpLt:
		return "<"
	case OpLike:
		return " LIKE "

	}
	return "??"
}

// following is absolute grossness because we forgot to ask students
// to expose heapfile name
func GetUnexportedField(field reflect.Value) interface{} {
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
}

func getStrFromObj(obj any) string {
	var v reflect.Value
	t := reflect.TypeOf(obj)
	if t.Kind() == reflect.Pointer {
		v = reflect.ValueOf(obj).Elem()
	} else {
		v = reflect.ValueOf(obj)
	}
	if reflect.TypeOf(v).Kind() != reflect.Struct {
		return fmt.Sprintf("%v", obj)
	}
	for i := 0; i < v.NumField(); i++ {
		v := GetUnexportedField(v.Field(i))
		str, ok := v.(string)
		if ok {
			return str
		}
	}
	return fmt.Sprintf("%v", obj)
}

func PrintPhysicalPlan(o Operator, indent string) {
	switch op := o.(type) {
	case *EqualityJoin[int64]:
		fmt.Printf("%sJoin, %+v == %+v\n", indent, exprToStr(op.leftField), exprToStr(op.rightField))
		indent = indent + "\t"
		PrintPhysicalPlan(*op.left, indent)
		PrintPhysicalPlan(*op.right, indent)
	case *EqualityJoin[string]:
		fmt.Printf("%sJoin, %+v == %+v\n", indent, exprToStr(op.leftField), exprToStr(op.rightField))
		indent = indent + "\t"
		PrintPhysicalPlan(*op.left, indent)
		PrintPhysicalPlan(*op.right, indent)

	case *Project:
		selectStr := ""
		for _, ex := range op.selectFields {
			selectStr += exprToStr(ex) + ","
		}
		fmt.Printf("%sProject %+v -> %+v\n", indent, selectStr, op.outputNames)
		indent = indent + "\t"
		PrintPhysicalPlan(op.child, indent)
	case *Filter[int64]:
		fmt.Printf("%sFilter %s %s %s\n", indent, exprToStr(op.left), opToStr(op.op), exprToStr(op.right))
		indent = indent + "\t"
		PrintPhysicalPlan(op.child, indent)
	case *Filter[string]:
		fmt.Printf("%sFilter %s %s %s\n", indent, exprToStr(op.left), opToStr(op.op), exprToStr(op.right))
		indent = indent + "\t"
		PrintPhysicalPlan(op.child, indent)
	case *HeapFile:
		fmt.Printf("%sHeap Scan %v\n", indent, getStrFromObj(op))
	case *OrderBy:
		orderStr := ""
		for _, ex := range op.orderBy {
			orderStr += exprToStr(ex) + ","
		}
		fmt.Printf("%sOrder By %s\n", indent, orderStr)
		indent = indent + "\t"
		PrintPhysicalPlan(op.child, indent)
	case *LimitOp:
		fmt.Printf("%sLimit %s\n", indent, exprToStr(op.limitTups))
		indent = indent + "\t"
		PrintPhysicalPlan(op.child, indent)
	case *Aggregator:
		gbyStr := ""
		if len(op.groupByFields) > 0 {
			gbyStr = "Group By "
		}
		for _, ex := range op.groupByFields {
			gbyStr += exprToStr(ex) + ","
		}

		aggStr := ""
		for _, ex := range op.newAggState {

			aggStr += fmt.Sprintf("%s(%s),", reflect.TypeOf(ex), ex.GetTupleDesc().HeaderString(false))
		}

		fmt.Printf("%sAggregate, %s %s\n", indent, aggStr, gbyStr)
		indent = indent + "\t"
		PrintPhysicalPlan(op.child, indent)
	default:
		fmt.Printf("%sUnknown op, %s\n", indent, reflect.TypeOf(op))
	}
}

func makePhysicalPlan(c *Catalog, plan *LogicalPlan) (Operator, error) {
	//build mapping from table names / aliases to operators

	tableMap := make(map[string]*PlanNode)

	for _, p := range plan.subqueries {
		subPhysP, err := makePhysicalPlan(c, p)
		if err != nil {
			return nil, err
		}
		var td *TupleDesc = subPhysP.Descriptor()
		td.setTableAlias(p.alias)
		//td = td.setTableAlias(p.alias)
		tableMap[p.alias] = &PlanNode{subPhysP, td}
	}
	for _, t := range plan.tables {
		name := t.tableName
		if t.alias != "" {
			name = t.alias
		}
		var td *TupleDesc = (*t.file).Descriptor()
		td.setTableAlias(name)
		//td = td.setTableAlias(name)
		tableMap[name] = &PlanNode{*t.file, td}
	}

	//now apply each filter to appropriate table
	for _, f := range plan.filters {
		tabName, fieldName, err := f.fieldExpr.getTableField(c, plan.subqueries, plan.tables)
		if err != nil {
			return nil, err
		}
		node, err := fieldToOp(tabName, fieldName, tableMap)
		if err != nil {
			return nil, err
		}
		leftExpr, _, err := f.fieldExpr.generateExpr(c, node.desc, tableMap)
		if err != nil {
			return nil, err
		}
		rightExpr, _, err := f.constExpr.generateExpr(c, node.desc, tableMap)
		if err != nil {
			return nil, err
		}

		op := node.op
		desc := *op.Descriptor()
		desc.setTableAlias(tabName)

		switch leftExpr.GetExprType().Ftype {
		case IntType:
			newOp, err := NewIntFilter(rightExpr, f.predOp, leftExpr, op)
			if err != nil {
				return nil, err
			}
			tableMap[leftExpr.GetExprType().TableQualifier] = &PlanNode{newOp, &desc}
		case StringType:
			newOp, err := NewStringFilter(rightExpr, f.predOp, leftExpr, op)
			if err != nil {
				return nil, err
			}
			tableMap[leftExpr.GetExprType().TableQualifier] = &PlanNode{newOp, &desc}
		}
	}
	//finally apply joins
	for _, j := range plan.joins {
		lTabName, lFieldName, err := j.left.getTableField(c, plan.subqueries, plan.tables)
		if err != nil {
			return nil, err
		}

		node1, err := fieldToOp(lTabName, lFieldName, tableMap)
		if err != nil {
			return nil, err
		}

		rTabName, rFieldName, err := j.right.getTableField(c, plan.subqueries, plan.tables)
		if err != nil {
			return nil, err
		}

		node2, err := fieldToOp(rTabName, rFieldName, tableMap)
		if err != nil {
			return nil, err
		}

		/*desc1 := *op1.Descriptor()
		desc1.setTableAlias(j.t1)
		desc2 := *op2.Descriptor()
		desc2.setTableAlias(j.t2)
		*/
		op1 := node1.op
		op2 := node2.op

		/*
			leftField, _ := fieldNameToField(j.t1, j.f1, node1)
			rightField, _ := fieldNameToField(j.t2, j.f2, node2)
		*/
		leftExpr, _, err := j.left.generateExpr(c, node1.desc, tableMap)
		if err != nil {
			return nil, err
		}
		rightExpr, _, err := j.right.generateExpr(c, node2.desc, tableMap)
		if err != nil {
			return nil, err
		}

		var (
			newOp Operator
		)
		switch leftExpr.GetExprType().Ftype {
		case IntType:
			newOp, err = NewIntJoin(op1, leftExpr, op2, rightExpr, JoinBufferSize)
		case StringType:
			newOp, err = NewStringJoin(op1, leftExpr, op2, rightExpr, JoinBufferSize)
		}
		if err != nil {
			return nil, err
		}
		newNode := &PlanNode{newOp, newOp.Descriptor()}
		for key, node := range tableMap {
			if node.op == op1 {
				tableMap[key] = newNode
			}
			if node.op == op2 {
				tableMap[key] = newNode
			}
		}
		tableMap[lTabName] = newNode
		tableMap[rTabName] = newNode
		//&PlanNode{newOp, newOp.Descriptor()}

	}

	//check that all tables have the same op (all tables are joined)
	first := true
	var curOp Operator
	for _, node := range tableMap {
		if first {
			curOp = node.op
			first = false
		} else {
			if curOp != node.op {
				return nil, GoDBError{ParseError, "not all tables are joined, cross products are not supported in GoDB"}
			}
		}
	}

	topOp := curOp

	//var fieldList []FieldType
	var fieldNames []string
	hasAgg := len(plan.aggs) > 0
	selectAll := false

	/*
		for _, s := range plan.selects {
			if s.exprType == ExprAggr {
				hasAgg = true
				break
			}
		}
	*/

	if hasAgg {
		var gbys []Expr
		var aggs []AggState

		var aggCnt int
		for _, s := range plan.aggs {
			/*
				selectNode, err := fieldToOp(s.table, s.field, tableMap)
				if err != nil {
					return nil, err
				}
				field, err := fieldNameToField(s.table, s.field, selectNode)
				if err != nil {
					return nil, err
				}
			*/

			var getter func(DBValue) any
			if s.exprType == ExprAggr {
				var as AggState

				tabName, fieldName, err := s.args[0].getTableField(c, plan.subqueries, plan.tables)
				if err != nil {
					return nil, err
				}
				node, err := fieldToOp(tabName, fieldName, tableMap)
				if err != nil {
					return nil, err
				}
				aggExpr, _, err := s.args[0].generateExpr(c, node.desc, tableMap)
				if err != nil {
					return nil, err
				}

				switch aggExpr.GetExprType().Ftype {
				case IntType:
					getter = intAggGetter
				case StringType:
					getter = stringAggGetter
				}

				switch *s.funcOp {
				case "max":
					if aggExpr.GetExprType().Ftype == StringType {
						as = &MaxAggState[string]{}
					} else {
						as = &MaxAggState[int64]{}
					}

				case "min":
					if aggExpr.GetExprType().Ftype == StringType {
						as = &MinAggState[string]{}
					} else {
						as = &MinAggState[int64]{}
					}
				case "avg":
					as = &AvgAggState[int64]{}
				case "sum":
					as = &SumAggState[int64]{}
				case "count":
					as = &CountAggState{}
				default:
					return nil, GoDBError{IllegalOperationError, fmt.Sprintf("unknown aggregate function %s", *s.funcOp)}
				}
				//make sure name has unique id
				name := fmt.Sprintf("%s(%s.%s)%d", *s.funcOp, tabName, fieldName, aggCnt)
				aggCnt++
				if s.alias != "" {
					name = s.alias
				}
				as.Init(name, aggExpr, getter)
				aggs = append(aggs, as)
				s.cachedField = &as.GetTupleDesc().Fields[0] //track aggregates by reference rather than name
			}
		}

		for _, gby := range plan.groupByFields {
			expr, _, err := gby.expr.generateExpr(c, topOp.Descriptor(), tableMap)
			if err != nil {
				return nil, err
			}
			gbys = append(gbys, expr)
		}

		if len(gbys) == 0 {
			topOp = NewAggregator(aggs, topOp)
		} else {
			topOp = NewGroupedAggregator(aggs, gbys, topOp)
		}
	}
	exprList := make([]Expr, len(plan.selects))
	for i, s := range plan.selects {
		switch s.exprType {
		case ExprStar:
			if s.field == "*" && s.funcOp == nil {
				selectAll = true
			}
		default:
			expr, field, err := s.generateExpr(c, topOp.Descriptor(), tableMap)
			if err != nil {
				return nil, err
			}
			exprList[i] = expr
			fieldNames = append(fieldNames, field)
		}
	}
	if !selectAll {
		projOp, err := NewProjectOp(exprList, fieldNames, plan.distinct, topOp)
		if err != nil {
			return nil, err
		}
		topOp = projOp
	}

	if len(plan.orderByFields) > 0 {
		var ascs []bool

		exprs := make([]Expr, len(plan.orderByFields))
		for i, oby := range plan.orderByFields {
			expr, _, err := oby.expr.generateExpr(c, topOp.Descriptor(), tableMap)
			if err != nil {
				return nil, err
			}
			exprs[i] = expr
			ascs = append(ascs, oby.ascending)

		}
		var err error
		topOp, err = NewOrderBy(exprs, topOp, ascs)
		if err != nil {
			return nil, err
		}

	}

	if plan.limit != nil {
		expr, _, err := plan.limit.generateExpr(c, topOp.Descriptor(), tableMap)
		if err != nil {
			return nil, err
		}
		topOp = NewLimitOp(expr, topOp)
	}
	return topOp, nil
}

func parseInsert(c *Catalog, insStmt *sqlparser.Insert) (Operator, error) {
	if insStmt.Columns != nil {
		return nil, GoDBError{ParseError, "GoDB doesn't support inserts of incomplete tuples"}
	}
	tab := insStmt.Table.Name
	file, err := c.GetTable(sqlparser.String(tab))
	if err != nil {
		return nil, err
	}

	switch stmt := insStmt.Rows.(type) {
	case sqlparser.Values:
		var exprAr []([]Expr)
		for _, t := range stmt {
			var tupAr []Expr
			for _, e := range t {
				expr, err := parseExpr(c, e, "")
				if err != nil {
					return nil, err
				}
				exprOp, _, err := expr.generateExpr(c, nil, nil) //ok for input desc and map to be null, since these are constant expressions
				if err != nil {
					return nil, err
				}
				tupAr = append(tupAr, exprOp)
			}
			exprAr = append(exprAr, tupAr)
		}
		iterOp := NewValueOp(exprAr)
		insertOp := NewInsertOp(file, iterOp)
		return insertOp, nil

	case *sqlparser.Select:
		plan, err := parseStatement(c, stmt)
		if err != nil {
			return nil, err
		}
		op, err := makePhysicalPlan(c, plan)
		if err != nil {
			return nil, err
		}

		insertOp := NewInsertOp(file, op)
		return insertOp, nil
	}
	return nil, nil
}

func parseDelete(c *Catalog, delStmt *sqlparser.Delete) (Operator, error) {
	if len(delStmt.TableExprs) > 1 {
		return nil, GoDBError{ParseError, "godb does not supporting deleting from multiple tables"}
	}
	tables, subplans, joins, err := parseFrom(c, delStmt.TableExprs[0])
	if err != nil {
		return nil, err
	}
	if len(tables) > 1 {
		return nil, GoDBError{ParseError, "godb does not supporting deleting from multiple tables"}
	}
	if subplans != nil || joins != nil {
		return nil, GoDBError{ParseError, "godb does not supporting deleting from multiple tables"}
	}

	tableMap := make(map[string]*PlanNode)
	tableMap[tables[0].tableName] = &PlanNode{*tables[0].file, (*tables[0].file).Descriptor()}

	var filters []*LogicalFilterNode = make([]*LogicalFilterNode, 0)
	if delStmt.Where != nil {
		filters, joins, err = parseWhere(c, subplans, tables, delStmt.Where.Expr)
		if err != nil {
			return nil, err
		}
		if joins != nil {
			return nil, GoDBError{ParseError, "godb does not supporting deleting from multiple tables"}
		}
	}
	var newOp Operator
	newOp = *tables[0].file
	for _, f := range filters {
		tabName, fieldName, err := f.fieldExpr.getTableField(c, subplans, tables)
		if err != nil {
			return nil, err
		}
		node, err := fieldToOp(tabName, fieldName, tableMap)
		if err != nil {
			return nil, err
		}
		leftExpr, _, err := f.fieldExpr.generateExpr(c, node.desc, tableMap)
		if err != nil {
			return nil, err
		}
		rightExpr, _, err := f.constExpr.generateExpr(c, node.desc, tableMap)
		if err != nil {
			return nil, err
		}

		//op := node.op
		//dbField, _ := fieldNameToField(f.table, f.field, &PlanNode{op, &desc})

		switch leftExpr.GetExprType().Ftype {
		case IntType:
			//newInt, _ := strconv.Atoi(f.constVal)
			newOp, err = NewIntFilter(rightExpr, f.predOp, leftExpr, newOp)
			if err != nil {
				return nil, err
			}
		case StringType:
			newOp, err = NewStringFilter(rightExpr, f.predOp, leftExpr, newOp)
			if err != nil {
				return nil, err
			}
		}
	}
	return NewDeleteOp(*tables[0].file, newOp), nil

}

type QueryType int

const (
	IteratorType         QueryType = iota
	BeginXactionType     QueryType = iota
	CommitXactionType    QueryType = iota
	AbortXactionType     QueryType = iota
	CreateTableQueryType QueryType = iota
	DropTableQueryType   QueryType = iota
	UnknownQueryType     QueryType = iota
)

func processDDL(c *Catalog, ddl *sqlparser.DDL) (QueryType, error) {
	switch ddl.Action {
	case "create":
		fields := make([]FieldType, len(ddl.TableSpec.Columns))
		tabName := sqlparser.String(ddl.NewName.Name)
		t, _ := c.GetTable(tabName)
		if t != nil {
			return UnknownQueryType, GoDBError{ParseError, fmt.Sprintf("table %s already exists", tabName)}
		}
		for i, col := range ddl.TableSpec.Columns {
			var colType DBType
			colName := sqlparser.String(col.Name)
			switch col.Type.Type {
			case "int":
				colType = IntType
			case "string":
				fallthrough
			case "text":
				fallthrough
			case "varchar":
				colType = StringType
			default:
				return UnknownQueryType, GoDBError{ParseError, fmt.Sprintf("unsupported column type %s", col.Type.Type)}

			}
			fields[i] = FieldType{colName, "", colType}
		}

		c.addTable(tabName, TupleDesc{fields})
		return CreateTableQueryType, nil

	case "drop":
		tabName := sqlparser.String(ddl.Table.Name)
		err := c.dropTable(tabName)
		if err != nil {
			return UnknownQueryType, err
		}
		return DropTableQueryType, nil
	default:
		return UnknownQueryType, GoDBError{ParseError, fmt.Sprintf("unsupported ddl statement %s", ddl.Action)}
	}
}

func Parse(c *Catalog, query string) (QueryType, Operator, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return UnknownQueryType, nil, err
	}
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		plan, err := parseStatement(c, stmt)
		if err != nil {
			//fmt.Printf("Err: %s\n", err.Error())
			return UnknownQueryType, nil, err
		}
		op, err := makePhysicalPlan(c, plan)
		if err != nil {
			//fmt.Printf("Err: %s\n", err.Error())
			return UnknownQueryType, nil, err
		}
		return IteratorType, op, nil
	case *sqlparser.Insert:
		op, err := parseInsert(c, stmt)
		if err != nil {
			return UnknownQueryType, nil, err
		}
		return IteratorType, op, nil
	case *sqlparser.Delete:
		op, err := parseDelete(c, stmt)
		if err != nil {
			return UnknownQueryType, nil, err
		}
		return IteratorType, op, nil
	case *sqlparser.Begin:
		return BeginXactionType, nil, nil
	case *sqlparser.Commit:
		return CommitXactionType, nil, nil
	case *sqlparser.Rollback:
		return AbortXactionType, nil, nil
	case *sqlparser.DDL:
		qtype, err := processDDL(c, stmt)
		if err != nil {
			return UnknownQueryType, nil, err
		} else {
			return qtype, nil, nil
		}
	}

	return UnknownQueryType, nil, GoDBError{ParseError, "invalid query"}
}
