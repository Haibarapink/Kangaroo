package godb

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type Table struct {
	name string
	desc TupleDesc
}

type Catalog struct {
	tables    []*Table
	tableMap  map[string]*Table
	columnMap map[string][]*Table
	bp        *BufferPool
	rootPath  string
}

func (c *Catalog) SaveToFile(catalogFile string, rootPath string) error {
	catalogString := c.CatalogString()
	f, err := os.OpenFile(rootPath+"/"+catalogFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	f.WriteString(catalogString)
	f.Close()
	return nil
}

func (c *Catalog) dropTable(table string) error {
	for i, t := range c.tables {
		if t.name == table {
			c.tableMap[table] = nil
			c.columnMap[table] = nil
			c.tables = append(c.tables[:i], c.tables[i+1:]...)
			os.Remove(c.tableNameToFile(table))
			return nil
		}
	}
	return GoDBError{NoSuchTableError, "couldn't find table to drop"}
}

func ImportCatalogFromCSVs(catalogFile string, bp *BufferPool, rootPath string, tableSuffix string, separator string) error {
	c, err := NewCatalogFromFile(catalogFile, bp, rootPath)
	if err != nil {
		return err
	}
	for _, t := range c.tables {
		fmt.Printf("Doing %s\n", t.name)
		fileName := rootPath + "/" + t.name + "." + tableSuffix
		hf, err := NewHeapFile(c.tableNameToFile(t.name), t.desc.copy(), c.bp)
		if err != nil {
			return err
		}
		f, err := os.Open(fileName)
		if err != nil {
			return err
		}
		err = hf.LoadFromCSV(f, false, separator, true)
		if err != nil {
			return err
		}

	}
	return nil
}

func parseCatalogFile(catalogFile string, rootPath string) ([]TupleDesc, []string, error) {
	var tables []TupleDesc
	var names []string
	f, err := os.Open(rootPath + "/" + catalogFile)
	if err != nil {
		return nil, nil, err
	}
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		// code to read each line
		line := strings.ToLower(scanner.Text())
		sep := strings.Split(line, "(")
		if len(sep) != 2 {
			return nil, nil, GoDBError{ParseError, fmt.Sprintf("expected one paren in catalog entry, got %d (%s)", len(sep), line)}
		}
		tableName := strings.TrimSpace(sep[0])
		rest := strings.Trim(sep[1], "()")
		fields := strings.Split(rest, ",")
		var fieldArray []FieldType
		for _, f := range fields {
			f := strings.TrimSpace(f)
			nameType := strings.Split(f, " ")
			if len(nameType) != 2 {
				return nil, nil, GoDBError{ParseError, fmt.Sprintf("malformed catalog entry %s (line %s)", nameType, line)}
			}
			switch nameType[1] {
			case "int":
				fallthrough
			case "integer":
				fieldArray = append(fieldArray, FieldType{nameType[0], "", IntType})
			case "string":
				fallthrough
			case "varchar":
				fallthrough
			case "text":
				fieldArray = append(fieldArray, FieldType{nameType[0], "", StringType})
			default:
				return nil, nil, GoDBError{ParseError, fmt.Sprintf("unknown type %s (line %s)", nameType[1], line)}
			}
		}
		tables = append(tables, TupleDesc{fieldArray})
		names = append(names, tableName)
	}
	return tables, names, nil

}

func NewCatalogFromFile(catalogFile string, bp *BufferPool, rootPath string) (*Catalog, error) {
	tabs, names, err := parseCatalogFile(catalogFile, rootPath)
	if err != nil {
		return nil, err
	}
	c := &Catalog{make([]*Table, 0), make(map[string]*Table), make(map[string][]*Table), bp, rootPath}
	for i, t := range tabs {
		c.addTable(names[i], t)
	}

	return c, nil

}

func (c *Catalog) addTable(named string, desc TupleDesc) error {
	_, err := c.GetTable(named)
	if err != nil {
		t := &Table{named, desc}
		c.tables = append(c.tables, t)
		c.tableMap[named] = t
		for _, f := range desc.Fields {
			mapList := c.columnMap[f.Fname]
			if mapList == nil {
				mapList = make([]*Table, 0)
			}
			mapList = append(mapList, t)
			c.columnMap[f.Fname] = mapList
		}
		return nil
	} else {
		return GoDBError{DuplicateTableError, fmt.Sprintf("a table named '%s' already exists", named)}
	}
}

func (c *Catalog) tableNameToFile(tableName string) string {
	return c.rootPath + "/" + tableName + ".dat"

}
func (c *Catalog) GetTable(named string) (DBFile, error) {
	t := c.tableMap[named]
	if t == nil {
		return nil, GoDBError{NoSuchTableError, fmt.Sprintf("no table '%s' found", named)}
	}
	return NewHeapFile(c.tableNameToFile(named), t.desc.copy(), c.bp)

}

func (c *Catalog) findTablesWithColumn(named string) []*Table {
	t := c.columnMap[named]
	return t

}

func (c *Catalog) NumTables() int {
	return len(c.tables)
}

func (c *Catalog) GetTableIdx(t int) (DBFile, error) {
	tab := c.tables[t]
	return c.GetTable(tab.name)
}

func (c *Catalog) CatalogString() string {
	outStr := ""
	for _, t := range c.tables {
		fieldStr := "("
		for i, f := range t.desc.Fields {
			if i != 0 {
				fieldStr = fieldStr + ", "
			}
			fieldStr = fieldStr + f.Fname + " " + typeNames[f.Ftype]
		}
		outStr = outStr + t.name + " " + fieldStr + ")\n"
	}
	return outStr
}
