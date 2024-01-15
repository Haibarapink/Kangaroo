package godb

import (
	"fmt"
	"os"
	"testing"
)

func MakeTestDatabaseEasy(bp *BufferPool) error {
	var td = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
	}}
	os.Remove("t2.dat")
	os.Remove("t.dat")

	hf, err := NewHeapFile("t.dat", &td, bp)
	if err != nil {
		return err
	}
	hf2, err := NewHeapFile("t2.dat", &td, bp)
	if err != nil {
		return err
	}

	f, err := os.Open("testdb.txt")
	if err != nil {
		return err
	}
	err = hf.LoadFromCSV(f, true, ",", false)
	if err != nil {
		return err
	}

	f, err = os.Open("testdb.txt")
	if err != nil {
		return err
	}
	err = hf2.LoadFromCSV(f, true, ",", false)
	if err != nil {
		return err
	}
	return nil
}

func TestParseEasy(t *testing.T) {
	var queries []string = []string{
		"select name,age,getsubstr(epochtodatetimestring(epoch() - age*365*24*60*60),24,4) birthyear from t",
		"select sum(age + 10) , sum(age) from t",
		"select min(age) + max(age) from t",
		"select * from t limit 1+2",
		"select t.name, t.age from t join t2 on t.name = t2.name, t2 as t3 where t.age < 50 and t3.age = t.age order by t.age asc, t.name asc",
		"select sq(sq(5)) from t",
		"select 1, name from t",
		"select age, name from t",
		"select t.name, sum(age) totage from t group by t.name",
		"select t.name, t.age from t join t2 on t.name = t2.name where t.age < 50",
		"select name from (select x.name from (select t.name from t) x)y order by name asc",
		"select age, count(*) from t group by age",
	}
	save := false        //set save to true to save the output of the current test run as the correct answer
	printOutput := false //print the result set during testing

	bp := NewBufferPool(10)
	err := MakeTestDatabaseEasy(bp)
	if err != nil {
		t.Errorf("failed to create test database, %s", err.Error())
		return
	}

	c, err := NewCatalogFromFile("catalog.txt", bp, "./")
	if err != nil {
		t.Errorf("failed load catalog, %s", err.Error())
		return
	}
	qNo := 0
	for _, sql := range queries {
		tid := NewTID()
		bp.BeginTransaction(tid)
		qNo++
		if qNo == 4 {
			continue
		}

		qType, plan, err := Parse(c, sql)
		if err != nil {
			t.Errorf("failed to parse, q=%s, %s", sql, err.Error())
			return
		}
		if plan == nil {
			t.Errorf("plan was nil")
			return
		}
		if qType != IteratorType {
			continue
		}

		var outfile *HeapFile
		var outfile_csv *os.File
		var resultSet []*Tuple
		fname := fmt.Sprintf("savedresults/q%d-easy-result.csv", qNo)

		if save {
			os.Remove(fname)
			outfile_csv, err = os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				t.Errorf("failed to open CSV file (%s)", err.Error())
				return
			}
			//outfile, _ = NewHeapFile(fname, plan.Descriptor(), bp)
		} else {
			fname_bin := fmt.Sprintf("savedresults/q%d-easy-result.dat", qNo)
			os.Remove(fname_bin)
			desc := plan.Descriptor()
			if desc == nil {
				t.Errorf("descriptor was nil")
				return
			}

			outfile, _ = NewHeapFile(fname_bin, desc, bp)
			if outfile == nil {
				t.Errorf("heapfile was nil")
				return
			}
			f, err := os.Open(fname)
			if err != nil {
				t.Errorf("csv file with results was nil (%s)", err.Error())
				return
			}
			err = outfile.LoadFromCSV(f, true, ",", false)
			if err != nil {
				t.Errorf(err.Error())
				return
			}

			resultIter, err := outfile.Iterator(tid)
			if err != nil {
				t.Errorf("%s", err.Error())
				return
			}
			for {
				tup, err := resultIter()
				if err != nil {
					t.Errorf("%s", err.Error())
					break
				}

				if tup != nil {
					resultSet = append(resultSet, tup)
				} else {
					break
				}
			}
		}

		if printOutput || save {
			fmt.Printf("Doing %s\n", sql)
			iter, err := plan.Iterator(tid)
			if err != nil {
				t.Errorf("%s", err.Error())
				return
			}
			nresults := 0
			if save {
				fmt.Fprintf(outfile_csv, "%s\n", plan.Descriptor().HeaderString(false))
			}
			fmt.Printf("%s\n", plan.Descriptor().HeaderString(true))
			for {
				tup, err := iter()
				if err != nil {
					t.Errorf("%s", err.Error())
					break
				}
				if tup == nil {
					break
				} else {
					fmt.Printf("%s\n", tup.PrettyPrintString(true))
				}
				nresults++
				if save {
					fmt.Fprintf(outfile_csv, "%s\n", tup.PrettyPrintString(false))
					//outfile.insertTuple(tup, tid)
				}
			}
			fmt.Printf("(%d results)\n\n", nresults)
		}
		if save {
			//outfile.bufPool.CommitTransaction(tid)
			outfile_csv.Close()
		} else {
			iter, err := plan.Iterator(tid)
			if err != nil {
				t.Errorf("%s", err.Error())
				return
			}
			match := CheckIfOutputMatches(iter, resultSet)
			if !match {
				t.Errorf("query '%s' did not match expected result set", sql)
				verbose := true
				if verbose {
					fmt.Print("Expected: \n")
					for _, r := range resultSet {
						fmt.Printf("%s\n", r.PrettyPrintString(true))
					}
				}
			}
		}
	}

	//print(op)

}
