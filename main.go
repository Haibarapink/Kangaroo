package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/chzyer/readline"
	"github.com/srmadden/godb"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

var helpText = `Enter a SQL query terminated by a ; to process it.  Commands prefixed with \ are processed as shell commands.

Available shell commands:
	\h : This help
	\c path/to/catalog : Change the current database to a specified catalog file
	\d : List tables and fields in the current database
	\f : List available functions for use in queries
	\a : Toggle aligned vs csv output
	\l table path/to/file [sep] [hasHeader]: Append csv file to end of table.  Default to sep = ',', hasHeader = 'true'`

/*func printCatalog(fname string) {
	f, err := os.Open(fname)
	if err != nil {
		fmt.Printf("failed load catalog, %s", err.Error())
		return
	}
	scanner := bufio.NewScanner(f)

	fmt.Print("\033[34;4mTables\033[0m\n")

	for scanner.Scan() {
		// code to read each line
		line := scanner.Text()
		fmt.Printf("\033[34m  %s\n\033[0m", line)
	}
	f.Close()
}*/

func printCatalog(c *godb.Catalog) {
	s := c.CatalogString()
	fmt.Printf("\033[34m%s\n\033[0m", s)
}

func main() {
	alarm := make(chan int, 1)

	go func() {
		c := make(chan os.Signal)

		signal.Notify(c, os.Interrupt, syscall.SIGINT)
		go func() {
			for {
				<-c
				alarm <- 1
				fmt.Println("Interrupted query.")
			}
		}()

	}()

	bp := godb.NewBufferPool(10000)
	/*
		err := godb.ImportCatalogFromCSVs("tpch-catalog.sql", bp, "godb/tpch-dbgen", "tbl", "|")
		if err != nil {
			fmt.Printf("failed load catalog, %s\n", err.Error())
			return
		}
	*/
	//catName := "tpch-catalog.sql"
	//catPath := "godb/tpch-dbgen"

	catName := "catalog.txt"
	catPath := "godb"

	c, err := godb.NewCatalogFromFile(catName, bp, catPath)
	if err != nil {
		fmt.Printf("failed load catalog, %s", err.Error())
		return
	}
	rl, err := readline.New("> ")
	if err != nil {
		panic(err)
	}
	defer rl.Close()

	fmt.Printf("\033[35;1m")
	fmt.Println(`Welcome to
	Kangaroo
	\h for help`)
	fmt.Printf("\033[0m\n")
	query := ""
	var autocommit bool = true
	var tid godb.TransactionID
	aligned := true
	for {

		//text := "SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue desc, o_orderdate LIMIT 20"
		//text := "select count(*) from lineitem where l_orderkey = 1;"
		f, err := os.Create("prog.prof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()

		text, err := rl.Readline()
		if err != nil { // io.EOF
			break
		}
		text = strings.TrimSpace(text)
		if len(text) == 0 {
			continue
		}
		//	// convert CRLF to LF
		//text = strings.Replace(text, "\n", "", -1)
		if text[0] == '\\' {
			switch text[1] {
			case 'd':
				printCatalog(c) // catPath + "/" + catName)
			case 'c':
				if len(text) > 3 {
					rest := text[3:len(text)]
					pathAr := strings.Split(rest, "/")
					catName = pathAr[len(pathAr)-1]
					catPath = strings.Join(pathAr[0:len(pathAr)-1], "/")
					c, err = godb.NewCatalogFromFile(catName, bp, catPath)
					if err != nil {
						fmt.Printf("failed load catalog, %s\n", err.Error())
						continue
					}
					fmt.Printf("Loaded %s/%s\n", catPath, catName)
					//	printCatalog(catPath + "/" + catName)
					printCatalog(c)

				} else {
					fmt.Printf("Expected catalog file name after /c")
				}
			case 'f':
				fmt.Println("Available functions:")
				fmt.Printf(godb.ListOfFunctions())
			case 'a':
				aligned = !aligned
				if aligned {
					fmt.Println("Output aligned")
				} else {
					fmt.Println("Output unaligned")
				}

			case '?':
				fallthrough
			case 'h':
				fmt.Println(helpText)

			case 'l':
				splits := strings.Split(text, " ")
				table := splits[1]
				path := splits[2]
				sep := ","
				hasHeader := true
				if len(splits) > 3 {
					sep = splits[3]
				}
				if len(splits) > 4 {
					hasHeader = splits[4] != "false"
				}

				//todo -- following code assumes data is in heap files
				hf, err := c.GetTable(table)
				if err != nil {
					fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
					continue
				}
				heapFile := hf.(*godb.HeapFile)
				f, err := os.Open(path)
				if err != nil {
					fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
					continue
				}
				err = heapFile.LoadFromCSV(f, hasHeader, sep, false)
				if err != nil {
					fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
					continue
				}
				fmt.Printf("\033[32;1mLOAD\033[0m\n\n")
			}

			query = ""
			continue
		}
		if text[len(text)-1] != ';' {
			query = query + " " + text
			continue
		}
		query = strings.TrimSpace(query + " " + text[0:len(text)-1])

		explain := false
		if strings.HasPrefix(strings.ToLower(query), "explain") {
			queryParts := strings.Split(query, " ")
			query = strings.Join(queryParts[1:], " ")
			explain = true
		}

		queryType, plan, err := godb.Parse(c, query)
		//fmt.Println(query)
		query = ""
		nresults := 0

		if err != nil {
			errStr := err.Error()
			if strings.Contains(errStr, "position") {
				//fmt.Println(errStr)
				positionPos := strings.LastIndex(errStr, "position")
				positionPos += 9

				spacePos := strings.Index(errStr[positionPos:], " ")
				if spacePos == -1 {
					spacePos = len(errStr) - positionPos
				}
				//fmt.Printf("%d\n", spacePos+positionPos)
				//fmt.Printf("%d\n", positionPos)

				posStr := errStr[positionPos : spacePos+positionPos]
				pos, err := strconv.Atoi(posStr)
				//fmt.Printf("%s (%d)\n", posStr, pos)
				if err == nil {
					s := strings.Repeat(" ", pos)
					fmt.Printf("\033[31;1m%s^\033[0m\n", s)
				}
			}
			fmt.Printf("\033[31;1mInvalid query (%s)\033[0m\n", err.Error())
			continue
		}

		switch queryType {
		case godb.IteratorType:
			if explain {
				fmt.Printf("\033[32m")
				godb.PrintPhysicalPlan(plan, "")
				fmt.Printf("\033[0m\n")
				break
			}
			if autocommit {
				tid = godb.NewTID()
				bp.BeginTransaction(tid)
			}
			start := time.Now()

			iter, err := plan.Iterator(tid)
			if err != nil {
				fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
				continue
			}

			fmt.Printf("\033[32;4m%s\033[0m\n", plan.Descriptor().HeaderString(aligned))

			for {
				tup, err := iter()
				if err != nil {
					fmt.Printf("%s\n", err.Error())
					break
				}
				if tup == nil {
					break
				} else {
					fmt.Printf("\033[32m%s\033[0m\n", tup.PrettyPrintString(aligned))
				}
				nresults++
				select {
				case <-alarm:
					fmt.Println("Aborting")
					goto outer
				default:

				}
			}
			if autocommit {
				bp.CommitTransaction(tid)
			}
		outer:
			fmt.Printf("\033[32;1m(%d results)\033[0m\n", nresults)
			duration := time.Since(start)
			fmt.Printf("\033[32;1m%v\033[0m\n\n", duration)

		case godb.BeginXactionType:
			if !autocommit {
				fmt.Printf("\033[31;1m%s\033[0m\n", "Cannot start transaction while in transaction")
			} else {
				tid = godb.NewTID()
				bp.BeginTransaction(tid)
				autocommit = false
				fmt.Printf("\033[32;1mBEGIN\033[0m\n\n")
			}
		case godb.AbortXactionType:
			if autocommit {
				fmt.Printf("\033[31;1m%s\033[0m\n", "Cannot abort transaction unless in transaction")
			} else {
				bp.AbortTransaction(tid)
				autocommit = true
				fmt.Printf("\033[32;1mABORT\033[0m\n\n")
			}

		case godb.CommitXactionType:
			if autocommit {
				fmt.Printf("\033[31;1m%s\033[0m\n", "Cannot commit transaction unless in transaction")
			} else {
				bp.CommitTransaction(tid)
				autocommit = true
				fmt.Printf("\033[32;1mCOMMIT\033[0m\n\n")
			}
		case godb.CreateTableQueryType:
			fmt.Printf("\033[32;1mCREATE\033[0m\n\n")
			err := c.SaveToFile(catName, catPath)
			if err != nil {
				fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
			}
		case godb.DropTableQueryType:
			fmt.Printf("\033[32;1mDROP\033[0m\n\n")
			err := c.SaveToFile(catName, catPath)
			if err != nil {
				fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
			}
		}

	}
}
