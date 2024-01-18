package godb

import "os"

// This function should load the csv file in fileName into a heap file (see
// [HeapFile.LoadFromCSV]) and then compute the sum of the integer field in
// string and return its value as an int The supplied csv file is comma
// delimited and has a header If the file doesn't exist or can't be opened, or
// the field doesn't exist, or the field is not and integer, should return an
// err. Note that when you create a HeapFile, you will need to supply a file
// name;  you can supply a non-existant file, in which case it will be created.
// However, subsequent invocations of this method will result in tuples being
// reinserted into this file unless you delete (e.g., with [os.Remove] it before
// calling NewHeapFile.
func computeFieldSum(fileName string, td TupleDesc, sumField string) (int, error) {
	heapName := "AuthorIsHaibarapink@gmail.com" // tmp filename
	bp := NewBufferPool(100)
	heapFile, err := NewHeapFile(heapName, &td, bp)
	defer os.Remove(heapName)

	if err != nil {
		return 0, err
	}
	if heapFile == nil {
		return 0, err
	}

	csvFile, err := os.Open(fileName)
	if err != nil {
		return 0, err
	}
	defer csvFile.Close()
	err = heapFile.LoadFromCSV(csvFile, true, ",", false)
	if err != nil {
		return 0, err
	}
	var tmp_tid int = -1
	iter, err := heapFile.Iterator(&tmp_tid)
	if err != nil {
		return 0, err
	}

	var sum = 0
	var desc = heapFile.Descriptor()
	// find field index
	var fieldIndex = -1
	for i := 0; i < len(desc.Fields); i++ {
		if desc.Fields[i].Fname == sumField {
			fieldIndex = i
			break
		}
	}
	if fieldIndex == -1 {
		return 0, err
	}
	for {
		tuple, err := iter()
		if err != nil {
			return 0, err
		}
		if tuple == nil {
			break
		}
		// get field
		var field = tuple.Fields[fieldIndex]
		var value = field.(IntField).Value
		sum = sum + int(value)
	}
	return sum, nil // replace me
}
