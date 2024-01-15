package godb

type TransactionID *int

var nextTid = 0

func NewTID() TransactionID {
	id := nextTid
	nextTid++
	return &id
}

//var tid TransactionID = NewTID()
