package godb

type ReqLockType struct {
	Tid  TransactionID
	Perm RWPerm
}

func NewReqLockType(tid TransactionID, perm RWPerm) *ReqLockType {
	return &ReqLockType{tid, perm}
}

type LockManager struct {
	// request map
	reqMap map[int]ReqLockType
}

func (mgr *LockManager) AcquireFor(tid TransactionID, perm RWPerm) bool {
	return false
}
