package godb

type ReqLockType struct {
	Tid   []TransactionID
	Perm  RWPerm
	RdCnt int
}

type LockManager struct {
	// request map
	reqMap map[int]ReqLockType
}

func NewLockManager() *LockManager {
	return &LockManager{}
}

func (mgr *LockManager) AcquireLock(tid TransactionID, pageNo int, perm RWPerm) bool {
	req, ok := mgr.reqMap[pageNo]
	if !ok || req.Perm == ReadPerm && perm == ReadPerm {
		if !ok {
			req = ReqLockType{}
		}
		req.Tid = append(req.Tid, tid)
		req.RdCnt++
		return true
	}
	return false
}

func (mgr *LockManager) ReleaseLock(tid TransactionID, pageNo int) {
	req, ok := mgr.reqMap[pageNo]
	if !ok {
		return
	}
	delete(mgr.reqMap, pageNo)

	if req.Perm == ReadPerm {
		req.RdCnt--
		if req.RdCnt != 0 {
			mgr.reqMap[pageNo] = req
		}
	}
}
