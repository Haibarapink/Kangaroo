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
	return &LockManager{make(map[int]ReqLockType)}
}

func (mgr *LockManager) AcquireLock(tid TransactionID, pageNo int, perm RWPerm) bool {
	req, ok := mgr.reqMap[pageNo]
	if !ok || req.Perm == ReadPerm && perm == ReadPerm {
		if !ok {
			req = ReqLockType{}
			req.Perm = perm
		}
		req.Tid = append(req.Tid, tid)
		if req.Perm == ReadPerm {
			req.RdCnt++
		}
		mgr.reqMap[pageNo] = req
		return true
	}
	return false
}

func (mgr *LockManager) ReleaseLock(tid TransactionID, pageNo int) {
	req, ok := mgr.reqMap[pageNo]
	if !ok {
		return
	}
	ok = false
	p := 0
	for i, t := range req.Tid {
		if t == tid {
			p = i
			ok = true
			break
		}
	}

	if !ok {
		panic("ReleaseLock fail because tid not existing")
	}

	delete(mgr.reqMap, pageNo)

	if req.Perm == ReadPerm {
		req.RdCnt--
		if req.RdCnt != 0 {
			req.Tid = append(req.Tid[:p], req.Tid[p+1:]...)
			mgr.reqMap[pageNo] = req
		}
	}
}
