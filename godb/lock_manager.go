package godb

type ReqLockType struct {
	Tid   []TransactionID
	Perm  RWPerm
	RdCnt int
}

type LockManager struct {
	// request map
	reqMap map[any]ReqLockType
}

func NewLockManager() *LockManager {
	return &LockManager{make(map[any]ReqLockType)}
}

// handle updating lock and reacquiring lock
func (mgr *LockManager) handleReacquireLock(tid TransactionID, pageKey any, tidsLen int, oldPerm RWPerm, newPerm RWPerm) bool {
	if oldPerm == ReadPerm && newPerm == WritePerm {
		// update logic lock
		if tidsLen > 1 {
			return false
		}
		newReq := ReqLockType{}
		newReq.Perm = WritePerm
		newReq.Tid = append(newReq.Tid, tid)
		delete(mgr.reqMap, pageKey)
		mgr.reqMap[pageKey] = newReq
		return true
	} else {
		return true
	}
}

func (mgr *LockManager) AcquireLock(tid TransactionID, pageKey any, perm RWPerm) bool {
	req, ok := mgr.reqMap[pageKey]
	if !ok || req.Perm == ReadPerm && perm == ReadPerm {
		if !ok {
			req = ReqLockType{}
			req.Perm = perm
		}
		if ok {
			tidsLen := len(req.Tid)
			for _, reqTid := range req.Tid {
				if reqTid == tid {
					return mgr.handleReacquireLock(tid, pageKey, tidsLen, req.Perm, perm)
				}
			}
		}
		req.Tid = append(req.Tid, tid)
		if req.Perm == ReadPerm {
			req.RdCnt++
		}
		mgr.reqMap[pageKey] = req
		return true
	}
	return false
}

func (mgr *LockManager) ReleaseLock(tid TransactionID, pageKey any) {
	req, ok := mgr.reqMap[pageKey]
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

	delete(mgr.reqMap, pageKey)

	if req.Perm == ReadPerm {
		req.RdCnt--
		if req.RdCnt != 0 {
			req.Tid = append(req.Tid[:p], req.Tid[p+1:]...)
			mgr.reqMap[pageKey] = req
		}
	}
}
