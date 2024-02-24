package godb

import "fmt"

type ReqLockType struct {
	Tid   []TransactionID
	Perm  RWPerm
	RdCnt int
}

type LockManager struct {
	// request map
	reqMap   map[any]ReqLockType
	wakeChan map[any]*chan bool
}

func NewLockManager() *LockManager {
	return &LockManager{make(map[any]ReqLockType), make(map[any]*chan bool)}
}

func (mgr *LockManager) Print(pageKey any, curReq TransactionID, perm RWPerm) {
	req := mgr.reqMap[pageKey]
	fmt.Print(pageKey, "[ ")
	for _, tid := range req.Tid {
		fmt.Print(*tid, " ")
	}
	fmt.Println(" ] ", *curReq, " ", req.Perm, " ", perm)
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

func (mgr *LockManager) AcquireLock(tid TransactionID, pageKey any, perm RWPerm) (*chan bool, bool) {
	req, ok := mgr.reqMap[pageKey]
	if !ok {
		ch := make(chan bool)
		mgr.wakeChan[pageKey] = &ch
		req = ReqLockType{}
		req.Perm = perm
	}

	// mgr.Print(pageKey, tid, perm)

	if ok {
		tidsLen := len(req.Tid)
		for _, reqTid := range req.Tid {
			if *reqTid == *tid {
				return mgr.wakeChan[pageKey], mgr.handleReacquireLock(tid, pageKey, tidsLen, req.Perm, perm)
			}
		}
	}

	// not same tid
	permOk := req.Perm == perm && perm == ReadPerm
	if !ok || permOk {
		req.Tid = append(req.Tid, tid)
		if req.Perm == ReadPerm {
			req.RdCnt++
		}
		mgr.reqMap[pageKey] = req
		return mgr.wakeChan[pageKey], true
	}
	return mgr.wakeChan[pageKey], false
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
		//panic("ReleaseLock fail because tid not existing")
		return
	}

	delete(mgr.reqMap, pageKey)

	if req.Perm == ReadPerm {
		req.RdCnt--
		if req.RdCnt != 0 {
			req.Tid = append(req.Tid[:p], req.Tid[p+1:]...)
			mgr.reqMap[pageKey] = req
		}
	}
	if (req.Perm == ReadPerm && req.RdCnt == 0) || req.Perm == WritePerm {
		// write to channel
		ch := mgr.wakeChan[pageKey]
		go func() { *ch <- true }()
	}
}

type Graph struct {
	g map[TransactionID]*map[TransactionID]bool
}

func NewGraph() Graph {
	return Graph{make(map[TransactionID]*map[TransactionID]bool)}
}

func (gh *Graph) HasEdge(from TransactionID, to TransactionID) bool {
	e, ok := gh.g[from]
	if !ok {
		return false
	}
	_, ok = (*e)[to]
	if !ok {
		return false
	}
	return true
}

func (gh *Graph) AddEdge(from TransactionID, to TransactionID) {
	if from == to {
		return
	}
	e, ok := gh.g[from]
	if !ok {
		tmp := make(map[TransactionID]bool)
		gh.g[from] = &tmp
		e = &tmp
	}

	(*e)[to] = true
}

func (gh *Graph) RemoveEdge(from TransactionID, to TransactionID) {
	if from == to {
		return
	}
	e, ok := gh.g[from]
	if !ok {
		return
	}
	delete(*e, to)
}

func (gh *Graph) RemoveVex(tid TransactionID) {
	e, ok := gh.g[tid]
	if !ok {
		return
	}
	delete(*e, tid)
	for _, e = range gh.g {
		delete(*e, tid)
	}

}

func (gh *Graph) Dfs(gray *map[TransactionID]bool, cur TransactionID) TransactionID {
	_, existed := (*gray)[cur]
	if existed {
		return cur
	}
	(*gray)[cur] = true
	e, ok := gh.g[cur]
	if !ok {
		return nil
	}
	for to, _ := range *e {
		res := gh.Dfs(gray, to)
		if res != nil {
			return res
		}
	}
	return nil
}

func (gh *Graph) CheckCycle() TransactionID {
	gray := make(map[TransactionID]bool)
	for tid, _ := range gh.g {
		res := gh.Dfs(&gray, tid)
		if res != nil {
			return res
		}
	}
	return nil
}

func (gh *Graph) Print() {
	for tid, ts := range gh.g {
		fmt.Print(*tid, "->|")
		for tid2, _ := range *ts {
			fmt.Print(" ", *tid2, " ")
		}
		fmt.Println(" | ")
	}
}
