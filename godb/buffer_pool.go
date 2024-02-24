package godb

import (
	"container/list"
	"sync"
	"time"
)

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

// replacer interface
type Replacer interface {
	touch(pageNo int)
	evict() (int, error)
}

type FifoReplacer struct {
	data *list.List
}

func NewFifoReplacer(num int) *FifoReplacer {
	var fr = FifoReplacer{}
	fr.data = list.New()
	return &fr
}

func (fr *FifoReplacer) touch(fid int) {
	for e := fr.data.Front(); e != nil; e = e.Next() {
		if e.Value == fid {
			fr.data.Remove(e)
			break
		}
	}
	fr.data.PushBack(fid)
}

func (fr *FifoReplacer) evict() (int, error) {
	if fr.data.Len() == 0 {
		return 0, GoDBError{BufferPoolFullError, "Can't evict from replacer which is empty"}
	}
	e := fr.data.Front()
	fr.data.Remove(e)
	return e.Value.(int), nil
}

type FetchedPageType struct {
	Pid  int
	Perm RWPerm
	File DBFile
}

type BufferPool struct {
	pages []Page
	// pageid to frameid
	corr map[any]int

	freeList list.List
	// replacer
	replacer Replacer

	// pin
	pin map[any]int

	// sync
	mu  sync.Mutex
	mgr *LockManager
	// transaction
	tranFetchedPid map[TransactionID]*[]FetchedPageType

	g Graph
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) *BufferPool {
	var bp = BufferPool{}

	bp.pages = make([]Page, numPages)
	bp.corr = make(map[any]int)
	bp.replacer = NewFifoReplacer(numPages)
	bp.mgr = NewLockManager()
	bp.pin = make(map[any]int)

	bp.tranFetchedPid = make(map[TransactionID]*[]FetchedPageType)
	bp.g = NewGraph()
	for i := 0; i < numPages; i++ {
		bp.freeList.PushBack(i)
	}

	go func() {
		for {
			time.Sleep(1000)
			res := bp.g.CheckCycle()
			if res != nil {
				//bp.g.Print()
				bp.AbortTransaction(res)
			}
		}
	}()

	return &bp
}

func (bp *BufferPool) Pin(pageKey any) {
	cnt, ok := bp.pin[pageKey]
	if !ok {
		cnt = 0
	}
	cnt += 1
	bp.pin[pageKey] = cnt
}

func (bp *BufferPool) Unpin(pageKey any) {
	cnt, ok := bp.pin[pageKey]
	if !ok || cnt == 0 {
		return
	}

	cnt -= 1
	if cnt == 0 {
		// replacer touch it
		fid, ok := bp.corr[pageKey]
		if !ok {
			return
		}
		if bp.pages[fid].isDirty() == false {
			bp.replacer.touch(fid)
		}
	}
	bp.pin[pageKey] = cnt
	return
}

func (bp *BufferPool) FlushAllPages() {
	for i := 0; i < len(bp.pages); i++ {
		if bp.pages[i] != nil && bp.pages[i].isDirty() {
			file := bp.pages[i].getFile()
			(*file).flushPage(&bp.pages[i])
			bp.pages[i].setDirty(false)
		}
	}
}

func (bp *BufferPool) RemoveFromLockMgr(tid TransactionID, p Page) {
	pid := p.(*heapPage).pageId
	key := (*p.getFile()).pageKey(pid)
	bp.mgr.ReleaseLock(tid, key)
}

func (bp *BufferPool) releasePageLock(tid TransactionID, forceWrite bool) {
	pidList, ok := bp.tranFetchedPid[tid]
	if !ok {
		return
	}
	bp.g.RemoveVex(tid)
	for _, val := range *pidList {
		file := val.File
		pid := val.Pid
		perm := val.Perm
		key := file.pageKey(pid)
		fid, ok := bp.corr[key]
		if !ok {
			continue
		}
		if perm == WritePerm {
			if forceWrite {
				// fetch page first
				page := bp.pages[fid]
				if page.isDirty() {
					err := file.flushPage(&page)
					if err != nil {
						panic("should not fail(assumed by lab document")
					}
				}
				page.setDirty(false)
			}
			bp.replacer.touch(fid)
		}
		bp.mgr.ReleaseLock(tid, key)
		bp.Unpin(key)
	}

}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtired will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.releasePageLock(tid, false)

	// reread dirty page
	pidList := bp.tranFetchedPid[tid]
	for _, val := range *pidList {
		pid := val.Pid
		file := val.File
		key := file.pageKey(pid)
		fid, ok := bp.corr[key]
		if !ok {
			panic("fid should exist")
		}
		page, err := file.readPage(pid)
		hf := file.(*HeapFile)
		if err != nil {
			if pid < (*hf).NumPages() {
				panic("reading page shouldn't fail")
			} else {
				// this is new page
				page = hf.AllocPage(pid)
			}
		}
		bp.replacer.touch(fid)
		bp.pages[fid] = *page
	}

	delete(bp.tranFetchedPid, tid)
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.releasePageLock(tid, true)
	delete(bp.tranFetchedPid, tid)
}

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	list := make([]FetchedPageType, 0)
	bp.tranFetchedPid[tid] = &list
	return nil
}

func (bp *BufferPool) changeCorrespond(file DBFile, pageId int, frameNo int) {
	// old
	oldPage := bp.pages[frameNo]
	// defending codes
	if oldPage != nil {
		// old page id
		oldPageId := oldPage.(*heapPage).pageId
		// delete old
		delete(bp.corr, (*oldPage.getFile()).pageKey(oldPageId))
	}

	bp.corr[file.pageKey(pageId)] = frameNo
}

func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// try fetching lock from lock manager
	key := file.pageKey(pageNo)
	fetchLockOk := false
	for fetchLockOk == false {
		ch, fetchLockOk := bp.mgr.AcquireLock(tid, key, perm)
		if fetchLockOk {
			// ok
			//fmt.Println(*tid, " sc ", perm)
			break
		}

		for _, to := range bp.mgr.reqMap[key].Tid {
			bp.g.AddEdge(tid, to)
		}

		// otherwise
		// block current thread
		//fmt.Println(*tid, " fa ", perm)
		bp.mu.Unlock()
		<-*ch
		bp.mu.Lock()
	}
	bp.Pin(key)
	// get page lock successfully
	currTidPageFetchedList, ok := bp.tranFetchedPid[tid]
	if ok {
		// with transaction
		*currTidPageFetchedList = append(*currTidPageFetchedList, FetchedPageType{pageNo, perm, file})
	}

	fid, ok := bp.corr[file.pageKey(pageNo)]
	// not only pid , but also file is same
	if ok {
		return &bp.pages[fid], nil
	}

	if bp.freeList.Len() > 0 {
		backElement := bp.freeList.Back()
		fid = backElement.Value.(int)
		bp.freeList.Remove(backElement)

		bp.changeCorrespond(file, pageNo, fid)

		pg, err := file.readPage(pageNo)
		if err != nil {
			bp.Unpin(key)
			return nil, err
		}
		(*pg).(*heapPage).pageId = pageNo
		bp.pages[fid] = *pg

		return &bp.pages[fid], nil
	}

	// read
	fid, err := bp.replacer.evict()
	if err != nil {
		bp.Unpin(key)
		return nil, err
	}

	if bp.pages[fid].isDirty() {
		panic("no steal")
	}
	// must be the first step
	bp.changeCorrespond(file, pageNo, fid)

	pg, err := file.readPage(pageNo)
	if err != nil {
		bp.Unpin(key)
		return nil, err
	}
	bp.pages[fid] = *pg

	return &bp.pages[fid], nil

}

// New a page
func (bp *BufferPool) NewPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// try fetching lock from lock manager
	fetchLockOk := false
	key := file.pageKey(pageNo)
	for !fetchLockOk {
		ch, fetchLockOk := bp.mgr.AcquireLock(tid, key, perm)
		if fetchLockOk {
			// ok
			//fmt.Println(*tid, " sc ", perm)
			break
		}
		for _, to := range bp.mgr.reqMap[key].Tid {
			bp.g.AddEdge(tid, to)
		}
		// otherwise
		// block current thread
		bp.mu.Unlock()
		<-*ch
		bp.mu.Lock()
	}
	bp.Pin(key)

	// get page successfully
	currTidPageFetchedList := bp.tranFetchedPid[tid]
	if currTidPageFetchedList != nil {
		*currTidPageFetchedList = append(*currTidPageFetchedList, FetchedPageType{pageNo, perm, file})
	}
	fid, ok := bp.corr[key]
	// not only pid , but also file is same
	if ok && (*bp.pages[fid].getFile()) == file {
		return &bp.pages[fid], nil
	}

	if bp.freeList.Len() > 0 {
		backElement := bp.freeList.Back()
		fid = backElement.Value.(int)
		bp.freeList.Remove(backElement)

		pg := newHeapPage(file.Descriptor(), pageNo, file.(*HeapFile))

		bp.pages[fid] = pg
		bp.changeCorrespond(file, pageNo, fid)
		return &bp.pages[fid], nil
	}

	// read
	fid, err := bp.replacer.evict()
	if err != nil {
		bp.Unpin(key)
		return nil, err
	}

	if bp.pages[fid].isDirty() {
		panic("no steal")
	}

	// must be first
	bp.changeCorrespond(file, pageNo, fid)

	pg := newHeapPage(file.Descriptor(), pageNo, file.(*HeapFile))
	if err != nil {
		bp.Unpin(key)
		return nil, err
	}
	bp.pages[fid] = pg

	return &bp.pages[fid], nil
}
