package godb

import (
	"container/list"
	"sync"
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
	// for lab1
	fr.data.PushBack(e.Value.(int))
	return e.Value.(int), nil
}

// 目前 bug是在于，每次都是新建一个page，而不是从缓存中取出来， 明天改
type BufferPool struct {
	pages []Page
	// pageid to frameid
	coord map[any]int

	free_list list.List
	// replacer
	replacer Replacer

	// sync
	mu sync.Mutex
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) *BufferPool {
	var bp = BufferPool{}

	bp.pages = make([]Page, numPages)
	bp.coord = make(map[any]int)
	bp.replacer = NewFifoReplacer(numPages)

	for i := 0; i < numPages; i++ {
		bp.free_list.PushBack(i)
		bp.replacer.touch(i)
	}

	return &bp
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	for i := 0; i < len(bp.pages); i++ {
		if bp.pages[i] != nil && bp.pages[i].isDirty() {
			file := bp.pages[i].getFile()
			(*file).flushPage(&bp.pages[i])
			bp.pages[i].setDirty(false)
		}
	}
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtired will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	// TODO: some code goes here
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	// TODO: some code goes here
}

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	return nil
}

func (bp *BufferPool) changeCoord(file DBFile, pageId int, frameNo int) {
	// old
	oldPage := bp.pages[frameNo]
	// defending codes
	if oldPage != nil {
		// old page id
		oldPageId := oldPage.(*heapPage).pageId
		// delete old
		delete(bp.coord, (*oldPage.getFile()).pageKey(oldPageId))
	}

	bp.coord[file.pageKey(pageId)] = frameNo
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. For lab 1, you do not need to
// implement locking or deadlock detection. [For future labs, before returning the page,
// attempt to lock it with the specified permission. If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock]. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	fid, ok := bp.coord[file.pageKey(pageNo)]
	// not only pid , but also file is same
	if ok && (*bp.pages[fid].getFile()) == file {
		return &bp.pages[fid], nil
	}

	if bp.free_list.Len() > 0 {
		backElement := bp.free_list.Back()
		fid = backElement.Value.(int)
		bp.free_list.Remove(backElement)

		bp.changeCoord(file, pageNo, fid)
		bp.replacer.touch(fid)

		pg, err := file.readPage(pageNo)
		if err != nil {
			return nil, err
		}
		(*pg).(*heapPage).pageId = pageNo
		bp.pages[fid] = *pg

		return &bp.pages[fid], nil
	}

	// read
	fid, err := bp.replacer.evict()
	if err != nil {
		return nil, err
	}

	if bp.pages[fid].isDirty() {
		// flush to disk
		pgFile := *bp.pages[fid].getFile()
		err := pgFile.flushPage(&bp.pages[fid])
		if err != nil {
			return nil, err
		}
	}
	// must be the first step
	bp.changeCoord(file, pageNo, fid)

	pg, err := file.readPage(pageNo)
	if err != nil {
		return nil, err
	}
	bp.pages[fid] = *pg

	return &bp.pages[fid], nil

}

// New a page
func (bp *BufferPool) NewPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
	fid, ok := bp.coord[file.pageKey(pageNo)]
	// not only pid , but also file is same
	if ok && (*bp.pages[fid].getFile()) == file {
		return &bp.pages[fid], nil
	}

	if bp.free_list.Len() > 0 {
		backElement := bp.free_list.Back()
		fid = backElement.Value.(int)
		bp.free_list.Remove(backElement)

		pg := newHeapPage(file.Descriptor(), pageNo, file.(*HeapFile))

		bp.pages[fid] = pg
		bp.changeCoord(file, pageNo, fid)
		return &bp.pages[fid], nil
	}

	// read
	fid, err := bp.replacer.evict()
	if err != nil {
		return nil, err
	}

	if bp.pages[fid].isDirty() {
		// flush to disk
		pgFile := *bp.pages[fid].getFile()
		err := pgFile.flushPage(&bp.pages[fid])
		if err != nil {
			return nil, err
		}
	}

	// must be first
	bp.changeCoord(file, pageNo, fid)

	pg := newHeapPage(file.Descriptor(), pageNo, file.(*HeapFile))
	if err != nil {
		return nil, err
	}
	bp.pages[fid] = pg

	return &bp.pages[fid], nil
}
