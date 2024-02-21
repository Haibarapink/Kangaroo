package godb

import (
	"bytes"
	"encoding/binary"
	"errors"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	desc         *TupleDesc
	tuples       []Tuple
	deleted      []bool // mark deleted tuples
	numSlots     int
	numUsedSlots int

	singleTupleSize int
	pageId          int

	dirty bool
	file  *HeapFile
}

func (h *heapPage) UpdateSlotNumAndSingleTupleSize() int {
	var remPageSize int = PageSize - 8 // bytes after header
	var bytesPerTuple int = h.desc.Size()
	var res int = remPageSize / (bytesPerTuple)
	h.numSlots = res
	h.singleTupleSize = bytesPerTuple
	return res
}

// Construct a new heap page
// read from file if pageNo is valid else is create a new page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) *heapPage {
	var res = new(heapPage)
	res.desc = desc
	res.UpdateSlotNumAndSingleTupleSize()
	res.numUsedSlots = 0
	res.pageId = pageNo

	res.dirty = false
	res.file = f

	// read from file if pageNo is valid else is create a new page
	if pageNo < f.NumPages() {
		var buf = bytes.Buffer{}
		buf.Grow(PageSize)
		f.file.Seek(int64(pageNo*PageSize), 0)
		var _, err = buf.ReadFrom(f.file)
		if err != nil {
			return nil
		}
		res.initFromBuffer(&buf)
	} else {
		f.file.Truncate(int64((pageNo + 1) * PageSize))
	}

	return res
}

func (h *heapPage) getNumSlots() int {
	return h.numSlots
}

func (h *heapPage) headerSize() int {
	return 8
}

func (h *heapPage) spaceUsed() int {
	return h.numUsedSlots*h.singleTupleSize + h.headerSize()
}

func (h *heapPage) tupleStartPos(slot int) int {
	return h.headerSize() + slot*h.singleTupleSize
}

// TODO test
func (h *heapPage) fetchTuple(slotIdx int) (*Tuple, error) {
	if slotIdx >= h.numUsedSlots {
		return nil, errors.New("invalid slot idx")
	}
	if h.deleted[slotIdx] {
		return nil, errors.New("tuple deleted")
	}
	return &h.tuples[slotIdx], nil
}

// TODO test
// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	if h.numUsedSlots >= h.numSlots {
		return Rid{}, errors.New("no free slots")
	}

	var rid Rid

	rid.PageNo = h.pageId
	rid.SlotNo = h.numUsedSlots
	h.numUsedSlots++
	t.Desc = *h.desc
	h.tuples = append(h.tuples, *t)
	h.deleted = append(h.deleted, false)
	return rid, nil
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (h *heapPage) deleteTuple(rid recordID) error {
	var r = rid.(Rid)
	if r.PageNo != h.pageId {
		return errors.New("invalid page no")
	}
	if r.SlotNo >= len(h.tuples) {
		return errors.New("invalid slot no")
	}
	if h.deleted[r.SlotNo] {
		return errors.New("tuple already deleted")
	}
	h.deleted[rid.(Rid).SlotNo] = true
	h.numUsedSlots--
	return nil
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	return h.dirty
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(dirty bool) {
	h.dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() *DBFile {
	var dbFile DBFile = p.file
	return &dbFile
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	var buf = new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(h.numSlots))
	binary.Write(buf, binary.LittleEndian, int32(h.numUsedSlots))
	for i, tuple := range h.tuples {
		if !h.deleted[i] {
			tuple.writeTo(buf)
		}
	}
	return buf, nil
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	var numSlots int32
	var numUsedSlots int32
	binary.Read(buf, binary.LittleEndian, &numSlots)
	binary.Read(buf, binary.LittleEndian, &numUsedSlots)
	h.numSlots = int(numSlots)
	h.numUsedSlots = int(numUsedSlots)
	for i := 0; i < h.numUsedSlots; i++ {
		t, err := readTupleFrom(buf, h.desc)
		if err != nil {
			return err
		}
		h.tuples = append(h.tuples, *t)
		h.deleted = append(h.deleted, false)
	}
	return nil
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	var i int = 0

	return func() (*Tuple, error) {
		for i < len(p.tuples) && p.deleted[i] {
			i++
		}
		if i >= len(p.tuples) {
			return nil, nil
		}
		var res = &p.tuples[i]
		res.Rid = Rid{p.pageId, i}
		i++
		return res, nil
	}
}
