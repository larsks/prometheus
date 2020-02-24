// Copyright 2020 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"sync"
)

// IsolationState holds the isolation information.
type IsolationState struct {
	// We will ignore all writes above the max, or that are incomplete.
	maxWriteID       uint64
	incompleteWrites map[uint64]struct{}
	lowWaterMark     uint64 // Lowest of incompleteWrites/maxWriteId.
	isolation        *isolation

	// Doubly linked list of active reads.
	next *IsolationState
	prev *IsolationState
}

// Close closes the state.
func (i *IsolationState) Close() {
	i.isolation.readMtx.Lock()
	defer i.isolation.readMtx.Unlock()
	i.next.prev = i.prev
	i.prev.next = i.next
}

// isolation is the global isolation state.
type isolation struct {
	// Mutex for accessing lastWriteId and writesOpen.
	writeMtx sync.Mutex
	// Each write is given an internal id.
	lastWriteID uint64
	// Which writes are currently in progress.
	writesOpen map[uint64]struct{}
	// Mutex for accessing readsOpen.
	// If taking both writeMtx and readMtx, take writeMtx first.
	readMtx sync.Mutex
	// All current in use isolationStates. This is a doubly-linked list.
	readsOpen *IsolationState
}

func newIsolation() *isolation {
	isoState := &IsolationState{}
	isoState.next = isoState
	isoState.prev = isoState

	return &isolation{
		writesOpen: map[uint64]struct{}{},
		readsOpen:  isoState,
	}
}

// lowWatermark returns the writeId below which
// we no longer need to track which writes were from
// which writeId.
func (i *isolation) lowWatermark() uint64 {
	i.writeMtx.Lock() // Take writeMtx first.
	defer i.writeMtx.Unlock()
	i.readMtx.Lock()
	defer i.readMtx.Unlock()
	if i.readsOpen.prev == i.readsOpen {
		return i.lastWriteID
	}
	return i.readsOpen.prev.lowWaterMark
}

// State returns an object used to control isolation
// between a query and writes. Must be closed when complete.
func (i *isolation) State() *IsolationState {
	i.writeMtx.Lock() // Take write mutex before read mutex.
	defer i.writeMtx.Unlock()
	isoState := &IsolationState{
		maxWriteID:       i.lastWriteID,
		lowWaterMark:     i.lastWriteID,
		incompleteWrites: make(map[uint64]struct{}, len(i.writesOpen)),
		isolation:        i,
	}
	for k := range i.writesOpen {
		isoState.incompleteWrites[k] = struct{}{}
		if k < isoState.lowWaterMark {
			isoState.lowWaterMark = k
		}
	}

	i.readMtx.Lock()
	defer i.readMtx.Unlock()
	isoState.prev = i.readsOpen
	isoState.next = i.readsOpen.next
	i.readsOpen.next.prev = isoState
	i.readsOpen.next = isoState
	return isoState
}

// newWriteID increments the transaction counter and returns a new transaction ID.
func (i *isolation) newWriteID() uint64 {
	i.writeMtx.Lock()
	defer i.writeMtx.Unlock()
	i.lastWriteID++
	i.writesOpen[i.lastWriteID] = struct{}{}
	return i.lastWriteID
}

func (i *isolation) closeWrite(writeID uint64) {
	i.writeMtx.Lock()
	defer i.writeMtx.Unlock()
	delete(i.writesOpen, writeID)
}

// The transactionID ring buffer.
type txRing struct {
	txIDs     []uint64
	txIDFirst int // Position of the first id in the ring.
	txIDCount int // How many ids in the ring.
}

func newTxRing(cap int) *txRing {
	return &txRing{
		txIDs: make([]uint64, cap),
	}
}

func (txr *txRing) add(writeID uint64) {
	if txr.txIDCount == len(txr.txIDs) {
		// Ring buffer is full, expand by doubling.
		newRing := make([]uint64, txr.txIDCount*2)
		idx := copy(newRing[:], txr.txIDs[txr.txIDFirst:])
		copy(newRing[idx:], txr.txIDs[:txr.txIDFirst])
		txr.txIDs = newRing
		txr.txIDFirst = 0
	}

	txr.txIDs[(txr.txIDFirst+txr.txIDCount)%len(txr.txIDs)] = writeID
	txr.txIDCount++
}

func (txr *txRing) cleanupWriteIDsBelow(bound uint64) {
	pos := txr.txIDFirst

	for txr.txIDCount > 0 {
		if txr.txIDs[pos] < bound {
			txr.txIDFirst++
			txr.txIDCount--
		} else {
			break
		}

		pos++
		if pos == len(txr.txIDs) {
			pos = 0
		}
	}

	txr.txIDFirst %= len(txr.txIDs)
}

func (txr *txRing) iterator() *txRingIterator {
	return &txRingIterator{
		pos: txr.txIDFirst,
		ids: txr.txIDs,
	}
}

// txRingIterator lets you iterate over the ring. It doesn't terminate,
// it DOESNT terminate.
type txRingIterator struct {
	ids []uint64

	pos int
}

func (it *txRingIterator) At() uint64 {
	return it.ids[it.pos]
}

func (it *txRingIterator) Next() {
	it.pos++
	if it.pos == len(it.ids) {
		it.pos = 0
	}
}
