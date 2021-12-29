package dmap

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sbromberger/dmap/safemap"
	mpi "github.com/sbromberger/gompi"
)

const MAXQUEUESIZE = 1024 * 1024 // 1 million messages

type MsgType uint8

const (
	MsgGet MsgType = iota // Get
	MsgSet                // Set
)

// Message is the unit of communication between ranks.
type Message[K safemap.KeyType, V safemap.ValType] struct {
	Type MsgType
	Key  K
	Val  V
}

// messageQueue is just a slice of messages.
type messageQueue[K safemap.KeyType, V safemap.ValType] []Message[K, V]

// safeQueue implements a lockable messageQueue.
// We embed the mutex here since this struct is not exported.
type safeQueue[K safemap.KeyType, V safemap.ValType] struct {
	q messageQueue[K, V]
	sync.RWMutex
}

// DMap represents one piece of a distributed map.
type DMap[K safemap.KeyType, V safemap.ValType] struct {
	o                  *mpi.Communicator
	Map                *safemap.SafeMap[K, V]
	myRank             int
	Inbox              chan Message[K, V]
	msgsSent, msgsRecv uint64 // do not access these directly; they're atomics.
	sendQs             map[int]*safeQueue[K, V]
	totalQSize         uint64 // do not access these directly; they're atomics.
}

// shouldFlush returns true if the queue size is greater than MAXQUEUESIZE
func (d *DMap[K, V]) shouldFlush() bool {
	return atomic.LoadUint64(&d.totalQSize) > MAXQUEUESIZE
}

// flushSend sends all sendQueues and flushes the queues.
func (d *DMap[K, V]) flushSend() {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	for dest, sq := range d.sendQs {
		sq.Lock()
		err := enc.Encode(sq.q)

		if err != nil {
			fmt.Printf("ERROR IN ENCODE: %v", err)
			log.Fatal("error in encode: ", err)
		}
		d.o.SendBytes(b.Bytes(), dest, 0)
		lenSq := uint64(len(sq.q))
		if lenSq > 0 {
			// fmt.Printf("%d: adding %d messages to sent\n", d.myRank, lenSq)
			atomic.AddUint64(&d.msgsSent, lenSq)
			// fmt.Printf("%d: msgsSent now %d\n", d.myRank, atomic.LoadUint64(&d.msgsSent))
		}
		b.Reset()
		sq.q = sq.q[:0]
		sq.Unlock()
	}
	// fmt.Printf("%d: called flushSend = sent %d msgs\n", d.myRank, ttlmsgs)
	atomic.StoreUint64(&d.totalQSize, uint64(0))
}

// New returns a pointer to a newly-created distributed map.
func New[K safemap.KeyType, V safemap.ValType](o *mpi.Communicator, chansize int) *DMap[K, V] {
	r := o.Rank()
	inbox := make(chan Message[K, V], chansize)
	sm := safemap.New[K, V]()

	sendQs := make(map[int]*safeQueue[K, V])
	dm := DMap[K, V]{o: o, Map: sm, myRank: r, Inbox: inbox, sendQs: sendQs}
	go recv(&dm)
	gob.Register(messageQueue[K, V]{})
	return &dm
}

// recv is a goroutine that performs asynchronous message dispatch.
func recv[K safemap.KeyType, V safemap.ValType](dmap *DMap[K, V]) {
	defer close(dmap.Inbox)
	// defer fmt.Printf("%d: recv terminating\n", dmap.myRank)
	for {
		recvbytes, status := dmap.o.MrecvBytes(mpi.AnySource, mpi.AnyTag)
		tag := status.GetTag()
		if tag == dmap.o.MaxTag {
			// fmt.Printf("%d: received maxtag\n", dmap.myRank)
			return
		}
		b := bytes.NewReader(recvbytes)
		dec := gob.NewDecoder(b)
		var messageQ messageQueue[K, V]
		if err := dec.Decode(&messageQ); err != nil {
			fmt.Printf("ERROR IN DECODE: %v", err)
			log.Fatal("decode error: ", err)
		}
		lenQ := len(messageQ)
		// fmt.Printf("%d: msgsRecv starting at %d\n", dmap.myRank, atomic.LoadUint64(&dmap.msgsRecv))
		for _, msg := range messageQ {
			switch msg.Type {
			case MsgGet:
				// fmt.Printf("%d: received get %v\n", dmap.myRank, rmsg.Key)
				dmap.Inbox <- msg
			case MsgSet:
				// fmt.Printf("%d: received set %v -> %v\n", dmap.myRank, msg.Key, msg.Val)
				dmap.Map.Set(msg.Key, msg.Val)
			default:
				log.Fatal("invalid message type: ", tag)
			}
		}
		if lenQ > 0 {
			// fmt.Printf("adding %d messages to msgsrecv\n", lenQ)
			atomic.AddUint64(&dmap.msgsRecv, uint64(lenQ))
			// fmt.Printf("%d: msgsRecv now %d\n", dmap.myRank, atomic.LoadUint64(&dmap.msgsRecv))
		}
	}
}

// queueMsg adds a message to a given sendQueue.
func queueMsg[K safemap.KeyType, V safemap.ValType](d *DMap[K, V], msg Message[K, V], dest int) {
	// fmt.Printf("%d: encoded message %v is %v; sending to %d\n", d.o.Rank(), *msg, encoded, dest)
	// fmt.Printf("%d: in queueMsg with msg %v\n", d.myRank, msg)
	sq, found := d.sendQs[dest]
	if !found {
		// fmt.Printf("%d: creating new sendQueue for dest %d\n", d.myRank, dest)
		mq := messageQueue[K, V]{}
		d.sendQs[dest] = new(safeQueue[K, V])
		d.sendQs[dest].q = mq
		sq = d.sendQs[dest]
	}
	// fmt.Printf("%d: sq = %v\n", d.myRank, sq)
	// fmt.Printf("%d: sendQs = %v\n", d.myRank, sq.q)
	sq.Lock()
	// fmt.Printf("%d: locked\n", d.myRank)
	sq.q = append(sq.q, msg)
	// fmt.Printf("%d: appended\n", d.myRank)
	sq.Unlock()
	atomic.AddUint64(&d.totalQSize, 1)
	// fmt.Printf("%d: queued msg %v\n", d.myRank, msg)
	if d.shouldFlush() {
		// fmt.Printf("%d: flushing\n", d.myRank)
		d.flushSend()
	}
}

// Get retrieves a value from a distributed map along with a boolean indicating whether
// the key existed.
func (m *DMap[K, V]) Get(k K) (V, bool) {
	dest := k.Hash() % m.o.Size()
	if dest == m.myRank {
		val, found := m.Map.Get(k)
		return val, found
	}
	msg := Message[K, V]{Type: MsgGet, Key: k}
	queueMsg(m, msg, dest)
	rmsg := <-m.Inbox
	if rmsg.Val.Empty() {
		var val V
		return val, false
	}

	return rmsg.Val, true
}

// Set sets the value for a key anywhere within the distributed map.
func (m *DMap[K, V]) Set(k K, v V) {
	dest := k.Hash() % m.o.Size()
	if dest == m.myRank {
		m.Map.Set(k, v)
		return
	}
	msg := Message[K, V]{Type: MsgSet, Key: k, Val: v}
	queueMsg(m, msg, dest)
	return
}

// Barrier performs a block and an explicit message flush.
func (m *DMap[K, V]) Barrier() {
	globalCt := make([]uint64, 2)
	localCt := make([]uint64, 2)

	lastsent, lastrecv := uint64(1), uint64(1)
	for (globalCt[0] != globalCt[1]) || (globalCt[0] != lastsent) || (globalCt[1] != lastrecv) {
		m.flushSend()
		m.o.Barrier()
		lastsent, lastrecv = globalCt[0], globalCt[1]
		localCt[0], localCt[1] = m.GetMsgCount()
		m.o.AllreduceUint64s(globalCt, localCt, mpi.OpSum, 0)
		time.Sleep(10 * time.Millisecond)
	}
}

// LocalSize returns the number of entries in the local component
// of a distributed map.
func (m *DMap[K, V]) LocalSize() uint64 {
	return m.Map.Size()
}

// Stop is required in order to cleanly shut down the distributed map.
// Failure to call Stop will likely lead to an MPI crash.
func (m *DMap[K, V]) Stop() {
	m.Barrier()
	m.o.SendString("q", m.myRank, m.o.MaxTag)
	// Wait for the channel to be closed - this signals that the queue
	// is clear and the goroutine is exiting.
	<-m.Inbox
}

// GetMsgCount returns the number of messages sent and received locally.
func (m *DMap[K, V]) GetMsgCount() (uint64, uint64) {
	return atomic.LoadUint64(&m.msgsSent), atomic.LoadUint64(&m.msgsRecv)
}
