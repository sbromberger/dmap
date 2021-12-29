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

type KeyType safemap.KeyType
type ValType safemap.ValType

const (
	MsgGet MsgType = iota // Get
	MsgSet                // Set
)

// Message is the unit of communication between ranks.
type Message[K KeyType, V ValType] struct {
	Type MsgType
	Key  K
	Val  V
}

// messageQueue is just a slice of messages.
type messageQueue[K KeyType, V ValType] []Message[K, V]

// safeQueue implements a lockable messageQueue.
// We embed the mutex here since this struct is not exported.
type safeQueue[K KeyType, V ValType] struct {
	q messageQueue[K, V]
	sync.RWMutex
}

// DMap represents one piece of a distributed map.
type DMap[K KeyType, V ValType] struct {
	o                  *mpi.Communicator
	mp                 *safemap.SafeMap[K, V]
	myRank             int
	inbox              chan Message[K, V]
	msgsSent, msgsRecv uint64 // do not access these directly; they're atomics.
	sendQs             map[int]*safeQueue[K, V]
	totalQSize         uint64 // do not access these directly; they're atomics.
}

// shouldFlush returns true if the queue size is greater than MAXQUEUESIZE
func (m *DMap[K, V]) shouldFlush() bool {
	return atomic.LoadUint64(&m.totalQSize) > MAXQUEUESIZE
}

func (m *DMap[K, V]) String() string {
	return fmt.Sprintf("%s", m.mp.String())
}

// flushAndSend sends all sendQueues and flushes the queues.
func (m *DMap[K, V]) flushAndSend() {
	for dest, sq := range m.sendQs {
		var b bytes.Buffer
		enc := gob.NewEncoder(&b)
		sq.Lock()
		lenSq := uint64(len(sq.q))
		if err := enc.Encode(sq.q); err != nil {
			fmt.Printf("ERROR IN ENCODE: %v\n", err)
			log.Fatal("error in encode: ", err)
		}
		m.o.SendBytes(b.Bytes(), dest, 0)
		sq.q = sq.q[:0]
		sq.Unlock()
		if lenSq > 0 {
			atomic.AddUint64(&m.msgsSent, lenSq)
		}
	}
	atomic.StoreUint64(&m.totalQSize, uint64(0))
}

// New returns a pointer to a newly-created distributed map.
func New[K KeyType, V ValType](o *mpi.Communicator, chansize int) *DMap[K, V] {
	r := o.Rank()
	inbox := make(chan Message[K, V], chansize)
	sm := safemap.New[K, V]()

	sendQs := make(map[int]*safeQueue[K, V])
	m := DMap[K, V]{o: o, mp: sm, myRank: r, inbox: inbox, sendQs: sendQs}
	go recv(&m)
	gob.Register(messageQueue[K, V]{})
	return &m
}

// recv is a goroutine that performs asynchronous message dispatch.
func recv[K KeyType, V ValType](m *DMap[K, V]) {
	defer close(m.inbox)
	// defer fmt.Printf("%d: recv terminating\n", dmap.myRank)
	for {
		recvbytes, status := m.o.MrecvBytes(mpi.AnySource, mpi.AnyTag)
		tag := status.GetTag()
		if tag == m.o.MaxTag {
			// fmt.Printf("%d: received maxtag\n", dmap.myRank)
			return
		}
		b := bytes.NewReader(recvbytes)
		dec := gob.NewDecoder(b)
		var messageQ messageQueue[K, V]
		if err := dec.Decode(&messageQ); err != nil {
			fmt.Printf("%d: ERROR IN DECODE: %v - bytes = %v\n", m.myRank, err, recvbytes)
			log.Fatal("decode error: ", err)
		}
		lenQ := len(messageQ)
		for _, msg := range messageQ {
			switch msg.Type {
			case MsgGet:
				m.inbox <- msg
			case MsgSet:
				m.mp.Set(msg.Key, msg.Val)
			default:
				log.Fatal("invalid message type: ", tag)
			}
		}
		if lenQ > 0 {
			atomic.AddUint64(&m.msgsRecv, uint64(lenQ))
		}
	}
}

// queueMsg adds a message to a given sendQueue.
func queueMsg[K KeyType, V ValType](m *DMap[K, V], msg Message[K, V], dest int) {
	// fmt.Printf("%d: encoded message %v is %v; sending to %d\n", d.o.Rank(), *msg, encoded, dest)
	// fmt.Printf("%d: in queueMsg with msg %v\n", d.myRank, msg)
	sq, found := m.sendQs[dest]
	if !found {
		mq := messageQueue[K, V]{}
		m.sendQs[dest] = new(safeQueue[K, V])
		m.sendQs[dest].q = mq
		sq = m.sendQs[dest]
	}
	sq.Lock()
	sq.q = append(sq.q, msg)
	sq.Unlock()
	atomic.AddUint64(&m.totalQSize, 1)
	if m.shouldFlush() {
		m.flushAndSend()
	}
}

// Get retrieves a value from a distributed map along with a boolean indicating whether
// the key existed.
func (m *DMap[K, V]) Get(k K) (V, bool) {
	dest := k.Hash() % m.o.Size()
	if dest == m.myRank {
		val, found := m.mp.Get(k)
		return val, found
	}
	msg := Message[K, V]{Type: MsgGet, Key: k}
	queueMsg(m, msg, dest)
	rmsg := <-m.inbox
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
		m.mp.Set(k, v)
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
		m.flushAndSend()
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
	return m.mp.Size()
}

// Stop is required in order to cleanly shut down the distributed map.
// Failure to call Stop will likely lead to an MPI crash.
func (m *DMap[K, V]) Stop() {
	m.Barrier()
	m.o.SendString("q", m.myRank, m.o.MaxTag)
	// Wait for the channel to be closed - this signals that the queue
	// is clear and the goroutine is exiting.
	<-m.inbox
}

// GetMsgCount returns the number of messages sent and received locally.
func (m *DMap[K, V]) GetMsgCount() (uint64, uint64) {
	return atomic.LoadUint64(&m.msgsSent), atomic.LoadUint64(&m.msgsRecv)
}
