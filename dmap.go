package dmap

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	mpi "github.com/sbromberger/gompi"
)

type MsgType uint8

const MAXQUEUESIZE = 1024 * 1024 // 1 million messages
const (
	MsgGet MsgType = iota
	MsgSet
)

type KeyType interface {
	comparable
	Hash() int
}

type ValType interface {
	Empty() bool
}

type Message[K KeyType, V ValType] struct {
	Type MsgType
	Key  K
	Val  V
}

func EncodeMsg[K KeyType, V ValType](msg Message[K, V]) []byte {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(msg)
	if err != nil {
		fmt.Printf("ERROR IN ENCODE: %v", err)
		log.Fatal("error in encode: ", err)
	}
	return b.Bytes()
}

type SafeCounter struct {
	sent uint64
	recv uint64
	sync.RWMutex
}

type SafeMap[K KeyType, V ValType] struct {
	Map map[K]V
	sync.RWMutex
}

type MessageQueue[K KeyType, V ValType] []Message[K, V]

type safeQueue[K KeyType, V ValType] struct {
	q MessageQueue[K, V]
	sync.RWMutex
}

type DMap[K KeyType, V ValType] struct {
	o          *mpi.Communicator
	Map        *SafeMap[K, V]
	myRank     int
	Inbox      chan Message[K, V]
	msgCount   *SafeCounter
	sendQs     map[int]*safeQueue[K, V]
	totalQSize uint64
}

func (d *DMap[K, V]) totalQueueLength() int {
	var a int
	for _, sq := range d.sendQs {
		sq.RLock()
		a += len(sq.q)
		sq.RUnlock()
	}
	return a
}

func (d *DMap[K, V]) shouldFlush() bool {
	return d.totalQSize > MAXQUEUESIZE
}

func (d *DMap[K, V]) flushSend() {
	var b bytes.Buffer
	var ttlmsgs uint64
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
		atomic.AddUint64(&d.msgCount.sent, lenSq)
		ttlmsgs += lenSq
		b.Reset()
		sq.q = sq.q[:0]
		sq.Unlock()
	}
	// fmt.Printf("%d: called flushSend = sent %d msgs\n", d.myRank, ttlmsgs)
	atomic.StoreUint64(&d.totalQSize, uint64(0))
}

func NewDMap[K KeyType, V ValType](o *mpi.Communicator, chansize int) DMap[K, V] {
	r := o.Rank()
	inbox := make(chan Message[K, V], chansize)
	sm := new(SafeMap[K, V])
	sm.Map = make(map[K]V)

	sendQs := make(map[int]*safeQueue[K, V])
	dm := DMap[K, V]{o: o, Map: sm, myRank: r, Inbox: inbox, msgCount: new(SafeCounter), sendQs: sendQs}
	go recv(dm)
	gob.Register(MessageQueue[K, V]{})
	return dm
}

func recv[K KeyType, V ValType](dmap DMap[K, V]) {
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
		var messageQ MessageQueue[K, V]
		if err := dec.Decode(&messageQ); err != nil {
			fmt.Printf("ERROR IN DECODE: %v", err)
			log.Fatal("decode error: ", err)
		}
		for _, msg := range messageQ {
			switch msg.Type {
			case MsgGet:
				// fmt.Printf("%d: received get %v\n", dmap.myRank, rmsg.Key)
				dmap.Inbox <- msg
			case MsgSet:
				// fmt.Printf("%d: received set %v -> %v\n", dmap.myRank, rmsg.Key, rmsg.Val)
				dmap.Map.Lock()
				dmap.Map.Map[msg.Key] = msg.Val
				dmap.Map.Unlock()
			default:
				log.Fatal("invalid message type: ", tag)
			}
		}
		atomic.AddUint64(&dmap.msgCount.recv, uint64(len(messageQ)))
	}
}
func queueMsg[K KeyType, V ValType](d *DMap[K, V], msg Message[K, V], dest int) {
	// fmt.Printf("%d: encoded message %v is %v; sending to %d\n", d.o.Rank(), *msg, encoded, dest)
	// fmt.Printf("%d: in queueMsg with msg %v\n", d.myRank, msg)
	sq, found := d.sendQs[dest]
	if !found {
		// fmt.Printf("%d: creating new sendQueue for dest %d\n", d.myRank, dest)
		mq := MessageQueue[K, V]{}
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

func (m *DMap[K, V]) Get(k K) (V, bool) {
	dest := k.Hash() % m.o.Size()
	if dest == m.myRank {
		m.Map.RLock()
		val, found := m.Map.Map[k]
		m.Map.RUnlock()
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

func (m *DMap[K, V]) Set(k K, v V) {
	dest := k.Hash() % m.o.Size()
	if dest == m.myRank {
		// fmt.Printf("hash = %d, size = %d, adding local\n", k.Hash(), m.o.Size())
		m.Map.Lock()
		m.Map.Map[k] = v
		m.Map.Unlock()
		return
	}
	msg := Message[K, V]{Type: MsgSet, Key: k, Val: v}
	// fmt.Printf("%d: sending set msg to %d: %v -> %v\n", m.myRank, dest, k, v)
	// fmt.Printf("hash = %d, size = %d, queuing to %d\n", k.Hash(), m.o.Size(), dest)
	queueMsg(m, msg, dest)
	return
}

func (m *DMap[K, V]) Barrier() {
	globalCt := make([]uint64, 2)
	localCt := make([]uint64, 2)

	lastsent, lastrecv := uint64(1), uint64(1)
	for (globalCt[0] != globalCt[1]) || (globalCt[0] != lastsent) || (globalCt[1] != lastrecv) {
		m.flushSend()
		m.o.Barrier()
		lastsent, lastrecv = globalCt[0], globalCt[1]
		localCt[0], localCt[1] = m.GetCount()
		m.o.AllreduceUint64s(globalCt, localCt, mpi.OpSum, 0)
		// fmt.Printf("%d: Barrier: local = %v, global = %v\n", m.myRank, localCt, globalCt)
		time.Sleep(10 * time.Millisecond)
	}
	// lastsent, lastrecv = uint64(1), uint64(1)
	// for globalCt[0] != globalCt[1] && globalCt[0] != lastsent && globalCt[1] != lastrecv {
	// 	lastsent, lastrecv = globalCt[0], globalCt[1]
	// 	localCt[0], localCt[1] = m.GetCount()
	// 	m.o.AllreduceUint64s(globalCt, localCt, mpi.OpSum, 0)
	// 	fmt.Printf("%d: Barrier: local = %v, global = %v\n", m.myRank, localCt, globalCt)
	// 	time.Sleep(10 * time.Millisecond)
	// }
}

func (m *DMap[K, V]) Stop() {
	// fmt.Println("in stop: pre-barrier")
	m.Barrier()
	// fmt.Println("in stop: post-barrier")
	// fmt.Printf("%d: sending done\n", m.myRank)
	m.o.SendString("q", m.myRank, m.o.MaxTag)
	// fmt.Printf("%d: sent done\n", m.myRank)
	<-m.Inbox
}

func (m *DMap[K, V]) GetCount() (uint64, uint64) {
	defer m.msgCount.RUnlock()
	m.msgCount.RLock()
	return m.msgCount.sent, m.msgCount.recv
}
