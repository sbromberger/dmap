package dmap

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"
	"time"

	mpi "github.com/sbromberger/gompi"
)

type MsgType uint8

const BUFSIZE = 1024 * 1024 * 32 // 32 MB
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
type DMap[K KeyType, V ValType] struct {
	o        *mpi.Communicator
	Map      *SafeMap[K, V]
	myRank   int
	Inbox    chan Message[K, V]
	msgCount *SafeCounter
}

func NewDMap[K KeyType, V ValType](o *mpi.Communicator) DMap[K, V] {
	r := o.Rank()
	inbox := make(chan Message[K, V], 100)
	sm := new(SafeMap[K, V])
	sm.Map = make(map[K]V)

	dm := DMap[K, V]{o: o, Map: sm, myRank: r, Inbox: inbox, msgCount: new(SafeCounter)}
	go recv(dm)
	gob.Register(Message[K, V]{})
	return dm
}

func recv[K KeyType, V ValType](dmap DMap[K, V]) {
	defer fmt.Printf("%d: recv terminating\n", dmap.myRank)
	// runtime.LockOSThread()
	for {
		recvbytes, status := dmap.o.MrecvBytes(mpi.AnySource, mpi.AnyTag)
		dmap.msgCount.Lock()
		dmap.msgCount.recv++
		dmap.msgCount.Unlock()

		tag := status.GetTag()
		if tag == dmap.o.MaxTag {
			return
		}
		b := bytes.NewReader(recvbytes)
		dec := gob.NewDecoder(b)
		var rmsg Message[K, V]
		if err := dec.Decode(&rmsg); err != nil {
			fmt.Printf("ERROR IN DECODE: %v", err)
			log.Fatal("decode error: ", err)
		}
		switch tag {
		case int(MsgGet):
			// fmt.Printf("%d: received get %v\n", dmap.myRank, rmsg.Key)
			dmap.Inbox <- rmsg
		case int(MsgSet):
			// fmt.Printf("%d: received set %v -> %v\n", dmap.myRank, rmsg.Key, rmsg.Val)
			dmap.Map.Lock()
			dmap.Map.Map[rmsg.Key] = rmsg.Val
			dmap.Map.Unlock()
		default:
			log.Fatal("invalid message type: ", tag)
		}
	}
}
func sendMsg[K KeyType, V ValType](d *DMap[K, V], msg *Message[K, V], dest int) {
	encoded := EncodeMsg(*msg)
	// fmt.Printf("%d: encoded message %v is %v; sending to %d\n", d.o.Rank(), *msg, encoded, dest)
	d.o.SendBytes(encoded, dest, int(msg.Type))
	d.msgCount.Lock()
	d.msgCount.sent++
	d.msgCount.Unlock()
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
	sendMsg(m, &msg, dest)
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
		m.Map.Lock()
		m.Map.Map[k] = v
		m.Map.Unlock()
		return
	}
	msg := Message[K, V]{Type: MsgSet, Key: k, Val: v}
	// fmt.Printf("%d: sending set msg to %d: %v -> %v\n", m.myRank, dest, k, v)
	sendMsg(m, &msg, dest)
	return
}

func (m *DMap[K, V]) Barrier() {
	globalCt := make([]uint64, 2)
	localCt := make([]uint64, 2)

	lastsent, lastrecv := uint64(1), uint64(1)
	for (globalCt[0] != globalCt[1]) || (globalCt[0] != lastsent) || (globalCt[1] != lastrecv) {
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
	m.o.SendString("done", m.myRank, m.o.MaxTag)
}

func (m *DMap[K, V]) GetCount() (uint64, uint64) {
	defer m.msgCount.RUnlock()
	m.msgCount.RLock()
	return m.msgCount.sent, m.msgCount.recv
}
