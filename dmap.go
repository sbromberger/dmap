package dmap

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"

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

func EncodeMsg[K KeyType, V ValType](msg Message[K, V], b *bytes.Buffer) []byte {
	b.Reset()
	enc := gob.NewEncoder(b)
	err := enc.Encode(msg)
	if err != nil {
		fmt.Printf("ERROR IN ENCODE: %v", err)
		log.Fatal("error in encode: ", err)
	}
	return b.Bytes()
}

type DMap[K KeyType, V ValType] struct {
	o      *mpi.Communicator
	Map    map[K]V
	myRank int
	Inbox  chan Message[K, V]
	b      bytes.Buffer
}

func NewDMap[K KeyType, V ValType](o *mpi.Communicator) DMap[K, V] {
	d := make(map[K]V)
	r := o.Rank()
	inbox := make(chan Message[K, V], 100)
	dm := DMap[K, V]{o: o, Map: d, myRank: r, Inbox: inbox}
	go recv(dm)
	gob.Register(Message[K, V]{})
	return dm
}

func recv[K KeyType, V ValType](dmap DMap[K, V]) {
	// runtime.LockOSThread()
	for {
		recvbytes, status := dmap.o.MrecvBytes(mpi.AnySource, mpi.AnyTag)
		tag := status.GetTag()
		if tag == dmap.o.MaxTag {
			return
		}
		b := bytes.NewBuffer(recvbytes)
		dec := gob.NewDecoder(b)
		var rmsg Message[K, V]
		if err := dec.Decode(&rmsg); err != nil {
			fmt.Printf("ERROR IN DECODE: %v", err)
			log.Fatal("decode error: ", err)
		}
		switch tag {
		case int(MsgGet):
			fmt.Printf("%d: received get %v\n", dmap.myRank, rmsg.Key)
			dmap.Inbox <- rmsg
		case int(MsgSet):
			fmt.Printf("%d: received set %v -> %v (raw %v | %v)\n", dmap.myRank, rmsg.Key, rmsg.Val, recvbytes, b)
			dmap.Map[rmsg.Key] = rmsg.Val
		default:
			log.Fatal("invalid message type: ", tag)
		}
	}
}
func sendMsg[K KeyType, V ValType](o *mpi.Communicator, msg *Message[K, V], dest int, b *bytes.Buffer) {
	encoded := EncodeMsg(*msg, b)
	fmt.Printf("%d: encoded message %v is %v; sending to %d\n", o.Rank(), *msg, encoded, dest)
	o.SendBytes(encoded, dest, int(msg.Type))
}

func (m *DMap[K, V]) Get(k K) (V, bool) {
	dest := k.Hash() % m.o.Size()
	if dest == m.myRank {
		val, found := m.Map[k]
		return val, found
	}
	msg := Message[K, V]{Type: MsgGet, Key: k}
	sendMsg(m.o, &msg, dest, &m.b)
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
		m.Map[k] = v
		return
	}
	msg := Message[K, V]{Type: MsgSet, Key: k, Val: v}
	fmt.Printf("%d: sending set msg to %d: %v -> %v\n", m.myRank, dest, k, v)
	sendMsg(m.o, &msg, dest, &m.b)
	return
}

func (m *DMap[K, V]) Stop() {
	m.o.SendString("done", m.myRank, m.o.MaxTag)
}
