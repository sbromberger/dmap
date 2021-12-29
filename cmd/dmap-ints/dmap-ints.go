package main

import (
	"fmt"

	"github.com/sbromberger/dmap"
	mpi "github.com/sbromberger/gompi"
)

type key int

func (k key) Hash() int {
	return int(k)
}

type val int

func (v val) Empty() bool {
	return int(v) == 0
}

func main() {
	mpi.Start(true)
	fmt.Println("started")
	o := mpi.NewCommunicator(nil)
	myRank := o.Rank()

	d := dmap.New[key, val](o, 1)

	if myRank == 0 {
		fmt.Println("0: setting keys 1 through 20 to values 101 to 120")
		for i := 1; i <= 20; i++ {
			fmt.Printf("0: key = %d, val = %d\n", i, 100+i)
			d.Set(key(i), val(100+i))
		}
	}
	d.Barrier()
	// s, r := d.GetMsgCount()
	// fmt.Printf("%d: count = %d (sent %d, recv %d)\n", myRank, int64(s)-int64(r), s, r)

	if myRank == 0 {
		fmt.Println("Barrier")
	}
	if myRank == 0 {
		fmt.Println("Stop")
	}
	d.Stop()
	for i := 0; i < o.Size(); i++ {
		if myRank == i {
			fmt.Printf("%d: %v\n", i, d)
		}
	}
	fmt.Println("pre mpi stop")
	mpi.Stop()
}
