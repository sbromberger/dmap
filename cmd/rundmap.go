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
	return v == 0
}

func main() {
	mpi.Start(true)
	fmt.Println("started")
	o := mpi.NewCommunicator(nil)
	myRank := o.Rank()

	d := dmap.NewDMap[key, val](o)

	if myRank == 0 {
		fmt.Println("0: setting keys 1 through 10 to values 101 to 110")
		for i := 1; i <= 10; i++ {
			d.Set(key(i), val(100+i))
		}
	}
	fmt.Printf("%d: count = %v\n", myRank, d.GetCount())
	o.Barrier()

	if myRank == 0 {
		fmt.Println("Barrier")
	}
	if myRank == 0 {
		fmt.Println("End")
	}
	o.Barrier()
	d.Stop()
	for i := 0; i < o.Size(); i++ {
		if myRank == i {
			fmt.Printf("%d: %v\n", i, d.Map)
		}
	}
	mpi.Stop()
}
