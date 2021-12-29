package main

import (
	"fmt"
	"os"
	"strconv"

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

	n, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic("Invalid number of iterations")
	}

	mpi.Start(true)
	o := mpi.NewCommunicator(nil)
	myRank := o.Rank()
	fmt.Printf("%d: startedn\n", myRank)
	size := o.Size()

	d := dmap.NewDMap[key, val](o, n)

	t0 := mpi.WorldTime()
	for i := 0; i < n; i++ {
		k := i*size + myRank + 1
		d.Set(key(k), val(k))

	}
	t1 := mpi.WorldTime()

	t2 := mpi.WorldTime()
	d.Stop()
	for i := 0; i < o.Size(); i++ {
		if myRank == i {
			fmt.Printf("%d: LocalSize %d\n", i, d.LocalSize())
			// fmt.Printf("***************** %d: %v\n", i, d.Map.Map)
		}
	}
	if myRank == 0 {

		fmt.Printf("set elapsed: %0.2f s, sync elapsed %0.2f s\n", t1-t0, t2-t0)
		fmt.Printf("set average: %0.2f µs, sync average %0.2f µs\n", (t1-t0)*1_000_000/float64(n*o.Size()), (t2-t0)*1_000_000/float64(n*o.Size()))
	}
	mpi.Stop()
}
