package hamt

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"

	dag "github.com/ipfs/go-ipfs/merkledag"
	mdtest "github.com/ipfs/go-ipfs/merkledag/test"
	ft "github.com/ipfs/go-ipfs/unixfs"
)

func getNames(prefix string, count int) []string {
	out := make([]string, count)
	for i := 0; i < count; i++ {
		out[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return out
}

const (
	opAdd = iota
	opDel
	opFind
)

type testOp struct {
	Op  int
	Val string
}

func stringArrToSet(arr []string) map[string]bool {
	out := make(map[string]bool)
	for _, s := range arr {
		out[s] = true
	}
	return out
}

func TestOrderConsistency(t *testing.T) {
	ds := mdtest.Mock()

	keep := getNames("good", 4000)
	temp := getNames("tempo", 6000)

	ops := genOpSet(keep, temp)
	s, err := executeOpSet(t, ds, ops)
	if err != nil {
		t.Fatal(err)
	}

	err = validateOpSetCompletion(t, s, keep, temp)
	if err != nil {
		t.Fatal(err)
	}

	ops2 := genOpSet(keep, temp)
	s2, err := executeOpSet(t, ds, ops2)
	if err != nil {
		t.Fatal(err)
	}

	err = validateOpSetCompletion(t, s2, keep, temp)
	if err != nil {
		t.Fatal(err)
	}

	nd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	k, err := nd.Key()
	if err != nil {
		t.Fatal(err)
	}

	nd2, err := s2.Node()
	if err != nil {
		t.Fatal(err)
	}

	k2, err := nd2.Key()
	if err != nil {
		t.Fatal(err)
	}

	if k != k2 {
		t.Fatal("got different results: ", k, k2)
	}
}

func validateOpSetCompletion(t *testing.T, s *HamtShard, keep, temp []string) error {
	for _, n := range keep {
		_, err := s.Find(n)
		if err != nil {
			return fmt.Errorf("couldnt find %s: %s", n, err)
		}
	}

	for _, n := range temp {
		_, err := s.Find(n)
		if err != os.ErrNotExist {
			return fmt.Errorf("expected not to find: %s", err)
		}
	}

	return nil
}

func executeOpSet(t *testing.T, ds dag.DAGService, ops []testOp) (*HamtShard, error) {
	s := NewHamtShard(ds, 256)
	e := ft.EmptyDirNode()
	ds.Add(e)

	for _, o := range ops {
		switch o.Op {
		case opAdd:
			err := s.Insert(o.Val, e)
			if err != nil {
				return nil, fmt.Errorf("inserting %s: %s", o.Val, err)
			}
		case opDel:
			err := s.Remove(o.Val)
			if err != nil {
				return nil, fmt.Errorf("deleting %s: %s", o.Val, err)
			}
		case opFind:
			_, err := s.Find(o.Val)
			if err != nil {
				return nil, fmt.Errorf("finding %s: %s", o.Val, err)
			}
		}
	}

	return s, nil
}

func genOpSet(keep, temp []string) []testOp {
	tempset := stringArrToSet(temp)

	allnames := append(keep, temp...)
	shuffle(allnames)

	var todel []string

	var ops []testOp

	for {
		n := len(allnames) + len(todel)
		if n == 0 {
			return ops
		}

		rn := rand.Intn(n)

		if rn < len(allnames) {
			next := allnames[0]
			allnames = allnames[1:]
			ops = append(ops, testOp{
				Op:  opAdd,
				Val: next,
			})

			if tempset[next] {
				todel = append(todel, next)
			}
		} else {
			//shuffle(todel)
			next := todel[0]
			todel = todel[1:]

			ops = append(ops, testOp{
				Op:  opDel,
				Val: next,
			})
		}
	}
}

// executes the given op set with a repl to allow easier debugging
func debugExecuteOpSet(ds dag.DAGService, ops []testOp) (*HamtShard, error) {
	s := NewHamtShard(ds, 256)
	e := ft.EmptyDirNode()
	ds.Add(e)

	run := 0

	opnames := map[int]string{
		opAdd: "add",
		opDel: "del",
	}

mainloop:
	for i := 0; i < len(ops); i++ {
		o := ops[i]

		fmt.Printf("Op %d: %s %s\n", i, opnames[o.Op], o.Val)
		for run == 0 {
			cmd := readCommand()
			parts := strings.Split(cmd, " ")
			switch parts[0] {
			case "":
				run = 1
			case "find":
				_, err := s.Find(parts[1])
				if err == nil {
					fmt.Println("success")
				} else {
					fmt.Println(err)
				}
			case "run":
				if len(parts) > 1 {
					n, err := strconv.Atoi(parts[1])
					if err != nil {
						panic(err)
					}

					run = n
				} else {
					run = -1
				}
			case "lookop":
				for k := 0; k < len(ops); k++ {
					if ops[k].Val == parts[1] {
						fmt.Printf("  Op %d: %s %s\n", k, opnames[ops[k].Op], parts[1])
					}
				}
			case "restart":
				s = NewHamtShard(ds, 256)
				i = -1
				continue mainloop
			case "print":
				nd, err := s.Node()
				if err != nil {
					panic(err)
				}
				printDag(ds, nd, 0)
			}
		}
		run--

		switch o.Op {
		case opAdd:
			err := s.Insert(o.Val, e)
			if err != nil {
				return nil, fmt.Errorf("inserting %s: %s", o.Val, err)
			}
		case opDel:
			fmt.Println("deleting: ", o.Val)
			err := s.Remove(o.Val)
			if err != nil {
				return nil, fmt.Errorf("deleting %s: %s", o.Val, err)
			}
		case opFind:
			_, err := s.Find(o.Val)
			if err != nil {
				return nil, fmt.Errorf("finding %s: %s", o.Val, err)
			}
		}
	}

	return s, nil
}

func readCommand() string {
	fmt.Print("> ")
	scan := bufio.NewScanner(os.Stdin)
	scan.Scan()
	return scan.Text()
}