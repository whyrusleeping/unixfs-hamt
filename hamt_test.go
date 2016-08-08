package hamt

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	key "github.com/ipfs/go-ipfs/blocks/key"
	dag "github.com/ipfs/go-ipfs/merkledag"
	mdtest "github.com/ipfs/go-ipfs/merkledag/test"
	dagutils "github.com/ipfs/go-ipfs/merkledag/utils"
	ft "github.com/ipfs/go-ipfs/unixfs"
)

func shuffle(seed int64, arr []string) {
	r := rand.New(rand.NewSource(seed))
	for i := 0; i < len(arr); i++ {
		a := r.Intn(len(arr))
		b := r.Intn(len(arr))
		arr[a], arr[b] = arr[b], arr[a]
	}
}

func makeDir(ds dag.DAGService, size int) ([]string, *HamtShard, error) {
	s := NewHamtShard(ds, 256)

	var dirs []string
	for i := 0; i < size; i++ {
		dirs = append(dirs, fmt.Sprintf("DIRNAME%d", i))
	}

	shuffle(time.Now().UnixNano(), dirs)

	for i := 0; i < len(dirs); i++ {
		nd := ft.EmptyDirNode()
		ds.Add(nd)
		err := s.Insert(dirs[i], nd)
		if err != nil {
			return nil, nil, err
		}
	}

	return dirs, s, nil
}

func assertLink(s *HamtShard, name string, found bool) error {
	_, err := s.Find(name)
	switch err {
	case os.ErrNotExist:
		if found {
			return err
		}

		return nil
	case nil:
		if found {
			return nil
		}

		return fmt.Errorf("expected not to find link named %s", name)
	default:
		return err
	}
}

func assertSerializationWorks(ds dag.DAGService, s *HamtShard) error {
	nd, err := s.Node()
	if err != nil {
		return err
	}

	nds, err := NewHamtFromDag(ds, nd)
	if err != nil {
		return err
	}

	linksA, err := s.EnumLinks()
	if err != nil {
		return err
	}

	linksB, err := nds.EnumLinks()
	if err != nil {
		return err
	}

	if len(linksA) != len(linksB) {
		return fmt.Errorf("links arrays are different sizes")
	}

	for i, a := range linksA {
		b := linksB[i]
		if a.Name != b.Name {
			return fmt.Errorf("links names mismatch")
		}

		if a.Hash.B58String() != b.Hash.B58String() {
			return fmt.Errorf("link hashes dont match")
		}

		if a.Size != b.Size {
			return fmt.Errorf("link sizes not the same")
		}
	}

	return nil
}

func TestBasicInsert(t *testing.T) {
	ds := mdtest.Mock()
	names, s, err := makeDir(ds, 1000)
	if err != nil {
		t.Fatal(err)
	}

	for _, d := range names {
		_, err := s.Find(d)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDirBuilding(t *testing.T) {
	ds := mdtest.Mock()
	s := NewHamtShard(ds, 256)

	_, s, err := makeDir(ds, 200)
	if err != nil {
		t.Fatal(err)
	}

	nd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	//printDag(ds, nd, 0)

	k, err := nd.Key()
	if err != nil {
		t.Fatal(err)
	}

	if k.B58String() != "QmQkGrUnPFpsmfNSDTBDipP9qygn6H9nx4erz4HuKL3wdE" {
		t.Fatalf("output didnt match what we expected (got %s)", k.B58String())
	}
}

func TestShardReload(t *testing.T) {
	ds := mdtest.Mock()
	s := NewHamtShard(ds, 256)

	_, s, err := makeDir(ds, 200)
	if err != nil {
		t.Fatal(err)
	}

	nd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	nds, err := NewHamtFromDag(ds, nd)
	if err != nil {
		t.Fatal(err)
	}

	lnks, err := nds.EnumLinks()
	if err != nil {
		t.Fatal(err)
	}

	if len(lnks) != 200 {
		t.Fatal("not enough links back")
	}

	_, err = nds.Find("DIRNAME50")
	if err != nil {
		t.Fatal(err)
	}

	// Now test roundtrip marshal with no operations

	nds, err = NewHamtFromDag(ds, nd)
	if err != nil {
		t.Fatal(err)
	}

	ond, err := nds.Node()
	if err != nil {
		t.Fatal(err)
	}

	outk, err := ond.Key()
	if err != nil {
		t.Fatal(err)
	}

	ndk, err := nd.Key()
	if err != nil {
		t.Fatal(err)
	}

	if outk != ndk {
		printDiff(ds, nd, ond)
		t.Fatal("roundtrip serialization failed")
	}
}

func TestRemoveElems(t *testing.T) {
	ds := mdtest.Mock()
	dirs, s, err := makeDir(ds, 500)
	if err != nil {
		t.Fatal(err)
	}

	shuffle(time.Now().UnixNano(), dirs)

	for _, d := range dirs {
		err := s.Remove(d)
		if err != nil {
			t.Fatal(err)
		}
	}

	nd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	if len(nd.Links) > 0 {
		t.Fatal("shouldnt have any links here")
	}

	err = s.Remove("doesnt exist")
	if err != os.ErrNotExist {
		t.Fatal("expected error does not exist")
	}
}

func TestInsertAfterMarshal(t *testing.T) {
	ds := mdtest.Mock()
	_, s, err := makeDir(ds, 300)
	if err != nil {
		t.Fatal(err)
	}

	nd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	nds, err := NewHamtFromDag(ds, nd)
	if err != nil {
		t.Fatal(err)
	}

	empty := ft.EmptyDirNode()
	for i := 0; i < 100; i++ {
		err := nds.Insert(fmt.Sprintf("moredirs%d", i), empty)
		if err != nil {
			t.Fatal(err)
		}
	}

	links, err := nds.EnumLinks()
	if err != nil {
		t.Fatal(err)
	}

	if len(links) != 400 {
		t.Fatal("expected 400 links")
	}

	err = assertSerializationWorks(ds, nds)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDuplicateAddShard(t *testing.T) {
	ds := mdtest.Mock()
	dir := NewHamtShard(ds, 256)
	nd := ft.EmptyDirNode()

	err := dir.Insert("test", nd)
	if err != nil {
		t.Fatal(err)
	}

	err = dir.Insert("test", nd)
	if err != nil {
		t.Fatal(err)
	}

	lnks, err := dir.EnumLinks()
	if err != nil {
		t.Fatal(err)
	}

	if len(lnks) != 1 {
		t.Fatal("expected only one link")
	}
}

func TestLoadFailsFromNonShard(t *testing.T) {
	ds := mdtest.Mock()
	nd := ft.EmptyDirNode()

	_, err := NewHamtFromDag(ds, nd)
	if err == nil {
		t.Fatal("expected dir shard creation to fail when given normal directory")
	}

	nd = new(dag.Node)

	_, err = NewHamtFromDag(ds, nd)
	if err == nil {
		t.Fatal("expected dir shard creation to fail when given normal directory")
	}
}

func TestFindNonExisting(t *testing.T) {
	ds := mdtest.Mock()
	_, s, err := makeDir(ds, 100)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 200; i++ {
		_, err := s.Find(fmt.Sprintf("notfound%d", i))
		if err != os.ErrNotExist {
			t.Fatal("expected ErrNotExist")
		}
	}
}

func TestRemoveElemsAfterMarshal(t *testing.T) {
	ds := mdtest.Mock()
	dirs, s, err := makeDir(ds, 30)
	if err != nil {
		t.Fatal(err)
	}

	sort.Strings(dirs)

	err = s.Remove(dirs[0])
	if err != nil {
		t.Fatal(err)
	}

	out, err := s.Find(dirs[0])
	if err == nil {
		t.Fatal("expected error, got: ", out)
	}

	nd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	nds, err := NewHamtFromDag(ds, nd)
	if err != nil {
		t.Fatal(err)
	}

	_, err = nds.Find(dirs[0])
	if err == nil {
		t.Fatal("expected not to find ", dirs[0])
	}

	for _, d := range dirs[1:] {
		_, err := nds.Find(d)
		if err != nil {
			t.Fatal("could not find expected link after unmarshaling")
		}
	}

	for _, d := range dirs[1:] {
		err := nds.Remove(d)
		if err != nil {
			t.Fatal(err)
		}
	}

	links, err := nds.EnumLinks()
	if err != nil {
		t.Fatal(err)
	}

	if len(links) != 0 {
		t.Fatal("expected all links to be removed")
	}

	err = assertSerializationWorks(ds, nds)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBitfieldIndexing(t *testing.T) {
	ds := mdtest.Mock()
	s := NewHamtShard(ds, 256)

	set := func(i int) {
		s.bitfield.SetBit(s.bitfield, i, 1)
	}

	assert := func(i int, val int) {
		if s.indexForBitPos(i) != val {
			t.Fatalf("expected index %d to be %d", i, val)
		}
	}

	assert(50, 0)
	set(4)
	set(5)
	set(60)

	assert(10, 2)
	set(3)
	assert(10, 3)
	assert(1, 0)

	assert(100, 4)
	set(50)
	assert(45, 3)
	set(100)
	assert(100, 5)
}

// test adding a sharded directory node as the child of another directory node.
// if improperly implemented, the parent hamt may assume the child is a part of
// itself.
func TestInsertHamtChild(t *testing.T) {
	ds := mdtest.Mock()
	s := NewHamtShard(ds, 256)

	e := ft.EmptyDirNode()
	ds.Add(e)

	err := s.Insert("bar", e)
	if err != nil {
		t.Fatal(err)
	}

	snd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	_, ns, err := makeDir(ds, 50)
	if err != nil {
		t.Fatal(err)
	}

	err = ns.Insert("foo", snd)
	if err != nil {
		t.Fatal(err)
	}

	nsnd, err := ns.Node()
	if err != nil {
		t.Fatal(err)
	}

	hs, err := NewHamtFromDag(ds, nsnd)
	if err != nil {
		t.Fatal(err)
	}

	err = assertLink(hs, "bar", false)
	if err != nil {
		t.Fatal(err)
	}

	err = assertLink(hs, "foo", true)
	if err != nil {
		t.Fatal(err)
	}
}

func printDag(ds dag.DAGService, nd *dag.Node, depth int) {
	padding := strings.Repeat(" ", depth)
	fmt.Println("{")
	for _, l := range nd.Links {
		fmt.Printf("%s%s: %s", padding, l.Name, l.Hash.B58String())
		ch, err := ds.Get(context.Background(), key.Key(l.Hash))
		if err != nil {
			panic(err)
		}

		printDag(ds, ch, depth+1)
	}
	fmt.Println(padding + "}")
}

func printDiff(ds dag.DAGService, a, b *dag.Node) {
	diff, err := dagutils.Diff(context.TODO(), ds, a, b)
	if err != nil {
		panic(err)
	}

	for _, d := range diff {
		fmt.Println(d)
	}
}
