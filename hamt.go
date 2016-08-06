package hamt

import (
	"fmt"
	"math"
	"math/big"
	"os"

	proto "github.com/gogo/protobuf/proto"
	"github.com/spaolacci/murmur3"
	"golang.org/x/net/context"

	dag "github.com/ipfs/go-ipfs/merkledag"
	format "github.com/ipfs/go-ipfs/unixfs"
	upb "github.com/ipfs/go-ipfs/unixfs/pb"
)

const (
	HashMurmur3 uint64 = iota
)

type HamtShard struct {
	nd *dag.Node

	bitfield *big.Int

	children []child

	tableSize    int
	tableSizeLg2 int

	hashFunc uint64

	prefixPadStr string
	maxpadlen    int

	dserv dag.DAGService
}

// child can either be another shard, or a leaf node value
type child interface {
	Node() (*dag.Node, error)
	Label() string
}

func NewHamtShard(dserv dag.DAGService, size int) *HamtShard {
	ds := makeHamtShard(dserv, size)
	ds.bitfield = big.NewInt(0)
	ds.nd = new(dag.Node)
	ds.hashFunc = HashMurmur3
	return ds
}

func makeHamtShard(ds dag.DAGService, size int) *HamtShard {
	maxpadding := fmt.Sprintf("%X", size-1)
	return &HamtShard{
		tableSizeLg2: int(math.Log2(float64(size))),
		prefixPadStr: fmt.Sprintf("%%0%dX", len(maxpadding)),
		maxpadlen:    len(maxpadding),
		tableSize:    size,
		dserv:        ds,
	}
}

func NewHamtFromDag(dserv dag.DAGService, nd *dag.Node) (*HamtShard, error) {
	pbd, err := format.FromBytes(nd.Data())
	if err != nil {
		return nil, err
	}

	if pbd.GetType() != upb.Data_HAMTShard {
		return nil, fmt.Errorf("node was not a dir shard")
	}

	if pbd.GetHashType() != HashMurmur3 {
		return nil, fmt.Errorf("only murmur3 supported as hash function")
	}

	ds := makeHamtShard(dserv, int(pbd.GetFanout()))
	ds.nd = nd.Copy()
	ds.children = make([]child, len(nd.Links))
	ds.bitfield = new(big.Int).SetBytes(pbd.GetData())
	ds.hashFunc = pbd.GetHashType()

	return ds, nil
}

// Node serializes the HAMT structure into a merkledag node with unixfs formatting
func (ds *HamtShard) Node() (*dag.Node, error) {
	out := new(dag.Node)

	// TODO: optimized 'for each set bit'
	for i := 0; i < ds.tableSize; i++ {
		if ds.bitfield.Bit(i) == 0 {
			continue
		}

		cindex := ds.indexForBitPos(i)
		child := ds.children[cindex]
		if child != nil {
			cnd, err := child.Node()
			if err != nil {
				return nil, err
			}

			err = out.AddNodeLinkClean(ds.linkNamePrefix(i)+child.Label(), cnd)
			if err != nil {
				return nil, err
			}
		} else {
			// child unloaded, just copy in link with updated name
			lnk := ds.nd.Links[cindex]
			label := lnk.Name[ds.maxpadlen:]

			err := out.AddRawLink(ds.linkNamePrefix(i)+label, lnk)
			if err != nil {
				return nil, err
			}
		}
	}

	typ := upb.Data_HAMTShard
	data, err := proto.Marshal(&upb.Data{
		Type:     &typ,
		Fanout:   proto.Uint64(uint64(ds.tableSize)),
		HashType: proto.Uint64(HashMurmur3),
		Data:     ds.bitfield.Bytes(),
	})
	if err != nil {
		return nil, err
	}

	out.SetData(data)

	_, err = ds.dserv.Add(out)
	if err != nil {
		return nil, err
	}

	return out, nil
}

type shardValue struct {
	key string
	val *dag.Node
}

func (sv *shardValue) Node() (*dag.Node, error) {
	return sv.val, nil
}

func (sv *shardValue) Label() string {
	return sv.key
}

func hash(val []byte) []byte {
	h := murmur3.New64()
	h.Write(val)
	return h.Sum(nil)
}

func (ds *HamtShard) Label() string {
	return ""
}

func (ds *HamtShard) Insert(name string, nd *dag.Node) error {
	hv := &hashBits{b: hash([]byte(name))}
	return ds.modifyHash(hv, name, nd)
}

func (ds *HamtShard) Remove(name string) error {
	hv := &hashBits{b: hash([]byte(name))}
	return ds.modifyHash(hv, name, nil)
}

func (ds *HamtShard) Find(name string) (*dag.Node, error) {
	hv := &hashBits{b: hash([]byte(name))}

	var out *dag.Node
	err := ds.consumeValue(hv, name, func(sv *shardValue) error {
		out = sv.val
		return nil
	})

	return out, err
}

func (ds *HamtShard) getChild(ctx context.Context, i int) (child, error) {
	c := ds.children[i]
	if c == nil {
		lnk := ds.nd.Links[i]
		nd, err := lnk.GetNode(ctx, ds.dserv)
		if err != nil {
			return nil, err
		}

		if len(lnk.Name) < ds.maxpadlen {
			return nil, fmt.Errorf("invalid link name '%s'", lnk.Name)
		}

		pbd, err := format.FromBytes(nd.Data())
		if err != nil {
			return nil, err
		}

		if len(lnk.Name) == ds.maxpadlen {
			if pbd.GetType() != format.THAMTShard {
				return nil, fmt.Errorf("HAMT entries must have non-zero length name")
			}

			cds, err := NewHamtFromDag(ds.dserv, nd)
			if err != nil {
				return nil, err
			}

			c = cds
		} else {
			c = &shardValue{
				key: lnk.Name[ds.maxpadlen:],
				val: nd,
			}
		}

		ds.children[i] = c
	}

	return c, nil
}

func (ds *HamtShard) setChild(i int, c child) {
	ds.children[i] = c
}

func (ds *HamtShard) insertChild(idx int, key string, val *dag.Node) error {
	if val == nil {
		return os.ErrNotExist
	}

	i := ds.indexForBitPos(idx)
	ds.bitfield.SetBit(ds.bitfield, idx, 1)
	sv := &shardValue{
		key: key,
		val: val,
	}

	ds.children = append(ds.children[:i], append([]child{sv}, ds.children[i:]...)...)
	ds.nd.Links = append(ds.nd.Links[:i], append([]*dag.Link{nil}, ds.nd.Links[i:]...)...)
	return nil
}

func (ds *HamtShard) rmChild(i int) error {
	copy(ds.children[i:], ds.children[i+1:])
	ds.children = ds.children[:len(ds.children)-1]

	lnk := ds.nd.Links[i]
	if lnk != nil {
		return ds.nd.RemoveNodeLink(lnk.Name)
	}
	return nil
}

func (ds *HamtShard) consumeValue(hv *hashBits, key string, cb func(*shardValue) error) error {
	idx := hv.Next(ds.tableSizeLg2)
	if ds.bitfield.Bit(int(idx)) == 1 {
		cindex := ds.indexForBitPos(idx)

		child, err := ds.getChild(context.TODO(), cindex)
		if err != nil {
			return err
		}

		switch child := child.(type) {
		case *HamtShard:
			return child.consumeValue(hv, key, cb)
		case *shardValue:
			if child.key == key {
				return cb(child)
			}
		}
	}

	return os.ErrNotExist
}

func (ds *HamtShard) EnumLinks() ([]*dag.Link, error) {
	var links []*dag.Link
	err := ds.walkTrie(func(sv *shardValue) error {
		lnk, err := dag.MakeLink(sv.val)
		if err != nil {
			return err
		}

		lnk.Name = sv.key

		links = append(links, lnk)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return links, nil
}

func (ds *HamtShard) walkTrie(cb func(*shardValue) error) error {
	for i := 0; i < ds.tableSize; i++ {
		if ds.bitfield.Bit(i) == 0 {
			continue
		}

		idx := ds.indexForBitPos(i)
		// NOTE: an optimized version could simply iterate over each
		//       element in the 'children' array.
		c, err := ds.getChild(context.TODO(), idx)
		if err != nil {
			return err
		}

		switch c := c.(type) {
		case *shardValue:
			err := cb(c)
			if err != nil {
				return err
			}

		case *HamtShard:
			err := c.walkTrie(cb)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected child type: %#v", c)
		}
	}
	return nil
}

func (ds *HamtShard) modifyHash(hv *hashBits, key string, val *dag.Node) error {
	idx := hv.Next(ds.tableSizeLg2)

	if ds.bitfield.Bit(idx) == 1 {
		cindex := ds.indexForBitPos(idx)

		child, err := ds.getChild(context.TODO(), cindex)
		if err != nil {
			return err
		}

		switch child := child.(type) {
		case *HamtShard:
			err := child.modifyHash(hv, key, val)
			if err != nil {
				return err
			}

			if val == nil {
				switch len(child.children) {
				case 0:
					// empty sub-shard, prune it
					// Note: this shouldnt normally ever happen
					//       in the event of another implementation creates flawed
					//       structures, this will help to normalize them.
					ds.bitfield.SetBit(ds.bitfield, idx, 0)
					return ds.rmChild(cindex)
				case 1:
					nchild, ok := child.children[0].(*shardValue)
					if ok {
						// sub-shard with a single value element, collapse it
						ds.setChild(cindex, nchild)
					}
					return nil
				}
			}

			return nil
		case *shardValue:
			switch {
			// passing a nil value signifies a 'delete'
			case val == nil:
				ds.bitfield.SetBit(ds.bitfield, idx, 0)
				return ds.rmChild(cindex)

				// value modification
			case child.key == key:
				child.val = val
				return nil
			default:
				ns := NewHamtShard(ds.dserv, ds.tableSize)
				chhv := &hashBits{
					b:        hash([]byte(child.key)),
					consumed: hv.consumed,
				}

				err := ns.modifyHash(hv, key, val)
				if err != nil {
					return err
				}

				err = ns.modifyHash(chhv, child.key, child.val)
				if err != nil {
					return err
				}

				ds.setChild(cindex, ns)
				return nil
			}
		default:
			return fmt.Errorf("unexpected type for child: %#v", child)
		}
	} else {
		return ds.insertChild(idx, key, val)
	}
}

func (ds *HamtShard) indexForBitPos(bp int) int {
	// NOTE: an optimization could reuse the same 'mask' here and change the size
	//       as needed. This isnt yet done as the bitset package doesnt make it easy
	//       to do.
	mask := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(bp)), nil), big.NewInt(1))
	mask.And(mask, ds.bitfield)

	return popCount(mask)
}

// linkNamePrefix takes in the bitfield index of an entry and returns its hex prefix
func (ds *HamtShard) linkNamePrefix(idx int) string {
	return fmt.Sprintf(ds.prefixPadStr, idx)
}
