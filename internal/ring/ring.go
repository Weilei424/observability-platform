// Package ring assigns keys — series and stream fingerprints — to members with
// a token ring. Each member places TokensPerMember tokens on a 64-bit ring; a
// key belongs to the member owning the first token at or after the key's
// position, wrapping past the top. Adding or removing one of N members moves
// about 1/N of the keys, and only to or from that member.
//
// The ring is a pure function of the member set: no I/O, no clock, no
// randomness. Keys are already FNV-64 fingerprints; Mix64 spreads whatever
// structure FNV leaves in them before they meet the token table.
package ring

import (
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// TokensPerMember is how many tokens each member places on the ring.
const TokensPerMember = 128

// Ring maps keys to members. It is immutable and safe for concurrent use.
type Ring struct {
	members []string // sorted
	tokens  []uint64 // ascending; equal tokens ordered by owner index
	owners  []int    // owners[i] is the members index of tokens[i]
}

// New builds the ring for members: non-empty, no empty IDs, no duplicates.
// List order does not matter.
func New(members []string) (*Ring, error) {
	if len(members) == 0 {
		return nil, errors.New("ring: no members")
	}
	sorted := slices.Clone(members)
	slices.Sort(sorted)
	for i, m := range sorted {
		if m == "" {
			return nil, errors.New("ring: empty member ID")
		}
		if i > 0 && sorted[i-1] == m {
			return nil, fmt.Errorf("ring: duplicate member at sorted position %d", i)
		}
	}
	type token struct {
		v     uint64
		owner int
	}
	toks := make([]token, 0, len(sorted)*TokensPerMember)
	for i, m := range sorted {
		for j := range TokensPerMember {
			toks = append(toks, token{tokenOf(m, j), i})
		}
	}
	sort.Slice(toks, func(a, b int) bool {
		if toks[a].v != toks[b].v {
			return toks[a].v < toks[b].v
		}
		return toks[a].owner < toks[b].owner
	})
	r := &Ring{members: sorted, tokens: make([]uint64, len(toks)), owners: make([]int, len(toks))}
	for i, t := range toks {
		r.tokens[i], r.owners[i] = t.v, t.owner
	}
	return r, nil
}

// Owner returns the member that owns key.
func (r *Ring) Owner(key uint64) string { return r.lookup(Mix64(key)) }

// lookup returns the owner of ring position h: the first token at or after h,
// wrapping to the first token. Among equal tokens the first in order wins,
// which is the member that sorts first.
func (r *Ring) lookup(h uint64) string {
	i := sort.Search(len(r.tokens), func(i int) bool { return r.tokens[i] >= h })
	if i == len(r.tokens) {
		i = 0
	}
	return r.members[r.owners[i]]
}

// Members returns the members, sorted. The slice is a copy.
func (r *Ring) Members() []string { return slices.Clone(r.members) }

// Hash identifies the member set: 8 hex characters, equal for two rings with
// the same members in any order. The gateway and querier log it at startup so
// an operator can see that both route and read over the same set.
func (r *Ring) Hash() string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(strings.Join(r.members, "\n")))
	return fmt.Sprintf("%08x", h.Sum32())
}

// Mix64 is the splitmix64 finalizer.
func Mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func tokenOf(member string, i int) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(member + "#" + strconv.Itoa(i)))
	return Mix64(h.Sum64())
}
