package b3

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"math/rand/v2"
	"runtime/debug"
	"slices"
	"testing"

	"github.com/zeebo/blake3"
)

// test_vectors.json is the BLAKE3 team's, from github.com/BLAKE3-team/BLAKE3/test_vectors.
//
//go:embed testdata/test_vectors.json
var vectorsJSON []byte

func random(n int) []byte {
	b := make([]byte, n)
	rand.NewChaCha8([32]byte{byte(n), byte(n >> 8), byte(n >> 16)}).Read(b)
	return b
}

// resumed hashes data in writes of w bytes, saving the state at each cut and going on in a new Hasher.
func resumed(t testing.TB, data []byte, w int, cuts ...int) []byte {
	h := New()
	prev := 0
	for _, c := range append(cuts, len(data)) {
		for off := prev; off < c; off += w {
			h.Write(data[off:min(off+w, c)])
		}
		if c == len(data) {
			break
		}
		state, err := h.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		h = New()
		if err := h.UnmarshalBinary(state); err != nil {
			t.Fatal(err)
		}
		prev = c
	}
	return h.Sum(nil)
}

// Straight and resumed at any cut, the hash is the official one, so b3sum agrees.
func TestOfficialVectors(t *testing.T) {
	var v struct {
		Cases []struct {
			InputLen int    `json:"input_len"`
			Hash     string `json:"hash"` // extended output, the hash is its first 32 bytes
		} `json:"cases"`
	}
	if err := json.Unmarshal(vectorsJSON, &v); err != nil || len(v.Cases) == 0 {
		t.Fatalf("vectors: %v", err)
	}
	for _, c := range v.Cases {
		data := make([]byte, c.InputLen)
		for i := range data {
			data[i] = byte(i % 251)
		}
		want := c.Hash[:64]
		step := 1 // every cut, every 997th above 8 KiB
		if c.InputLen > 8193 {
			step = 997
		}
		for cut := 0; cut <= c.InputLen; cut += step {
			for _, w := range []int{c.InputLen + 1, 1000} {
				if got := hex.EncodeToString(resumed(t, data, w, cut)); got != want {
					t.Fatalf("%d bytes cut at %d in %d-byte writes: %s, want %s", c.InputLen, cut, w, got, want)
				}
			}
		}
	}
}

// Stripe and part edges, several resumes in a row, random cuts.
func TestSaveRestore(t *testing.T) {
	n := 3<<20 + 777
	data := random(n)
	want := blake3.Sum256(data)
	cuts := [][]int{{512 << 10}, {1 << 20}, {1<<20 + 1}, {1 << 20, 2 << 20, 3 << 20}, {512 << 10, 1 << 20, 1536 << 10}}
	r := rand.New(rand.NewPCG(1, 2))
	for range 50 {
		c := []int{r.IntN(n + 1), r.IntN(n + 1), r.IntN(n + 1)}
		slices.Sort(c)
		cuts = append(cuts, c)
	}
	for _, c := range cuts {
		for _, w := range []int{n + 1, 512 << 10, 1000} {
			if got := resumed(t, data, w, c...); !bytes.Equal(got, want[:]) {
				t.Fatalf("cut at %v in %d-byte writes: %x, want %x", c, w, got, want)
			}
		}
	}
}

func TestRejects(t *testing.T) {
	if bi, ok := debug.ReadBuildInfo(); !ok || !slices.ContainsFunc(bi.Deps, func(m *debug.Module) bool { return m.Path == "github.com/zeebo/blake3" }) {
		t.Error("zeebo's version isn't in the build info, the fingerprint is layout only")
	}
	h := New()
	h.Write([]byte("hello"))
	state, err := h.MarshalBinary()
	if err != nil || !Valid(state) {
		t.Fatalf("state: %v", err)
	}
	for name, b := range map[string][]byte{
		"empty":       nil,
		"truncated":   state[:len(state)-1],
		"long":        append(slices.Clone(state), 0),
		"other build": append([]byte{^state[0]}, state[1:]...),
	} {
		if err := New().UnmarshalBinary(b); err == nil {
			t.Errorf("%s state taken", name)
		}
	}
}

func FuzzSaveRestore(f *testing.F) {
	f.Add([]byte("hello"), uint32(2))
	f.Add(random(9000), uint32(8192))
	f.Fuzz(func(t *testing.T, data []byte, cut uint32) {
		want := blake3.Sum256(data)
		if got := resumed(t, data, len(data)+1, int(cut)%(len(data)+1)); !bytes.Equal(got, want[:]) {
			t.Fatalf("cut at %d of %d: %x, want %x", cut, len(data), got, want)
		}
	})
}
