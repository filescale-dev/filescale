// Package b3 is BLAKE3 (github.com/zeebo/blake3) with a state that can be saved and
// restored, for resumable uploads.
package b3

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"unsafe"

	"github.com/zeebo/blake3"
)

// Hasher saves its state as the raw bytes of a blake3.Hasher. That's the whole state:
// it holds no pointers, and its own Clone is a plain copy.
type Hasher struct{ *blake3.Hasher }

func New() *Hasher { return &Hasher{blake3.New()} }

const size = unsafe.Sizeof(blake3.Hasher{})

// layout tells states of other builds apart: the struct's fields, offsets and types, the
// zeebo version and GOARCH. Empty if the struct holds pointers, which can't be copied as bytes.
var layout = fingerprint()

func fingerprint() []byte {
	var b strings.Builder
	if !describe(reflect.TypeOf(blake3.Hasher{}), &b) {
		return nil
	}
	b.WriteString(runtime.GOARCH)
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, m := range bi.Deps {
			if m.Path == "github.com/zeebo/blake3" {
				b.WriteString(m.Version)
			}
		}
	}
	sum := sha256.Sum256([]byte(b.String()))
	return sum[:8]
}

func describe(t reflect.Type, b *strings.Builder) bool {
	switch t.Kind() {
	case reflect.Struct:
		fmt.Fprintf(b, "{%d", t.Size())
		for i := range t.NumField() {
			f := t.Field(i)
			fmt.Fprintf(b, " %s@%d:", f.Name, f.Offset)
			if !describe(f.Type, b) {
				return false
			}
		}
		b.WriteByte('}')
	case reflect.Array:
		fmt.Fprintf(b, "[%d]", t.Len())
		return describe(t.Elem(), b)
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		fmt.Fprintf(b, "%s", t.Kind())
	default:
		return false
	}
	return true
}

func (h *Hasher) raw() []byte { return unsafe.Slice((*byte)(unsafe.Pointer(h.Hasher)), size) }

func (h *Hasher) MarshalBinary() ([]byte, error) {
	if layout == nil {
		return nil, errors.New("blake3.Hasher holds pointers, its state can't be saved")
	}
	return append(bytes.Clone(layout), h.raw()...), nil
}

// UnmarshalBinary refuses a state saved by another build.
func (h *Hasher) UnmarshalBinary(b []byte) error {
	if !Valid(b) {
		return errors.New("blake3 state from another build")
	}
	copy(h.raw(), b[len(layout):])
	return nil
}

// Valid reports whether this build can restore state b.
func Valid(b []byte) bool {
	return layout != nil && len(b) == len(layout)+int(size) && bytes.Equal(b[:len(layout)], layout)
}
