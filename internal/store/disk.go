package store

import (
	"os"
	"time"

	"golang.org/x/sys/unix"
)

func bootTime() (time.Time, error) {
	var si unix.Sysinfo_t
	if err := unix.Sysinfo(&si); err != nil {
		return time.Time{}, err
	}
	return time.Now().Add(-time.Duration(si.Uptime) * time.Second), nil
}

func diskSpace(dir string) (free, total int64, err error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, 0, err
	}
	return int64(st.Bavail) * int64(st.Bsize), int64(st.Blocks) * int64(st.Bsize), nil
}

// preallocate reserves a shard's full size up front so it lands in one extent instead of
// interleaving with the other shards being written. KEEP_SIZE: a cut-off shard keeps its real
// length. Best effort.
func preallocate(f *os.File, n int64) { unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, n) }

// sequential tells the kernel a shard is read front to back: twice the readahead.
func sequential(f *os.File) { unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_SEQUENTIAL) }

func dontNeed(f *os.File) { unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED) }
