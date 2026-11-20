package serve

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"filescale/internal/b3"
	"filescale/internal/db"
)

// offset answers HEAD /name?upload=id: how much of the upload is stored, always a part boundary.
func (s *Serve) offset(w http.ResponseWriter, r *http.Request, name string, upload db.UUID) {
	rows, err := s.DB.UploadParts(r.Context(), name, upload)
	if err != nil {
		fail(w, err)
		return
	}
	if len(rows) == 0 {
		http.NotFound(w, r)
		return
	}
	total := rows[0].Total
	w.Header().Set("Upload-Offset", strconv.FormatInt(int64(len(recorded(rows, total)))*partSize(total), 10))
	w.Header().Set("Upload-Length", strconv.FormatInt(total, 10))
	w.WriteHeader(http.StatusOK)
}

// resumed returns the recorded parts before from, 409 with the offset to use if from isn't one.
func (s *Serve) resumed(ctx context.Context, w http.ResponseWriter, name string, upload db.UUID, from, total int64) ([]db.UploadPart, error) {
	rows, err := s.DB.UploadParts(ctx, name, upload)
	if err != nil {
		return nil, err
	}
	rows = recorded(rows, total)
	ps := partSize(total)
	if off := int64(len(rows)) * ps; from%ps != 0 || from > off {
		w.Header().Set("Upload-Offset", strconv.FormatInt(off, 10))
		return nil, &statusError{http.StatusConflict, fmt.Sprintf("resume at %d, or a part boundary before it", off)}
	}
	return rows[:from/ps], nil
}

// recorded is the run of parts from the first that belong to one upload of total bytes,
// each with a hash state this build can go on from (another zeebo version can't).
func recorded(rows []db.UploadPart, total int64) []db.UploadPart {
	for i, p := range rows {
		if p.Index != i || p.Total != total || p.Size != partSize(total) ||
			p.DataShards != rows[0].DataShards || p.ParityShards != rows[0].ParityShards || !b3.Valid(p.HashState) {
			return rows[:i]
		}
	}
	return rows
}

// contentRange parses "bytes <from>-<size-1>/<size>" for a body of n bytes.
func contentRange(h string, n int64) (from, size int64, err error) {
	var last int64
	if _, err := fmt.Sscanf(h, "bytes %d-%d/%d", &from, &last, &size); err != nil {
		return 0, 0, err
	}
	if from < 0 || from > last || last != size-1 || size-from != n {
		return 0, 0, errors.New("bad Content-Range")
	}
	return from, size, nil
}
