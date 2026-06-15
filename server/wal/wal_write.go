package wal

import (
	"encoding/binary"
	"io"
)

// AppendTo serializes the log onto dst and returns the extended slice, using
// binary.LittleEndian.AppendUint* instead of reflection-based binary.Write.
// The byte layout is identical to the previous WriteTo output.
func (l *WriteLog) AppendTo(dst []byte) []byte {
	dst = append(dst, byte(WriteType))
	dst = binary.LittleEndian.AppendUint64(dst, l.timestamp)
	dst = appendString(dst, l.key)
	dst = appendString(dst, l.value)
	return dst
}

func (l *DeleteLog) AppendTo(dst []byte) []byte {
	dst = append(dst, byte(DeleteType))
	dst = binary.LittleEndian.AppendUint64(dst, l.timestamp)
	dst = appendString(dst, l.key)
	return dst
}

func (l *checkpointLog) AppendTo(dst []byte) []byte {
	dst = append(dst, byte(CheckpointType))
	dst = binary.LittleEndian.AppendUint64(dst, l.timestamp)
	return dst
}

// WriteTo writes the log's serialized bytes to w (satisfies io.WriterTo and
// keeps compaction working). It delegates to AppendTo so the on-disk format is
// shared with the flush path.
func (l *WriteLog) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(l.AppendTo(nil))
	return int64(n), err
}

func (l *DeleteLog) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(l.AppendTo(nil))
	return int64(n), err
}

func (l *checkpointLog) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(l.AppendTo(nil))
	return int64(n), err
}
