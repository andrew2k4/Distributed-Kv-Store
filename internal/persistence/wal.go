package persistence

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	opSet byte = 0
	opDel byte = 1
)

type WAL struct {
	file       *os.File
	mu         sync.Mutex
	writeCount int
	syncCount  int
	offset     int64
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &WAL{
		file:      file,
		syncCount: 100,
		offset:    info.Size(),
	}, nil
}

func (w *WAL) Set(key, value string) error {
	return w.write(opSet, key, value)
}

func (w *WAL) Delete(key string) error {
	return w.write(opDel, key, "")
}

func (w *WAL) write(op byte, key, value string) error {
	record := make([]byte, 0, 9+len(key)+len(value))
	record = append(record, op)

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(key)))
	record = append(record, lenBuf[:]...)
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(value)))
	record = append(record, lenBuf[:]...)

	record = append(record, key...)
	record = append(record, value...)

	w.mu.Lock()
	defer w.mu.Unlock()

	n, err := w.file.Write(record)
	if err != nil {
		return err
	}
	w.offset += int64(n)

	w.writeCount++
	if w.writeCount >= w.syncCount {
		w.writeCount = 0
		return w.file.Sync()
	}

	return nil
}

func (w *WAL) Recovery(apply func(op, key, value string)) error {
	reader := bufio.NewReader(w.file)

	for {
		header := make([]byte, 9)
		if _, err := io.ReadFull(reader, header); err != nil {
			break
		}

		keyLen := binary.BigEndian.Uint32(header[1:5])
		valueLen := binary.BigEndian.Uint32(header[5:9])

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			break
		}
		value := make([]byte, valueLen)
		if _, err := io.ReadFull(reader, value); err != nil {
			break
		}

		switch header[0] {
		case opSet:
			apply("SET", string(key), string(value))
		case opDel:
			apply("DEL", string(key), "")
		}
	}

	return nil
}

func (w *WAL) Offset() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}

func (w *WAL) TruncateTo(uptoOffset int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	path := w.file.Name()

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close wal: %w", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read wal: %w", err)
	}

	if uptoOffset > int64(len(data)) {
		uptoOffset = int64(len(data))
	}
	remaining := data[uptoOffset:]

	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("truncate wal: %w", err)
	}
	if _, err := file.Write(remaining); err != nil {
		file.Close()
		return fmt.Errorf("write remaining wal: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close wal: %w", err)
	}

	file, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("reopen wal: %w", err)
	}

	w.file = file
	w.offset = int64(len(remaining))
	return nil
}
