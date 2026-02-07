package persistence

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

type WAL struct {
	file *os.File
	mu   sync.Mutex
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
	}, nil
}

func (w *WAL) Set(key, value string) error {
	return w.write("SET", key, value)
}

func (w *WAL) Delete(key string) error {
	return w.write("DEL", key, "")
}

func (w *WAL) write(op, key, value string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// save data in wal.log as SET key value or DEL key value
	var line string
	if op == "SET" {
		line = fmt.Sprintf("SET %s %s\n", key, value)
	} else {
		line = fmt.Sprintf("DEL %s\n", key)
	}

	if _, err := w.file.WriteString(line); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) Recovery(apply func(op, key, value string)) error {
	scanner := bufio.NewScanner(w.file)

	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), " ", 3)
		if parts[0] == "SET" && len(parts) == 3 {
			apply("SET", parts[1], parts[2])
		} else if parts[0] == "DEL" && len(parts) >= 2 {
			apply("DEL", parts[1], "")
		}
	}

	return scanner.Err()
}
