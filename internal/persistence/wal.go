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
	writeCount int
	syncCount int
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
		syncCount: 100,
	}, nil
}

func (w *WAL) Set(key, value string) error {
	return w.write("SET", key, value)
}

func (w *WAL) Delete(key string) error {
	return w.write("DEL", key, "")
}

func (w *WAL) write(op, key, value string) error {
	
	// save data in wal.log as SET key value or DEL key value
	var line string
	if op == "SET" {
		line = fmt.Sprintf("SET %s %s\n", key, value)
	} else {
		line = fmt.Sprintf("DEL %s\n", key)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.file.WriteString(line); err != nil {
		return err
	}
	w.writeCount ++
	if(w.writeCount >= w.syncCount){
		w.writeCount = 0
		return w.file.Sync()
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

func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Close(); err != nil{
		return fmt.Errorf("close wal: %w", err)
	}

	path := w.file.Name()
	file, err := os.OpenFile(path,os.O_APPEND|os.O_WRONLY|os.O_TRUNC ,0644)

	if(err != nil){
		return fmt.Errorf("truncate wal: %w", err)
	}

	w.file = file
	return nil
}
