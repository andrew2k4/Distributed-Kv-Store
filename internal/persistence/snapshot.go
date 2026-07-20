package persistence

import (
	"encoding/json"
	"fmt"
	"os"
)

type Snapshotable interface {
	GetAllData() map[string]string
}

type Wal interface {
	Truncate() error
}

func SaveSnapshot(path string, obj Snapshotable, wal Wal) error {
	data := obj.GetAllData()
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	err = encoder.Encode(data)
	if err != nil {
		return fmt.Errorf("failed to encode snapshot data: %w", err)
	}
	if err := wal.Truncate(); err != nil {
		return fmt.Errorf("failed to truncate wal: %w", err)
	}

	return nil
}





