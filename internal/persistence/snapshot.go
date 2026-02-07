package persistence

import (
	"encoding/json"
	"os"
)

type Snapshotable interface {
	GetAllData() map[string]string
}

func SaveSnapshot(path string, obj Snapshotable) error {
	data := obj.GetAllData()

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") 
	return encoder.Encode(data)
}




