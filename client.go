package reception

import (
	"encoding/json"
)

type (
	Client interface {
		Register(service *Service) error
		ListServices(name string) ([]*Service, error)
		NewWatcher(name string) Watcher
	}

	Watcher interface {
		Start() (<-chan []*Service, error)
		Stop() error
	}

	Service struct {
		ID       string
		Name     string
		Metadata Metadata
	}

	Metadata map[string]string
)

func (s *Service) SerializedMetadata() []byte {
	data, _ := json.Marshal(s.Metadata)
	return data
}

func parseMetadata(data []byte) (Metadata, bool) {
	metadata := Metadata{}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, false
	}

	return metadata, true
}
