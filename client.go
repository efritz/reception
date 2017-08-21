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
		Stop()
	}

	Service struct {
		ID         string     `json:"-"`
		Name       string     `json:"-"`
		Address    string     `json:"address"`
		Port       int        `json:"port"`
		Attributes Attributes `json:"attributes"`
	}

	Attributes map[string]string
)

func (s *Service) serializeMetadata() []byte {
	data, _ := json.Marshal(s)
	return data
}

func (s *Service) serializeAttributes() []byte {
	data, _ := json.Marshal(s.Attributes)
	return data
}

func (s *Service) parseMetadata(data []byte) bool {
	return json.Unmarshal(data, &s) == nil
}

func (s *Service) parseAttributes(data []byte) bool {
	return json.Unmarshal(data, &s.Attributes) == nil
}
