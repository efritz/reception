package reception

import (
	"encoding/json"

	"github.com/google/uuid"
)

type (
	// Client provides abstractions for registering and querying the current set
	// of live instances of a service within a cluster.
	Client interface {
		// Register links the current process as an instance of the given service.
		// The on-disconnect function, if non-nil, is called if the connection to
		// the remote service discovery service is severed (this indicates the
		// possibliity that a node has seen the current process de-register from
		// the remote service).
		Register(service *Service, onDisconnect func(error)) error

		// ListServices returns all currently registered instances of the service
		// with the given name.
		ListServices(name string) ([]*Service, error)

		// NewWatcher creates a watch which is updated whenever the live node list
		// of the serfvice with the given name changes. Some implementations may
		// call this as a heartbeat, providing the same set of services as the
		// previous update.
		NewWatcher(name string) Watcher
	}

	// Watcher provides a mechanism to receive push-based updates about the current
	// set of live instances of a service within a cluster.
	Watcher interface {
		// Start creates a channel on which updates are pushed.
		Start() (<-chan *ServiceState, error)

		// Stop halts the background processes spawned by the Start method.
		Stop()
	}

	// Service holds metadata about an instance of a service.
	Service struct {
		ID         string     `json:"-"`
		Name       string     `json:"-"`
		Address    string     `json:"address"`
		Port       int        `json:"port"`
		Attributes Attributes `json:"attributes"`
	}

	// ServiceState contains either the current set of lvie instance of a service
	// or an error which occurred when querying the remote service discovery service.
	ServiceState struct {
		Services []*Service
		Err      error
	}

	// Attributes are additional metadata about a service.
	Attributes map[string]string
)

// MakeService will instantiate a Service instance with a random ID.
func MakeService(name, addr string, port int, attributes Attributes) (*Service, error) {
	return makeService("", name, addr, port, attributes)

}

func makeService(id, name, addr string, port int, attributes Attributes) (service *Service, err error) {
	if id == "" {
		id, err = makeID()
	}

	service = &Service{
		ID:         id,
		Name:       name,
		Address:    addr,
		Port:       port,
		Attributes: attributes,
	}

	return
}

func makeID() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}

	return id.String(), nil
}

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
