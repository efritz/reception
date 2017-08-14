package reception

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
