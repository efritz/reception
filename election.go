package reception

import (
	"errors"

	"github.com/satori/go.uuid"
)

type (
	Elector interface {
		Elect() error
		Cancel()
	}

	elector struct {
		client     Client
		name       string
		attributes Attributes
		stop       chan struct{}
	}
)

var ErrElectionCanceled = errors.New("election canceled")

func NewElector(client Client, name string, attributes Attributes) Elector {
	return &elector{
		client:     client,
		name:       name,
		attributes: attributes,
		stop:       make(chan struct{}),
	}
}

func (e *elector) Elect() error {
	service := &Service{
		ID:         uuid.NewV4().String(),
		Name:       e.name,
		Attributes: e.attributes,
	}

	if err := e.client.Register(service); err != nil {
		return err
	}

	watcher := e.client.NewWatcher(e.name)
	ch, err := watcher.Start()
	if err != nil {
		return err
	}

	defer watcher.Stop()

loop:
	for {
		select {
		case <-e.stop:
			return ErrElectionCanceled

		case services := <-ch:
			if len(services) > 0 && services[0].ID == service.ID {
				break loop
			}
		}
	}

	go func() {
		for range ch {
		}
	}()

	return nil
}

func (e *elector) Cancel() {
	close(e.stop)
}
