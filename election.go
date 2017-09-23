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
		client       Client
		name         string
		stop         chan struct{}
		attributes   Attributes
		onDisconnect func(error)
	}

	ElectorConfigFunc func(*elector)
)

var ErrElectionCanceled = errors.New("election canceled")

func NewElector(client Client, name string, configs ...ElectorConfigFunc) Elector {
	elector := &elector{
		client: client,
		name:   name,
		stop:   make(chan struct{}),
	}

	for _, f := range configs {
		f(elector)
	}

	return elector
}

func WithAttributes(attributes Attributes) ElectorConfigFunc {
	return func(e *elector) { e.attributes = attributes }
}

func WithDisconnectionCallback(onDisconnect func(error)) ElectorConfigFunc {
	return func(e *elector) { e.onDisconnect = onDisconnect }
}

func (e *elector) Elect() error {
	service := &Service{
		ID:         uuid.NewV4().String(),
		Name:       e.name,
		Attributes: e.attributes,
	}

	if err := e.client.Register(service, e.onDisconnect); err != nil {
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

		case state := <-ch:
			if state.Err != nil {
				return state.Err
			}

			if len(state.Services) > 0 && state.Services[0].ID == service.ID {
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
