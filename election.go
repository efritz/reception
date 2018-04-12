package reception

import (
	"errors"

	"github.com/satori/go.uuid"
)

type (
	Elector interface {
		// Elect will begin an election. The current process is put into
		// the pool of candidates and then waits until the current leader
		// (if one exists) disconnects from the cluster. This method will
		// block until the current process has been elected.
		Elect() error

		// Cancel the election and unblock the Elect function.
		Cancel()
	}

	elector struct {
		client       Client
		name         string
		stop         chan struct{}
		attributes   Attributes
		onDisconnect func(error)
	}

	// ElectorConfigFunc is provided to NewElector to change the default
	// elector parameters.
	ElectorConfigFunc func(*elector)
)

// ErrElectionCanceled occurs when an election is concurrently cancelled.
var ErrElectionCanceled = errors.New("election canceled")

// NewElector create an elector for the service using the given name and
// backing client.
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

// WithAttributes sets the attributes of the instance participating in the
// election.
func WithAttributes(attributes Attributes) ElectorConfigFunc {
	return func(e *elector) { e.attributes = attributes }
}

// WithDisconnectionCallback sets the callback function which is invoked if
// the backing client disconnects after the election has unblocked.
func WithDisconnectionCallback(onDisconnect func(error)) ElectorConfigFunc {
	return func(e *elector) { e.onDisconnect = onDisconnect }
}

func (e *elector) Elect() error {
	id, err := uuid.NewV4()
	if err != nil {
		return err
	}
	service := &Service{
		ID:         id.String(),
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
