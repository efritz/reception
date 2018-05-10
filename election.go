package reception

import (
	"errors"
)

type (
	Elector interface {
		// Elect will begin an election. The current process is put into
		// the pool of candidates and then waits until the current leader
		// (if one exists) disconnects from the cluster. This method will
		// block until the current process has been elected.
		Elect() error

		// Leader will return the service that was elected in the most
		// recent election. This method will only return nil until the
		// goroutine in which Elect was called has received a response
		// from the remote server.
		Leader() *Service

		// Cancel the election and unblock the Elect function.
		Cancel()
	}

	elector struct {
		client       Client
		serviceID    string
		name         string
		host         string
		port         int
		attributes   Attributes
		onDisconnect func(error)
		leader       *Service
		stop         chan struct{}
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

// WithServiceID sets the service ID of the instance participating in the
// election. If one is not set, then one is randomly generated.
func WithServiceID(serviceID string) ElectorConfigFunc {
	return func(e *elector) { e.serviceID = serviceID }
}

// WithAttributes sets the attributes of the instance participating in the
// election.
func WithAttributes(attributes Attributes) ElectorConfigFunc {
	return func(e *elector) { e.attributes = attributes }
}

// WithHostAndPort sets the host and port of the instance participating in
// the election.
func WithHostAndPort(host string, port int) ElectorConfigFunc {
	return func(e *elector) {
		e.host = host
		e.port = port
	}
}

// WithDisconnectionCallback sets the callback function which is invoked if
// the backing client disconnects after the election has unblocked.
func WithDisconnectionCallback(onDisconnect func(error)) ElectorConfigFunc {
	return func(e *elector) { e.onDisconnect = onDisconnect }
}

func (e *elector) Elect() error {
	service, err := makeService(e.serviceID, e.name, e.host, e.port, e.attributes)
	if err != nil {
		return err
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

			if len(state.Services) > 0 {
				// Update leader
				e.leader = state.Services[0]

				// Compare against current client
				if e.leader.ID == service.ID {
					break loop
				}
			}
		}
	}

	go func() {
		for range ch {
		}
	}()

	return nil
}

func (e *elector) Leader() *Service {
	return e.leader
}

func (e *elector) Cancel() {
	close(e.stop)
}
