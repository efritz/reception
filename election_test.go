package reception

import (
	"github.com/aphistic/sweet"
	. "github.com/onsi/gomega"
)

type ElectionSuite struct{}

func (s *ElectionSuite) TestElect(t sweet.T) {
	var (
		client            = newMockClient()
		watcher           = newMockWatcher()
		serviceChan       = make(chan []*Service)
		errChan           = make(chan error)
		registeredService *Service
		stopCalled        = false
		elector           = NewElector(client, "service", map[string]string{
			"foo": "bar",
			"baz": "bonk",
		})
	)

	defer close(errChan)

	client.register = func(service *Service) error {
		registeredService = service
		return nil
	}

	client.newWatcher = func(service string) Watcher {
		return watcher
	}

	watcher.start = func() (<-chan []*Service, error) {
		return serviceChan, nil
	}

	watcher.stop = func() {
		stopCalled = true
	}

	go func() {
		errChan <- elector.Elect()
	}()

	serviceChan <- []*Service{}
	Consistently(errChan).ShouldNot(Receive())

	serviceChan <- []*Service{&Service{ID: "wrong id"}}
	Consistently(errChan).ShouldNot(Receive())

	serviceChan <- []*Service{&Service{ID: registeredService.ID}}
	Eventually(errChan).Should(Receive(BeNil()))

	Expect(stopCalled).To(BeTrue())
	Expect(registeredService.Name).To(Equal("service"))
	Expect(registeredService.Attributes).To(Equal(Attributes(map[string]string{
		"foo": "bar",
		"baz": "bonk",
	})))
}

func (s *ElectionSuite) TestCancel(t sweet.T) {
	var (
		client     = newMockClient()
		watcher    = newMockWatcher()
		errChan    = make(chan error)
		stopCalled = false
		elector    = NewElector(client, "service", nil)
	)

	defer close(errChan)

	client.newWatcher = func(service string) Watcher {
		return watcher
	}

	watcher.start = func() (<-chan []*Service, error) {
		return make(chan []*Service), nil
	}

	watcher.stop = func() {
		stopCalled = true
	}

	go func() {
		errChan <- elector.Elect()
	}()

	Consistently(errChan).ShouldNot(Receive())
	elector.Cancel()
	Eventually(errChan).Should(Receive(Equal(ErrElectionCanceled)))
	Expect(stopCalled).To(BeTrue())
}

//
// Mocks

type mockClient struct {
	register     func(service *Service) error
	listServices func(name string) ([]*Service, error)
	newWatcher   func(name string) Watcher
}

func newMockClient() *mockClient {
	return &mockClient{
		register:     func(service *Service) error { return nil },
		listServices: func(name string) ([]*Service, error) { return nil, nil },
		newWatcher:   func(name string) Watcher { return nil },
	}
}

func (c *mockClient) Register(service *Service) error              { return c.register(service) }
func (c *mockClient) ListServices(name string) ([]*Service, error) { return c.listServices(name) }
func (c *mockClient) NewWatcher(name string) Watcher               { return c.newWatcher(name) }

type mockWatcher struct {
	start func() (<-chan []*Service, error)
	stop  func()
}

func newMockWatcher() *mockWatcher {
	return &mockWatcher{
		start: func() (<-chan []*Service, error) { return nil, nil },
		stop:  func() {},
	}
}

func (w *mockWatcher) Start() (<-chan []*Service, error) { return w.start() }
func (w *mockWatcher) Stop()                             { w.stop() }
