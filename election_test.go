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
		serviceChan       = make(chan *ServiceState)
		errChan           = make(chan error)
		registeredService *Service
		stopCalled        = false
		elector           = NewElector(client, "service", WithAttributes(map[string]string{
			"foo": "bar",
			"baz": "bonk",
		}))
	)

	defer close(errChan)

	client.register = func(service *Service, onDisconnect func(error)) error {
		registeredService = service
		return nil
	}

	client.newWatcher = func(service string) Watcher {
		return watcher
	}

	watcher.start = func() (<-chan *ServiceState, error) {
		return serviceChan, nil
	}

	watcher.stop = func() {
		stopCalled = true
	}

	go func() {
		errChan <- elector.Elect()
	}()

	serviceChan <- &ServiceState{}
	Consistently(errChan).ShouldNot(Receive())

	serviceChan <- &ServiceState{Services: []*Service{&Service{ID: "not there"}}}
	Consistently(errChan).ShouldNot(Receive())
	Eventually(func() bool { return elector.Leader().ID == "not there" }).Should(BeTrue())

	serviceChan <- &ServiceState{Services: []*Service{&Service{ID: "not first"}, &Service{ID: registeredService.ID}}}
	Consistently(errChan).ShouldNot(Receive())
	Eventually(func() bool { return elector.Leader().ID == "not first" }).Should(BeTrue())

	serviceChan <- &ServiceState{Services: []*Service{&Service{ID: registeredService.ID}}}
	Eventually(errChan).Should(Receive(BeNil()))
	Eventually(func() bool { return elector.Leader().ID == registeredService.ID }).Should(BeTrue())

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
		elector    = NewElector(client, "service")
	)

	defer close(errChan)

	client.newWatcher = func(service string) Watcher {
		return watcher
	}

	watcher.start = func() (<-chan *ServiceState, error) {
		return make(chan *ServiceState), nil
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
	register     func(service *Service, onDisconnect func(error)) error
	listServices func(name string) ([]*Service, error)
	newWatcher   func(name string) Watcher
}

func newMockClient() *mockClient {
	return &mockClient{
		register:     func(service *Service, onDisconnect func(error)) error { return nil },
		listServices: func(name string) ([]*Service, error) { return nil, nil },
		newWatcher:   func(name string) Watcher { return nil },
	}
}

func (c *mockClient) Register(service *Service, onDisconnect func(error)) error {
	return c.register(service, onDisconnect)
}

func (c *mockClient) ListServices(name string) ([]*Service, error) {
	return c.listServices(name)
}

func (c *mockClient) NewWatcher(name string) Watcher {
	return c.newWatcher(name)
}

type mockWatcher struct {
	start func() (<-chan *ServiceState, error)
	stop  func()
}

func newMockWatcher() *mockWatcher {
	return &mockWatcher{
		start: func() (<-chan *ServiceState, error) { return nil, nil },
		stop:  func() {},
	}
}

func (w *mockWatcher) Start() (<-chan *ServiceState, error) { return w.start() }
func (w *mockWatcher) Stop()                                { w.stop() }
