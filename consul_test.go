package reception

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/aphistic/sweet"
	"github.com/efritz/glock"
	consul "github.com/hashicorp/consul/api"
	. "github.com/onsi/gomega"
)

type ConsulSuite struct{}

func (s *ConsulSuite) TestRegister(t sweet.T) {
	var (
		api    = newMockConsulAPI()
		client = newConsulClient(api, WithHost("localhost"), WithLogger(NewNilLogger()))
	)

	testHost(api, client)
}

func (s *ConsulSuite) TestRegisterImplicitHost(t sweet.T) {
	os.Clearenv()
	os.Setenv("HOST", "localhost")

	var (
		api    = newMockConsulAPI()
		client = newConsulClient(api, WithLogger(NewNilLogger()))
	)

	testHost(api, client)
}

func testHost(api *mockConsulAPI, client Client) {
	var (
		setCalls     = make(chan *consul.AgentServiceRegistration)
		result       = make(chan error)
		registration *consul.AgentServiceRegistration
	)

	api.register = func(registration *consul.AgentServiceRegistration) error {
		setCalls <- registration
		return nil
	}

	go func() {
		defer close(result)

		service := &Service{
			ID:      "node-a",
			Name:    "service",
			Address: "localhost",
			Port:    1234,
			Attributes: map[string]string{
				"foo": "bar",
				"baz": "bonk",
			},
		}

		result <- client.Register(service, nil)
	}()

	Eventually(setCalls).Should(Receive(&registration))
	Eventually(result).Should(Receive(BeNil()))

	Expect(registration.ID).To(Equal("node-a"))
	Expect(registration.Name).To(Equal("service"))
	Expect(registration.Address).To(Equal("localhost"))
	Expect(registration.Port).To(Equal(1234))
	Expect(registration.Tags).To(HaveLen(1))
	Expect(registration.Tags[0]).To(MatchJSON(`{
		"foo": "bar",
		"baz": "bonk"
	}`))

	checkConsulHealthEndpoint(registration.Check.HTTP)
}

func (s *ConsulSuite) TestRegisterNoHost(t sweet.T) {
	os.Clearenv()

	var (
		api    = newMockConsulAPI()
		client = newConsulClient(api, WithLogger(NewNilLogger()))
	)

	service := &Service{
		ID:   "node-a",
		Name: "service",
		Attributes: map[string]string{
			"foo": "bar",
			"baz": "bonk",
		},
	}

	Expect(client.Register(service, nil)).To(Equal(ErrIllegalHost))
}

func (s *ConsulSuite) TestRegisterError(t sweet.T) {
	var (
		api    = newMockConsulAPI()
		client = newConsulClient(api, WithHost("localhost"), WithLogger(NewNilLogger()))
	)

	api.register = func(registration *consul.AgentServiceRegistration) error {
		return errors.New("utoh")
	}

	service := &Service{
		ID:   "node-a",
		Name: "service",
		Attributes: map[string]string{
			"foo": "bar",
			"baz": "bonk",
		},
	}

	Expect(client.Register(service, nil)).To(MatchError("utoh"))
}

func (s *ConsulSuite) TestRegisterDisconnect(t sweet.T) {
	var (
		api             = newMockConsulAPI()
		clock           = glock.NewMockClock()
		setCalls        = make(chan *consul.AgentServiceRegistration)
		disconnectCalls = make(chan error)
		registration    *consul.AgentServiceRegistration
		client          = newConsulClient(
			api,
			WithHost("localhost"),
			WithLogger(NewNilLogger()),
			WithCheckDeregisterTimeout(time.Minute),
			withConsulClock(clock),
		)
	)

	defer close(disconnectCalls)

	api.register = func(registration *consul.AgentServiceRegistration) error {
		setCalls <- registration
		return nil
	}

	go func() {
		service := &Service{
			ID:      "node-a",
			Name:    "service",
			Address: "localhost",
			Port:    1234,
			Attributes: map[string]string{
				"foo": "bar",
				"baz": "bonk",
			},
		}

		client.Register(service, func(err error) {
			disconnectCalls <- err
		})
	}()

	Eventually(setCalls).Should(Receive(&registration))

	checkConsulHealthEndpoint(registration.Check.HTTP)
	clock.Advance(time.Minute / 2)
	checkConsulHealthEndpoint(registration.Check.HTTP)
	clock.Advance(time.Minute / 2)
	Consistently(disconnectCalls).ShouldNot(Receive())
	clock.Advance(time.Minute / 2)
	Eventually(disconnectCalls).Should(Receive(Equal(ErrNoConsulHealthCheck)))

}

func (s *ConsulSuite) TestListServices(t sweet.T) {
	var (
		api        = newMockConsulAPI()
		client     = newConsulClient(api)
		calledName string
	)

	api.list = func(name string) ([]*consul.CatalogService, error) {
		calledName = name

		return []*consul.CatalogService{
			&consul.CatalogService{ServiceID: "node-d", ServiceAddress: "localhost", ServicePort: 5004, ServiceTags: []string{`{"foo": "d"}`}, CreateIndex: 4},
			&consul.CatalogService{ServiceID: "node-b", ServiceAddress: "localhost", ServicePort: 5002, ServiceTags: []string{`{"foo": "b"}`}, CreateIndex: 2},
			&consul.CatalogService{ServiceID: "node-a", ServiceAddress: "localhost", ServicePort: 5001, ServiceTags: []string{`{"foo": "a"}`}, CreateIndex: 1},
			&consul.CatalogService{ServiceID: "node-c", ServiceAddress: "localhost", ServicePort: 5003, ServiceTags: []string{`{"foo": "c"}`}, CreateIndex: 3},
		}, nil
	}

	services, err := client.ListServices("service")

	Expect(err).To(BeNil())
	Expect(services).To(Equal([]*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
		&Service{ID: "node-b", Name: "service", Address: "localhost", Port: 5002, Attributes: map[string]string{"foo": "b"}},
		&Service{ID: "node-c", Name: "service", Address: "localhost", Port: 5003, Attributes: map[string]string{"foo": "c"}},
		&Service{ID: "node-d", Name: "service", Address: "localhost", Port: 5004, Attributes: map[string]string{"foo": "d"}},
	}))

	Expect(calledName).To(Equal("service"))
}

func (s *ConsulSuite) TestListServicesError(t sweet.T) {
	var (
		api    = newMockConsulAPI()
		client = newConsulClient(api)
	)

	api.list = func(path string) ([]*consul.CatalogService, error) {
		return nil, errors.New("utoh")
	}

	_, err := client.ListServices("service")
	Expect(err).To(MatchError("utoh"))
}

func (s *ConsulSuite) TestWatcher(t sweet.T) {
	var (
		api          = newMockConsulAPI()
		client       = newConsulClient(api)
		watcher      = client.NewWatcher("service")
		indices      = make(chan uint64, 1)
		childrenChan = make(chan []*consul.CatalogService)
		children     = []*consul.CatalogService{
			&consul.CatalogService{ServiceID: "node-a", ServiceAddress: "localhost", ServicePort: 5001, ServiceTags: []string{`{"foo": "a"}`}, CreateIndex: 1},
			&consul.CatalogService{ServiceID: "node-b", ServiceAddress: "localhost", ServicePort: 5002, ServiceTags: []string{`{"foo": "b"}`}, CreateIndex: 2},
			&consul.CatalogService{ServiceID: "node-c", ServiceAddress: "localhost", ServicePort: 5003, ServiceTags: []string{`{"foo": "c"}`}, CreateIndex: 3},
			&consul.CatalogService{ServiceID: "node-d", ServiceAddress: "localhost", ServicePort: 5004, ServiceTags: []string{`{"foo": "d"}`}, CreateIndex: 4},
		}
	)

	defer close(indices)
	defer close(childrenChan)

	api.watch = func(name string, index uint64, ctx context.Context) ([]*consul.CatalogService, uint64, error) {
		indices <- index

		select {
		case <-ctx.Done():
			return nil, 0, errors.New("Canceled")

		case c := <-childrenChan:
			return c, index + 1, nil
		}
	}

	ch, err := watcher.Start()
	Expect(err).To(BeNil())

	childrenChan <- children[:1]
	Eventually(indices).Should(Receive(Equal(uint64(0))))
	Eventually(ch).Should(Receive(Equal(&ServiceState{Services: []*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
	}})))

	childrenChan <- children[:2]
	Eventually(indices).Should(Receive(Equal(uint64(1))))
	Eventually(ch).Should(Receive(Equal(&ServiceState{Services: []*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
		&Service{ID: "node-b", Name: "service", Address: "localhost", Port: 5002, Attributes: map[string]string{"foo": "b"}},
	}})))

	childrenChan <- children[:4]
	Eventually(indices).Should(Receive(Equal(uint64(2))))
	Eventually(ch).Should(Receive(Equal(&ServiceState{Services: []*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
		&Service{ID: "node-b", Name: "service", Address: "localhost", Port: 5002, Attributes: map[string]string{"foo": "b"}},
		&Service{ID: "node-c", Name: "service", Address: "localhost", Port: 5003, Attributes: map[string]string{"foo": "c"}},
		&Service{ID: "node-d", Name: "service", Address: "localhost", Port: 5004, Attributes: map[string]string{"foo": "d"}},
	}})))

	watcher.Stop()
	Eventually(ch).Should(BeClosed())
}

func (s *ConsulSuite) TestWatcherNoUpdates(t sweet.T) {
	var (
		api          = newMockConsulAPI()
		client       = newConsulClient(api)
		watcher      = client.NewWatcher("service")
		childrenChan = make(chan []*consul.CatalogService)
		children     = []*consul.CatalogService{
			&consul.CatalogService{ServiceID: "node-a", ServiceAddress: "localhost", ServicePort: 5001, ServiceTags: []string{`{"foo": "a"}`}, CreateIndex: 1},
		}
	)

	defer close(childrenChan)

	api.watch = func(name string, index uint64, ctx context.Context) ([]*consul.CatalogService, uint64, error) {
		select {
		case <-ctx.Done():
			return nil, 0, errors.New("Canceled")

		case c := <-childrenChan:
			return c, index, nil
		}
	}

	ch, err := watcher.Start()
	Expect(err).To(BeNil())

	for i := 0; i < 5; i++ {
		childrenChan <- children
		Eventually(ch).ShouldNot(Receive())
	}

	watcher.Stop()
	Eventually(ch).Should(BeClosed())
}

func (s *ConsulSuite) TestMapConsulServices(t sweet.T) {
	serviceList := []*consul.CatalogService{
		&consul.CatalogService{ServiceID: "a", ServiceAddress: "localhost", ServicePort: 5001, ServiceTags: []string{`{"name": "a"}`}, CreateIndex: 3},
		&consul.CatalogService{ServiceID: "b", ServiceAddress: "localhost", ServicePort: 5002, ServiceTags: []string{`{"name": "b"}`}, CreateIndex: 2},
		&consul.CatalogService{ServiceID: "c", ServiceAddress: "localhost", ServicePort: 5003, ServiceTags: []string{`{"name": "c"}`}, CreateIndex: 1},
		&consul.CatalogService{ServiceID: "d", ServiceAddress: "localhost", ServicePort: 5004, ServiceTags: []string{`{"name": "d"}`}, CreateIndex: 5},
		&consul.CatalogService{ServiceID: "e", ServiceAddress: "localhost", ServicePort: 5005, ServiceTags: []string{`{"name": 'e'}`}, CreateIndex: 4},
		&consul.CatalogService{ServiceID: "f", ServiceAddress: "localhost", ServicePort: 5006, CreateIndex: 4},
	}

	services := mapConsulServices(serviceList, "test-service")
	Expect(services).To(HaveLen(4))
	Expect(services[0].ID).To(Equal("c"))
	Expect(services[1].ID).To(Equal("b"))
	Expect(services[2].ID).To(Equal("a"))
	Expect(services[3].ID).To(Equal("d"))
	Expect(services[0].Address).To(Equal("localhost"))
	Expect(services[1].Address).To(Equal("localhost"))
	Expect(services[2].Address).To(Equal("localhost"))
	Expect(services[3].Address).To(Equal("localhost"))
	Expect(services[0].Port).To(Equal(5003))
	Expect(services[1].Port).To(Equal(5002))
	Expect(services[2].Port).To(Equal(5001))
	Expect(services[3].Port).To(Equal(5004))
	Expect(services[0].Attributes).To(Equal(Attributes(map[string]string{"name": "c"})))
	Expect(services[1].Attributes).To(Equal(Attributes(map[string]string{"name": "b"})))
	Expect(services[2].Attributes).To(Equal(Attributes(map[string]string{"name": "a"})))
	Expect(services[3].Attributes).To(Equal(Attributes(map[string]string{"name": "d"})))
}

//
// Mocks

type mockConsulAPI struct {
	register func(registration *consul.AgentServiceRegistration) error
	list     func(name string) ([]*consul.CatalogService, error)
	watch    func(name string, index uint64, ctx context.Context) ([]*consul.CatalogService, uint64, error)
}

func newMockConsulAPI() *mockConsulAPI {
	return &mockConsulAPI{
		register: func(*consul.AgentServiceRegistration) error { return nil },
		list:     func(string) ([]*consul.CatalogService, error) { return nil, nil },
		watch:    func(string, uint64, context.Context) ([]*consul.CatalogService, uint64, error) { return nil, 0, nil },
	}
}

func (c *mockConsulAPI) Register(registration *consul.AgentServiceRegistration) error {
	return c.register(registration)
}

func (c *mockConsulAPI) List(name string) ([]*consul.CatalogService, error) {
	return c.list(name)
}

func (c *mockConsulAPI) Watch(name string, index uint64, ctx context.Context) ([]*consul.CatalogService, uint64, error) {
	return c.watch(name, index, ctx)
}
