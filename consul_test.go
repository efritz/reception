package reception

import (
	"errors"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/aphistic/sweet"
	consul "github.com/hashicorp/consul/api"
	. "github.com/onsi/gomega"
	"golang.org/x/net/context"
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

		result <- client.Register(&Service{
			ID:      "node-a",
			Name:    "service",
			Address: "localhost",
			Port:    1234,
			Attributes: map[string]string{
				"foo": "bar",
				"baz": "bonk",
			},
		})
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

	err := client.Register(&Service{
		ID:   "node-a",
		Name: "service",
		Attributes: map[string]string{
			"foo": "bar",
			"baz": "bonk",
		},
	})

	Expect(err).To(Equal(ErrIllegalHost))
}

func (s *ConsulSuite) TestRegisterError(t sweet.T) {
	var (
		api    = newMockConsulAPI()
		client = newConsulClient(api, WithHost("localhost"), WithLogger(NewNilLogger()))
	)

	api.register = func(registration *consul.AgentServiceRegistration) error {
		return errors.New("utoh")
	}

	err := client.Register(&Service{
		ID:   "node-a",
		Name: "service",
		Attributes: map[string]string{
			"foo": "bar",
			"baz": "bonk",
		},
	})

	Expect(err).To(MatchError("utoh"))
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
	Eventually(ch).Should(Receive(Equal([]*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
	})))

	childrenChan <- children[:2]
	Eventually(indices).Should(Receive(Equal(uint64(1))))
	Eventually(ch).Should(Receive(Equal([]*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
		&Service{ID: "node-b", Name: "service", Address: "localhost", Port: 5002, Attributes: map[string]string{"foo": "b"}},
	})))

	childrenChan <- children[:4]
	Eventually(indices).Should(Receive(Equal(uint64(2))))
	Eventually(ch).Should(Receive(Equal([]*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
		&Service{ID: "node-b", Name: "service", Address: "localhost", Port: 5002, Attributes: map[string]string{"foo": "b"}},
		&Service{ID: "node-c", Name: "service", Address: "localhost", Port: 5003, Attributes: map[string]string{"foo": "c"}},
		&Service{ID: "node-d", Name: "service", Address: "localhost", Port: 5004, Attributes: map[string]string{"foo": "d"}},
	})))

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

func (s *ConsulSuite) TestMakeCheckServer(t sweet.T) {
	endpoints := []string{}

	for i := 0; i < 10; i++ {
		endpoint, err := makeCheckServer("localhost", NewNilLogger())
		Expect(err).To(BeNil())
		checkConsulHealthEndpoint(endpoint)
		endpoints = append(endpoints, endpoint)
	}

	for i := 0; i < 10; i++ {
		for j := i + 1; j < 10; j++ {
			Expect(endpoints[i]).NotTo(Equal(endpoints[j]))
		}
	}
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

//
// Helpers

func checkConsulHealthEndpoint(endpoint string) {
	req, err := http.NewRequest("GET", endpoint, nil)
	Expect(err).To(BeNil())

	resp, err := http.DefaultClient.Do(req)
	Expect(err).To(BeNil())
	defer resp.Body.Close()

	data, _ := ioutil.ReadAll(resp.Body)
	Expect(resp.StatusCode).To(Equal(http.StatusOK))
	Expect(data).To(MatchJSON(`{"alive": true}`))
}
