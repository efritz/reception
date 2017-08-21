package reception

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"

	consul "github.com/hashicorp/consul/api"
	"golang.org/x/net/context"
)

type (
	consulClient struct {
		host string
		api  consulAPI
	}

	consulWatcher struct {
		name  string
		api   consulAPI
		index uint64
		stop  chan struct{}
	}
)

var ErrIllegalHost = errors.New("illegal host")

func DialConsul(addr, host string) (Client, error) {
	if host == "" {
		host = os.Getenv("HOST")
	}

	client, err := consul.NewClient(consul.DefaultConfig())
	if err != nil {
		return nil, err
	}

	shim := &consulShim{
		agent:   client.Agent(),
		catalog: client.Catalog(),
	}

	return newConsulClient(host, shim), nil
}

func newConsulClient(host string, api consulAPI) Client {
	return &consulClient{
		host: host,
		api:  api,
	}
}

func (c *consulClient) Register(service *Service) error {
	// TODO - find way to set unhealthy
	// TODO - add explicit deregistration

	if c.host == "" {
		return ErrIllegalHost
	}

	endpoint, err := makeServer(c.host)
	if err != nil {
		return err
	}

	return c.api.Register(&consul.AgentServiceRegistration{
		ID:      service.ID,
		Name:    service.Name,
		Address: service.Address,
		Port:    service.Port,
		Tags:    []string{string(service.SerializeAttributes())},
		// TODO - make configurable
		Check: &consul.AgentServiceCheck{
			HTTP:     endpoint,
			Timeout:  "1s",
			Interval: "2s",
			Status:   "passing",
			DeregisterCriticalServiceAfter: "30s",
		},
	})
}

func (c *consulClient) ListServices(name string) ([]*Service, error) {
	services, err := c.api.List(name)
	if err != nil {
		return nil, err
	}

	return mapConsulServices(services, name), nil
}

func (c *consulClient) NewWatcher(name string) Watcher {
	return &consulWatcher{
		name:  name,
		api:   c.api,
		index: 0,
		stop:  make(chan struct{}),
	}
}

func (w *consulWatcher) Start() (<-chan []*Service, error) {
	ch := make(chan []*Service)

	go func() {
		defer close(ch)

		for {
			services, ok := w.update()
			if !ok {
				return
			}

			// TODO - ignore if same
			ch <- services
		}
	}()

	return ch, nil
}

func (w *consulWatcher) update() ([]*Service, bool) {
	var (
		services []*consul.CatalogService
		err      error
	)

	ok, err := withContext(w.stop, func(ctx context.Context) error {
		services, w.index, err = w.api.Watch(w.name, w.index, ctx)
		return err
	})

	if err != nil {
		fmt.Printf("Whoops: %#v\n", err.Error())
		return nil, false
	}

	if !ok {
		return nil, false
	}

	return mapConsulServices(services, w.name), true
}

func (w *consulWatcher) Stop() error {
	close(w.stop)
	return nil
}

//
// Helpers

func mapConsulServices(services []*consul.CatalogService, name string) []*Service {
	serviceMap := map[int]*Service{}
	for _, service := range services {
		s := &Service{
			ID:      service.ServiceID,
			Name:    name,
			Address: service.ServiceAddress,
			Port:    service.ServicePort,
		}

		if len(service.ServiceTags) > 0 && s.parseAttributes([]byte(service.ServiceTags[0])) {
			serviceMap[int(service.CreateIndex)] = s
		}
	}

	return sortServiceMap(serviceMap)
}

func makeServer(host string) (string, error) {
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return "", err
	}

	port := listener.Addr().(*net.TCPAddr).Port
	addr := fmt.Sprintf("http://%s:%d/health", host, port)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", consulHealthEndpoint)

	go func() {
		// TODO - use a logger
		fmt.Printf("Running health check at %s\n", addr)
		http.Serve(listener, mux)
	}()

	return addr, nil
}

func consulHealthEndpoint(w http.ResponseWriter, r *http.Request) {
	// TODO - use a logger
	fmt.Printf("Consul performing health check\n")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"alive": true}`))
}
