package reception

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	consul "github.com/hashicorp/consul/api"
	"golang.org/x/net/context"
)

type (
	consulClient struct {
		api    consulAPI
		config *consulConfig
	}

	consulWatcher struct {
		name  string
		api   consulAPI
		index uint64
		stop  chan struct{}
	}

	consulConfig struct {
		host                   string
		endpoint               string
		checkTimeout           string
		checkInterval          string
		checkDeregisterTimeout string
		logger                 Logger
	}

	ConsulConfig func(*consulConfig)
)

var ErrIllegalHost = errors.New("illegal host")

func DialConsul(addr string, configs ...ConsulConfig) (Client, error) {
	client, err := consul.NewClient(consul.DefaultConfig())
	if err != nil {
		return nil, err
	}

	shim := &consulShim{
		agent:   client.Agent(),
		catalog: client.Catalog(),
	}

	return newConsulClient(shim, configs...), nil
}

func newConsulClient(api consulAPI, configs ...ConsulConfig) Client {
	config := &consulConfig{
		host:                   os.Getenv("HOST"),
		endpoint:               "",
		checkTimeout:           "10s",
		checkInterval:          "5s",
		checkDeregisterTimeout: "30s",
		logger:                 &defaultLogger{},
	}

	for _, f := range configs {
		f(config)
	}

	return &consulClient{
		api:    api,
		config: config,
	}
}

func WithHost(host string) ConsulConfig {
	return func(c *consulConfig) { c.host = host }
}

func WithEndpoint(endpoint string) ConsulConfig {
	return func(c *consulConfig) { c.endpoint = endpoint }
}

func WithCheckTimeout(time.Duration) ConsulConfig {
	return func(c *consulConfig) { c.checkTimeout = fmt.Sprintf("%#v", c) }
}

func WithCheckInterval(time.Duration) ConsulConfig {
	return func(c *consulConfig) { c.checkInterval = fmt.Sprintf("%#v", c) }
}

func WithCheckDeregisterTimeout(time.Duration) ConsulConfig {
	return func(c *consulConfig) { c.checkDeregisterTimeout = fmt.Sprintf("%#v", c) }
}

func WithLogger(logger Logger) ConsulConfig {
	return func(c *consulConfig) { c.logger = logger }
}

//
// Client

func (c *consulClient) Register(service *Service) error {
	endpoint, err := makeCheckEndpoint(c.config.host, c.config.endpoint, c.config.logger)
	if err != nil {
		return err
	}

	return c.api.Register(&consul.AgentServiceRegistration{
		ID:      service.ID,
		Name:    service.Name,
		Address: service.Address,
		Port:    service.Port,
		Tags:    []string{string(service.serializeAttributes())},
		Check: &consul.AgentServiceCheck{
			HTTP:                           endpoint,
			Status:                         "passing",
			Timeout:                        c.config.checkTimeout,
			Interval:                       c.config.checkInterval,
			DeregisterCriticalServiceAfter: c.config.checkDeregisterTimeout,
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

func (w *consulWatcher) Start() (<-chan *ServiceState, error) {
	ch := make(chan *ServiceState)

	go func() {
		defer close(ch)

		for {
			var (
				lastIndex = w.index
				services  []*consul.CatalogService
				err       error
			)

			again, err := withContext(w.stop, func(ctx context.Context) error {
				services, w.index, err = w.api.Watch(w.name, w.index, ctx)
				return err
			})

			if err != nil {
				sendOrStop(ch, w.stop, &ServiceState{Err: err})
				return
			}

			if !again {
				return
			}

			if w.index != lastIndex && !sendOrStop(ch, w.stop, &ServiceState{Services: mapConsulServices(services, w.name)}) {
				return
			}
		}
	}()

	return ch, nil
}

func (w *consulWatcher) Stop() {
	close(w.stop)
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

func makeCheckEndpoint(host, endpoint string, logger Logger) (string, error) {
	if endpoint != "" {
		return endpoint, nil
	}

	if host == "" {
		return "", ErrIllegalHost
	}

	return makeCheckServer(host, logger)
}

func makeCheckServer(host string, logger Logger) (string, error) {
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return "", err
	}

	port := listener.Addr().(*net.TCPAddr).Port
	addr := fmt.Sprintf("http://%s:%d/health", host, port)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", makeCheckHTTPFunc(logger))

	go func() {
		logger.Printf("Running health check at %s\n", addr)
		http.Serve(listener, mux)
	}()

	return addr, nil
}

func makeCheckHTTPFunc(logger Logger) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		logger.Printf("Consul performing health check\n")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"alive": true}`))
	}
}
