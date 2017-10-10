package reception

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/efritz/glock"
	consul "github.com/hashicorp/consul/api"
)

type (
	consulClient struct {
		api         consulAPI
		config      *consulConfig
		checkServer *checkServer
	}

	consulWatcher struct {
		name  string
		api   consulAPI
		index uint64
		stop  chan struct{}
	}

	consulConfig struct {
		host                   string
		port                   int
		checkTimeout           time.Duration
		checkInterval          time.Duration
		checkDeregisterTimeout time.Duration
		logger                 Logger
		clock                  glock.Clock
	}

	// ConsulConfigFunc is provided to DialEtcd to change the default
	// client parameters.
	ConsulConfigFunc func(*consulConfig)
)

// ErrNoConsulHealthCheck occurs when Consul has not contacted the health
// endpoint of a registered service for a duration (its deregister timeout).
var ErrNoConsulHealthCheck = errors.New("consul has not pinged in disconnect timeout")

// DialConsul creates a new Client by connecting to a Consul node.
func DialConsul(addr string, configs ...ConsulConfigFunc) (Client, error) {
	client, err := consul.NewClient(&consul.Config{Address: addr})
	if err != nil {
		return nil, err
	}

	shim := &consulShim{
		agent:   client.Agent(),
		catalog: client.Catalog(),
	}

	return newConsulClient(shim, configs...), nil
}

func newConsulClient(api consulAPI, configs ...ConsulConfigFunc) Client {
	config := &consulConfig{
		host:                   os.Getenv("HOST"),
		port:                   0,
		checkTimeout:           time.Second * 10,
		checkInterval:          time.Second * 5,
		checkDeregisterTimeout: time.Second * 30,
		logger:                 &defaultLogger{},
		clock:                  glock.NewRealClock(),
	}

	for _, f := range configs {
		f(config)
	}

	checkServer := newCheckServer(
		config.host,
		config.port,
		config.logger,
		config.clock,
	)

	checkServer.start()

	return &consulClient{
		api:         api,
		config:      config,
		checkServer: checkServer,
	}
}

// WithHost sets the host of the current process. This must be set before calling
// the Register function, otherwise the proper address (resolvable outside of the
// current machine) cannot be given to Consul for health check information.
func WithHost(host string) ConsulConfigFunc {
	return func(c *consulConfig) { c.host = host }
}

// WithPort sets the port on which the health check server should listen. By default,
// this will be a dynamically bound port (any free port on the machine).
func WithPort(port int) ConsulConfigFunc {
	return func(c *consulConfig) { c.port = port }
}

// WithCheckTimeout sets Consul's TTL for health checks to this process.
func WithCheckTimeout(timeout time.Duration) ConsulConfigFunc {
	return func(c *consulConfig) { c.checkTimeout = timeout }
}

// WithCheckInterval sets Consul's interval for health checks to this process.
func WithCheckInterval(timeout time.Duration) ConsulConfigFunc {
	return func(c *consulConfig) { c.checkInterval = timeout }
}

// WithCheckDeregisterTimeout sets the timeout after which Consul will consider the
// process unhealthy.
func WithCheckDeregisterTimeout(timeout time.Duration) ConsulConfigFunc {
	return func(c *consulConfig) { c.checkDeregisterTimeout = timeout }
}

// WithLogger sets the logger sets the logger which will print the access logs of the
// health check server.
func WithLogger(logger Logger) ConsulConfigFunc {
	return func(c *consulConfig) { c.logger = logger }
}

//
// Client

func (c *consulClient) Register(service *Service, onDisconnect func(error)) error {
	if onDisconnect == nil {
		onDisconnect = func(error) {}
	}

	ping := c.checkServer.register()

	select {
	case err := <-ping:
		if err != nil {
			return err
		}
	default:
	}

	go func() {
		for {
			select {
			case <-c.config.clock.After(c.config.checkDeregisterTimeout):
				onDisconnect(ErrNoConsulHealthCheck)

			case err, ok := <-ping:
				if !ok {
					return
				}

				if err != nil {
					onDisconnect(err)
				}
			}
		}
	}()

	return c.api.Register(&consul.AgentServiceRegistration{
		ID:      service.ID,
		Name:    service.Name,
		Address: service.Address,
		Port:    service.Port,
		Tags:    []string{string(service.serializeAttributes())},
		Check: &consul.AgentServiceCheck{
			HTTP:                           c.checkServer.addr,
			Status:                         "passing",
			Timeout:                        c.config.checkTimeout.String(),
			Interval:                       c.config.checkInterval.String(),
			DeregisterCriticalServiceAfter: c.config.checkDeregisterTimeout.String(),
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
