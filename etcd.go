package reception

import (
	"fmt"
	"time"

	etcd "github.com/coreos/etcd/client"
	"github.com/efritz/glock"
	"golang.org/x/net/context"
)

type (
	etcdClient struct {
		api    keysAPI
		config *etcdConfig
	}

	etcdWatcher struct {
		prefix string
		name   string
		api    keysAPI
		stop   chan struct{}
	}

	etcdConfig struct {
		prefix          string
		ttl             time.Duration
		refreshInterval time.Duration
		clock           glock.Clock
	}

	EtcdConfig func(*etcdConfig)
)

func DialEtcd(addr string, configs ...EtcdConfig) (Client, error) {
	client, err := etcd.New(etcd.Config{
		Endpoints:               []string{addr},
		Transport:               etcd.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	})

	if err != nil {
		return nil, err
	}

	shim := &etcdShim{
		api: etcd.NewKeysAPI(client),
	}

	return newEtcdClient(shim), nil
}

func newEtcdClient(api keysAPI, configs ...EtcdConfig) Client {
	config := &etcdConfig{
		prefix:          "",
		ttl:             time.Second * 30,
		refreshInterval: time.Second * 5,
		clock:           glock.NewRealClock(),
	}

	for _, f := range configs {
		f(config)
	}

	return &etcdClient{
		api:    api,
		config: config,
	}
}

// TODO - overload somehow
func WithEtcdPrefix(prefix string) EtcdConfig {
	return func(c *etcdConfig) { c.prefix = prefix }
}

func WithTTL(ttl time.Duration) EtcdConfig {
	return func(c *etcdConfig) { c.ttl = ttl }
}

func WithRefreshInterval(refreshInterval time.Duration) EtcdConfig {
	return func(c *etcdConfig) { c.refreshInterval = refreshInterval }
}

func WithClock(clock glock.Clock) EtcdConfig {
	return func(c *etcdConfig) { c.clock = clock }
}

//
// Client

func (c *etcdClient) Register(service *Service) error {
	var (
		root = makePath(c.config.prefix, service.Name)
		path = makePath(c.config.prefix, service.Name, service.ID)
		data = string(service.serializeMetadata())

		rootOpts = &etcd.SetOptions{PrevExist: etcd.PrevNoExist, Dir: true}
		leafOpts = &etcd.SetOptions{PrevExist: etcd.PrevNoExist, TTL: c.config.ttl}
		tickOpts = &etcd.SetOptions{Refresh: true, TTL: c.config.ttl}
	)

	if err := c.api.Set(root, "", rootOpts); err != nil {
		if !isNodeExists(err) {
			return err
		}
	}

	if err := c.api.Set(path, data, leafOpts); err != nil {
		return err
	}

	go func() {
		for {
			<-c.config.clock.After(c.config.refreshInterval)

			if err := c.api.Set(path, "", tickOpts); err != nil {
				fmt.Printf("Whoops: %#v\n", err.Error())
				return
			}
		}
	}()

	return nil
}

func (c *etcdClient) ListServices(name string) ([]*Service, error) {
	resp, err := c.api.Get(makePath(c.config.prefix, name))
	if err != nil {
		return nil, err
	}

	return mapEtcdServices(resp, name), nil
}

func (c *etcdClient) NewWatcher(name string) Watcher {
	return &etcdWatcher{
		prefix: c.config.prefix,
		name:   name,
		api:    c.api,
		stop:   make(chan struct{}),
	}
}

func (w *etcdWatcher) Start() (<-chan []*Service, error) {
	ch := make(chan []*Service)

	go func() {
		defer close(ch)

		for range makeEtcdEventChannel(w.api, makePath(w.prefix, w.name), w.stop) {
			resp, err := w.api.Get(makePath(w.prefix, w.name))
			if err != nil {
				fmt.Printf("Whoops: %#v\n", err.Error())
				return
			}

			select {
			case ch <- mapEtcdServices(resp, w.name):
			case <-w.stop:
				return
			}
		}
	}()

	return ch, nil
}

func (w *etcdWatcher) Stop() {
	close(w.stop)
}

//
// Helpers

func mapEtcdServices(resp *etcd.Response, name string) []*Service {
	serviceMap := map[int]*Service{}
	for _, node := range resp.Node.Nodes {
		s := &Service{
			ID:   node.Key,
			Name: name,
		}

		if s.parseMetadata([]byte(node.Value)) {
			serviceMap[int(node.CreatedIndex)] = s
		}
	}

	return sortServiceMap(serviceMap)
}

func isNodeExists(err error) bool {
	if etcdErr, ok := err.(etcd.Error); ok {
		return etcdErr.Code == etcd.ErrorCodeNodeExist
	}

	return false
}

func makeEtcdEventChannel(api keysAPI, path string, stop <-chan struct{}) <-chan struct{} {
	var (
		ch      = make(chan struct{})
		watcher = api.Watcher(path)
	)

	go func() {
		defer close(ch)

		ch <- struct{}{}

		for {
			ok, err := withContext(stop, func(ctx context.Context) error {
				if _, err := watcher.Next(ctx); err != nil {
					return err
				}

				return nil
			})

			if err != nil {
				fmt.Printf("Whoops: %#v\n", err.Error())
				return
			}

			if !ok {
				return
			}

			ch <- struct{}{}
		}
	}()

	return ch
}
