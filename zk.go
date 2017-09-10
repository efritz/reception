package reception

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type (
	zkClient struct {
		conn   zkConn
		config *zkConfig
	}

	zkWatcher struct {
		prefix string
		name   string
		conn   zkConn
		stop   chan struct{}
	}

	zkConfig struct {
		prefix string
	}

	ZkConfig func(*zkConfig)
)

// _c_24fc775f3171da36dae47369165c78d4-7fe38486-0488-4335-8fcc-a456108ff6d0_0000000002
// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^|....................................|^^^^^^^^^^
//           Zookeeper Prefix         |              Server ID             |  Seq No

var servicePathPattern = regexp.MustCompile(`^_c_[A-Za-z0-9]{32}-.+-\d{10}$`)

func DialZk(addr string, configs ...ZkConfig) (Client, error) {
	conn, _, err := zk.Connect([]string{addr}, time.Second)
	if err != nil {
		return nil, err
	}

	shim := &zkShim{
		conn: conn,
	}

	return newZkClient(shim, configs...), nil
}

func DialExhibitor(addr string, configs ...ZkConfig) (Client, error) {
	zkAddr, err := chooseRandomServer(addr)
	if err != nil {
		return nil, err
	}

	return DialZk(zkAddr, configs...)
}

func newZkClient(conn zkConn, configs ...ZkConfig) Client {
	config := &zkConfig{
		prefix: "",
	}

	for _, f := range configs {
		f(config)
	}

	return &zkClient{
		conn:   conn,
		config: config,
	}
}

// TODO - overload somehow
func WithZkPrefix(prefix string) ZkConfig {
	return func(c *zkConfig) { c.prefix = prefix }
}

//
// Client

func (c *zkClient) Register(service *Service) error {
	if err := createZkPath(c.conn, makePath(c.config.prefix, service.Name)); err != nil {
		return err
	}

	return c.conn.CreateEphemeral(
		makePath(c.config.prefix, service.Name, fmt.Sprintf("%s-", service.ID)),
		service.serializeMetadata(),
	)
}

func (c *zkClient) ListServices(name string) ([]*Service, error) {
	for {
		paths, err := c.conn.Children(makePath(c.config.prefix, name))
		if err != nil {
			return nil, err
		}

		services, err := readZkServices(c.conn, c.config.prefix, name, paths)
		if err == zk.ErrNoNode {
			continue
		}

		return services, err
	}
}

func (c *zkClient) NewWatcher(name string) Watcher {
	return &zkWatcher{
		prefix: c.config.prefix,
		name:   name,
		conn:   c.conn,
		stop:   make(chan struct{}),
	}
}

func (w *zkWatcher) Start() (<-chan *ServiceState, error) {
	ch := make(chan *ServiceState)

	go func() {
		defer close(ch)

		for {
			paths, watch, err := w.conn.ChildrenW(makePath(w.prefix, w.name))
			if err != nil {
				sendOrStop(ch, w.stop, &ServiceState{Err: err})
				return
			}

			services, err := readZkServices(w.conn, w.prefix, w.name, paths)
			if err == zk.ErrNoNode {
				continue
			}

			if err != nil {
				sendOrStop(ch, w.stop, &ServiceState{Err: err})
				return
			}

			if !sendOrStop(ch, w.stop, &ServiceState{Services: services}) {
				return
			}

			select {
			case <-watch:
			case <-w.stop:
				return
			}
		}
	}()

	return ch, nil
}

func (w *zkWatcher) Stop() {
	close(w.stop)
}

//
// Helpers

func readZkServices(conn zkConn, prefix, name string, paths []string) ([]*Service, error) {
	serviceMap := map[int]*Service{}

	for _, path := range paths {
		id, sequenceNumber, ok := extractZkMeta(path)
		if !ok {
			continue
		}

		data, err := conn.Get(makePath(prefix, name, path))
		if err != nil {
			return nil, err
		}

		s := &Service{
			ID:   id,
			Name: name,
		}

		if s.parseMetadata(data) {
			serviceMap[sequenceNumber] = s
		}
	}

	return sortServiceMap(serviceMap), nil
}

func extractZkMeta(path string) (string, int, bool) {
	if !servicePathPattern.MatchString(path) {
		return "", 0, false
	}

	// Get text between _c_.{32}- and version
	id := path[36 : len(path)-11]

	// Get version (last 10 digits) and convert
	sequenceNumber, _ := strconv.Atoi(path[len(path)-10:])

	return id, sequenceNumber, true
}

func createZkPath(conn zkConn, path string) error {
	parts := strings.Split(path, "/")

	for i := 2; i <= len(parts); i++ {
		err := conn.Create(strings.Join(parts[:i], "/"), nil)
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}

	return nil
}
