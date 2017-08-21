package reception

import (
	"github.com/aphistic/sweet"
	"github.com/samuel/go-zookeeper/zk"

	. "github.com/onsi/gomega"
)

type ZkSuite struct{}

func (s *ZkSuite) TestRegister(t sweet.T) {
	var (
		conn          = NewMockZkConn()
		client        = newZkClient(conn, WithZkPrefix("prefix"))
		paths         = []string{}
		ephemeralPath string
		ephemeralData []byte
	)

	conn.create = func(path string, data []byte) error {
		paths = append(paths, path)
		return nil
	}

	conn.createEphemeral = func(path string, data []byte) error {
		ephemeralPath = path
		ephemeralData = data
		return nil
	}

	err := client.Register(&Service{
		ID:      "node-a",
		Name:    "service",
		Address: "localhost",
		Port:    1234,
		Attributes: map[string]string{
			"foo": "bar",
			"baz": "bonk",
		},
	})

	Expect(err).To(BeNil())
	Expect(paths).To(ContainElement(Equal("/prefix/service")))
	Expect(ephemeralPath).To(Equal("/prefix/service/node-a-"))
	Expect(ephemeralData).To(MatchJSON(`{
		"address": "localhost",
		"port": 1234,
		"attributes": {
			"foo": "bar",
			"baz": "bonk"
		}
	}`))
}

func (s *ZkSuite) TestRegisterError(t sweet.T) {
	var (
		conn   = NewMockZkConn()
		client = newZkClient(conn, WithZkPrefix("prefix"))
	)

	conn.createEphemeral = func(path string, data []byte) error {
		return zk.ErrUnknown
	}

	err := client.Register(&Service{
		ID:      "node-a",
		Name:    "service",
		Address: "localhost",
		Port:    1234,
		Attributes: map[string]string{
			"foo": "bar",
			"baz": "bonk",
		},
	})

	Expect(err).To(Equal(zk.ErrUnknown))
}

func (s *ZkSuite) TestListServices(t sweet.T) {
	var (
		conn       = NewMockZkConn()
		client     = newZkClient(conn, WithZkPrefix("prefix"))
		calledPath string
		data       = map[string][]byte{
			"/prefix/service/_c_345e7574c5464a76bf5f0c5b77ed8de7-node-a-0000000001": []byte(`{"address": "localhost", "port": 5001, "attributes": {"foo": "a"}}`),
			"/prefix/service/_c_ebd335e2080f406f8967818107ec71bb-node-b-0000000002": []byte(`{"address": "localhost", "port": 5002, "attributes": {"foo": "b"}}`),
			"/prefix/service/_c_64a32eb0990843dabc9332856a2aec7e-node-c-0000000123": []byte(`{"address": "localhost", "port": 5003, "attributes": {"foo": "c"}}`),
			"/prefix/service/_c_97383ee5c96b43f89d71d5f80a0b1927-node-d-9000000000": []byte(`{"address": "localhost", "port": 5004, "attributes": {"foo": "d"}}`),
		}
	)

	conn.children = func(path string) ([]string, error) {
		calledPath = path

		return []string{
			"_c_ebd335e2080f406f8967818107ec71bb-node-b-0000000002",
			"_c_64a32eb0990843dabc9332856a2aec7e-node-c-0000000123",
			"_c_97383ee5c96b43f89d71d5f80a0b1927-node-d-9000000000",
			"_c_345e7574c5464a76bf5f0c5b77ed8de7-node-a-0000000001",
		}, nil
	}

	conn.get = func(path string) ([]byte, error) {
		return data[path], nil
	}

	services, err := client.ListServices("service")

	Expect(err).To(BeNil())
	Expect(services).To(Equal([]*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
		&Service{ID: "node-b", Name: "service", Address: "localhost", Port: 5002, Attributes: map[string]string{"foo": "b"}},
		&Service{ID: "node-c", Name: "service", Address: "localhost", Port: 5003, Attributes: map[string]string{"foo": "c"}},
		&Service{ID: "node-d", Name: "service", Address: "localhost", Port: 5004, Attributes: map[string]string{"foo": "d"}},
	}))

	Expect(calledPath).To(Equal("/prefix/service"))
}

func (s *ZkSuite) TestListServicesGetRace(t sweet.T) {
	var (
		conn          = NewMockZkConn()
		client        = newZkClient(conn, WithZkPrefix("prefix"))
		childrenCalls = 0
		getCalls      = 0

		data = map[string][]byte{
			"/prefix/service/_c_345e7574c5464a76bf5f0c5b77ed8de7-node-a-0000000001": []byte(`{"address": "localhost", "port": 5001, "attributes": {"foo": "a"}}`),
			"/prefix/service/_c_ebd335e2080f406f8967818107ec71bb-node-b-0000000002": []byte(`{"address": "localhost", "port": 5002, "attributes": {"foo": "b"}}`),
			"/prefix/service/_c_64a32eb0990843dabc9332856a2aec7e-node-c-0000000123": []byte(`{"address": "localhost", "port": 5003, "attributes": {"foo": "c"}}`),
			"/prefix/service/_c_97383ee5c96b43f89d71d5f80a0b1927-node-d-9000000000": nil,
		}

		children = [][]string{
			[]string{
				"_c_ebd335e2080f406f8967818107ec71bb-node-b-0000000002",
				"_c_64a32eb0990843dabc9332856a2aec7e-node-c-0000000123",
				"_c_97383ee5c96b43f89d71d5f80a0b1927-node-d-9000000000",
				"_c_345e7574c5464a76bf5f0c5b77ed8de7-node-a-0000000001",
			}, []string{
				"_c_ebd335e2080f406f8967818107ec71bb-node-b-0000000002",
				"_c_64a32eb0990843dabc9332856a2aec7e-node-c-0000000123",
				"_c_345e7574c5464a76bf5f0c5b77ed8de7-node-a-0000000001",
			},
		}
	)

	conn.children = func(path string) ([]string, error) {
		temp := children[childrenCalls]
		childrenCalls++
		return temp, nil
	}

	conn.get = func(path string) ([]byte, error) {
		getCalls++
		if data[path] == nil {
			return nil, zk.ErrNoNode
		}

		return data[path], nil
	}

	services, err := client.ListServices("service")

	Expect(err).To(BeNil())
	Expect(services).To(Equal([]*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
		&Service{ID: "node-b", Name: "service", Address: "localhost", Port: 5002, Attributes: map[string]string{"foo": "b"}},
		&Service{ID: "node-c", Name: "service", Address: "localhost", Port: 5003, Attributes: map[string]string{"foo": "c"}},
	}))

	Expect(childrenCalls).To(Equal(2))
	Expect(getCalls).To(Equal(6))
}

func (s *ZkSuite) TestListServicesError(t sweet.T) {
	var (
		conn   = NewMockZkConn()
		client = newZkClient(conn, WithZkPrefix("prefix"))
	)

	conn.children = func(path string) ([]string, error) {
		return []string{
			"_c_ebd335e2080f406f8967818107ec71bb-node-b-0000000002",
			"_c_64a32eb0990843dabc9332856a2aec7e-node-c-0000000123",
			"_c_97383ee5c96b43f89d71d5f80a0b1927-node-d-9000000000",
			"_c_345e7574c5464a76bf5f0c5b77ed8de7-node-a-0000000001",
		}, nil
	}

	conn.get = func(path string) ([]byte, error) {
		return nil, zk.ErrUnknown
	}

	_, err := client.ListServices("service")
	Expect(err).To(Equal(zk.ErrUnknown))
}

func (s *ZkSuite) TestWatcher(t sweet.T) {
	var (
		conn         = NewMockZkConn()
		client       = newZkClient(conn, WithZkPrefix("prefix"))
		watcher      = client.NewWatcher("service")
		update       = make(chan struct{})
		childrenChan = make(chan []string, 3)
		calledPath   string
		data         = map[string][]byte{
			"/prefix/service/_c_345e7574c5464a76bf5f0c5b77ed8de7-node-a-0000000001": []byte(`{"address": "localhost", "port": 5001, "attributes": {"foo": "a"}}`),
			"/prefix/service/_c_ebd335e2080f406f8967818107ec71bb-node-b-0000000002": []byte(`{"address": "localhost", "port": 5002, "attributes": {"foo": "b"}}`),
			"/prefix/service/_c_64a32eb0990843dabc9332856a2aec7e-node-c-0000000123": []byte(`{"address": "localhost", "port": 5003, "attributes": {"foo": "c"}}`),
			"/prefix/service/_c_97383ee5c96b43f89d71d5f80a0b1927-node-d-9000000000": []byte(`{"address": "localhost", "port": 5004, "attributes": {"foo": "d"}}`),
		}

		children = []string{
			"_c_345e7574c5464a76bf5f0c5b77ed8de7-node-a-0000000001",
			"_c_ebd335e2080f406f8967818107ec71bb-node-b-0000000002",
			"_c_64a32eb0990843dabc9332856a2aec7e-node-c-0000000123",
			"_c_97383ee5c96b43f89d71d5f80a0b1927-node-d-9000000000",
		}
	)

	for _, i := range []int{1, 2, 4} {
		childrenChan <- children[:i]
	}

	conn.childrenW = func(path string) ([]string, <-chan struct{}, error) {
		calledPath = path
		return <-childrenChan, update, nil
	}

	conn.get = func(path string) ([]byte, error) {
		return data[path], nil
	}

	ch, err := watcher.Start()
	Expect(err).To(BeNil())

	Eventually(ch).Should(Receive(Equal([]*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
	})))

	update <- struct{}{}
	Eventually(ch).Should(Receive(Equal([]*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
		&Service{ID: "node-b", Name: "service", Address: "localhost", Port: 5002, Attributes: map[string]string{"foo": "b"}},
	})))

	update <- struct{}{}
	Eventually(ch).Should(Receive(Equal([]*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
		&Service{ID: "node-b", Name: "service", Address: "localhost", Port: 5002, Attributes: map[string]string{"foo": "b"}},
		&Service{ID: "node-c", Name: "service", Address: "localhost", Port: 5003, Attributes: map[string]string{"foo": "c"}},
		&Service{ID: "node-d", Name: "service", Address: "localhost", Port: 5004, Attributes: map[string]string{"foo": "d"}},
	})))

	watcher.Stop()
	Eventually(ch).Should(BeClosed())
	Expect(calledPath).To(Equal("/prefix/service"))
}

func (s *ZkSuite) TestWatcherGetRace(t sweet.T) {
	var (
		conn          = NewMockZkConn()
		client        = newZkClient(conn, WithZkPrefix("prefix"))
		watcher       = client.NewWatcher("service")
		update        = make(chan struct{})
		childrenChan  = make(chan []string, 3)
		childrenCalls = 0
		getCalls      = 0
		data          = map[string][]byte{
			"/prefix/service/_c_345e7574c5464a76bf5f0c5b77ed8de7-node-a-0000000001": []byte(`{"address": "localhost", "port": 5001, "attributes": {"foo": "a"}}`),
			"/prefix/service/_c_ebd335e2080f406f8967818107ec71bb-node-b-0000000002": []byte(`{"address": "localhost", "port": 5002, "attributes": {"foo": "b"}}`),
		}

		children = []string{
			"_c_345e7574c5464a76bf5f0c5b77ed8de7-node-a-0000000001",
			"_c_ebd335e2080f406f8967818107ec71bb-node-b-0000000002",
			"_c_97383ee5c96b43f89d71d5f80a0b1927-node-d-9000000000",
		}
	)

	for _, i := range []int{3, 2} {
		childrenChan <- children[:i]
	}

	conn.childrenW = func(path string) ([]string, <-chan struct{}, error) {
		childrenCalls++
		return <-childrenChan, update, nil
	}

	conn.get = func(path string) ([]byte, error) {
		getCalls++
		if _, ok := data[path]; !ok {
			return nil, zk.ErrNoNode
		}

		return data[path], nil
	}

	ch, err := watcher.Start()
	Expect(err).To(BeNil())

	Eventually(ch).Should(Receive(Equal([]*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
		&Service{ID: "node-b", Name: "service", Address: "localhost", Port: 5002, Attributes: map[string]string{"foo": "b"}},
	})))

	Consistently(ch).ShouldNot(Receive())
	watcher.Stop()
	Eventually(ch).Should(BeClosed())

	Expect(childrenCalls).To(Equal(2))
	Expect(getCalls).To(Equal(5))
}

func (s *ZkSuite) TestReadZkServices(t sweet.T) {
	var (
		conn = NewMockZkConn()
		data = map[string][]byte{
			"/prefix/service/_c_345e7574c5464a76bf5f0c5b77ed8de7-node-a-0000000001": []byte(`{"address": "localhost", "port": 5001, "attributes": {"foo": "a"}}`),
			"/prefix/service/_c_ebd335e2080f406f8967818107ec71bb-node-b-0000000002": []byte(`{"address": "localhost", "port": 5002, "attributes": {"foo": "b"}}`),
			"/prefix/service/_c_64a32eb0990843dabc9332856a2aec7e-node-c-0000000123": []byte(`{"address": "localhost", "port": 5003, "attributes": {"foo": "c"}}`),
			"/prefix/service/_c_97383ee5c96b43f89d71d5f80a0b1927-node-d-9000000000": []byte(`{"address": "localhost", "port": 5004, "attributes": {"foo": "d"}}`),
		}
	)

	conn.get = func(path string) ([]byte, error) {
		return data[path], nil
	}

	services, err := readZkServices(conn, "prefix", "service", []string{
		"_c_ebd335e2080f406f8967818107ec71bb-node-b-0000000002",
		"_c_64a32eb0990843dabc9332856a2aec7e-node-c-0000000123",
		"_c_97383ee5c96b43f89d71d5f80a0b1927-node-d-9000000000",
		"_c_345e7574c5464a76bf5f0c5b77ed8de7-node-a-0000000001",
	})

	Expect(err).To(BeNil())
	Expect(services).To(Equal([]*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
		&Service{ID: "node-b", Name: "service", Address: "localhost", Port: 5002, Attributes: map[string]string{"foo": "b"}},
		&Service{ID: "node-c", Name: "service", Address: "localhost", Port: 5003, Attributes: map[string]string{"foo": "c"}},
		&Service{ID: "node-d", Name: "service", Address: "localhost", Port: 5004, Attributes: map[string]string{"foo": "d"}},
	}))
}

func (s *ZkSuite) TestReadZkServicesNonconformingNodePath(t sweet.T) {
	var (
		conn = NewMockZkConn()
		data = map[string][]byte{
			"/prefix/service/_c_345e7574c5464a76bf5f0c5b77ed8de7-node-a-0000000001": []byte(`{"address": "localhost", "port": 5001, "attributes": {"foo": "a"}}`),
			"/prefix/service/_c_ebd335e2080f406f8967818107ec71bb-node-b-0000000002": []byte(`{"address": "localhost", "port": 5002, "attributes": {"foo": "b"}}`),
			"/prefix/service/_c_64a32eb0990843dabc9332856a2aec7g-node-c-0000000123": []byte(`{"address": "localhost", "port": 5003, "attributes": {"foo": "c"}}`),
			"/prefix/service/_c_97383ee5c96b43f89d71d5f80a0b1927-node-d-a000000000": []byte(`{"address": "localhost", "port": 5004, "attributes": {"foo": "d"}}`),
		}
	)

	conn.get = func(path string) ([]byte, error) {
		return data[path], nil
	}

	services, err := readZkServices(conn, "prefix", "service", []string{
		"_c_ebd335e2080f406f8967818107ec71bb-node-b-0000000002",
		"_c_64a32eb0990843dabc9332856a2aec7e-node-c-0000000123",
		"_c_97383ee5c96b43f89d71d5f80a0b1927-node-d-9000000000",
		"_c_345e7574c5464a76bf5f0c5b77ed8de7-node-a-0000000001",
	})

	Expect(err).To(BeNil())
	Expect(services).To(Equal([]*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
		&Service{ID: "node-b", Name: "service", Address: "localhost", Port: 5002, Attributes: map[string]string{"foo": "b"}},
	}))
}

func (s *ZkSuite) TestReadZkServicesNonconformingNodeJSON(t sweet.T) {
	var (
		conn = NewMockZkConn()
		data = map[string][]byte{
			"/prefix/service/_c_345e7574c5464a76bf5f0c5b77ed8de7-node-a-0000000001": []byte(`{"address": "localhost", "port": 5001, "attributes": {"foo": "a"}}`),
			"/prefix/service/_c_ebd335e2080f406f8967818107ec71bb-node-b-0000000002": []byte(`{"address": "localhost", "port": 5002, "attributes": {"foo": 123}}`),
			"/prefix/service/_c_64a32eb0990843dabc9332856a2aec7e-node-c-0000000123": []byte(`not json`),
			"/prefix/service/_c_97383ee5c96b43f89d71d5f80a0b1927-node-d-9000000000": []byte(`{"address": "localhost", "port": 5004, "attributes": {"foo": "d"}}`),
		}
	)

	conn.get = func(path string) ([]byte, error) {
		return data[path], nil
	}

	services, err := readZkServices(conn, "prefix", "service", []string{
		"_c_ebd335e2080f406f8967818107ec71bb-node-b-0000000002",
		"_c_64a32eb0990843dabc9332856a2aec7e-node-c-0000000123",
		"_c_97383ee5c96b43f89d71d5f80a0b1927-node-d-9000000000",
		"_c_345e7574c5464a76bf5f0c5b77ed8de7-node-a-0000000001",
	})

	Expect(err).To(BeNil())
	Expect(services).To(Equal([]*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
		&Service{ID: "node-d", Name: "service", Address: "localhost", Port: 5004, Attributes: map[string]string{"foo": "d"}},
	}))
}

func (s *ZkSuite) TestReadZkServicesError(t sweet.T) {
	conn := NewMockZkConn()
	conn.get = func(path string) ([]byte, error) {
		return nil, zk.ErrUnknown
	}

	_, err := readZkServices(conn, "prefix", "service", []string{
		"_c_ebd335e2080f406f8967818107ec71bb-node-b-0000000002",
		"_c_64a32eb0990843dabc9332856a2aec7e-node-c-0000000123",
		"_c_97383ee5c96b43f89d71d5f80a0b1927-node-d-9000000000",
		"_c_345e7574c5464a76bf5f0c5b77ed8de7-node-a-0000000001",
	})

	Expect(err).To(Equal(zk.ErrUnknown))
}

func (s *ZkSuite) TestExtractZkMeta(t sweet.T) {
	id, sequenceNumber, ok := extractZkMeta("_c_ebd335e2080f406f8967818107ec71bb-node-a-0000000002")
	Expect(ok).To(BeTrue())
	Expect(id).To(Equal("node-a"))
	Expect(sequenceNumber).To(Equal(02))
}

func (s *ZkSuite) TestExtractZkMetaHighSequenceNumber(t sweet.T) {
	id, sequenceNumber, ok := extractZkMeta("_c_ebd335e2080f406f8967818107ec71bb-node-a-9000000000")
	Expect(ok).To(BeTrue())
	Expect(id).To(Equal("node-a"))
	Expect(sequenceNumber).To(Equal(9000000000))
}

func (s *ZkSuite) TestExtractZkMetaNonMatch(t sweet.T) {
	_, _, ok := extractZkMeta("_c_ebd335e2080f406f8967818107ec71bb-node-a-a000000002")
	Expect(ok).To(BeFalse())
}

func (s *ZkSuite) TestCreateZkPathSimple(t sweet.T) {
	var (
		conn  = NewMockZkConn()
		paths = []string{}
	)

	conn.create = func(path string, data []byte) error {
		paths = append(paths, path)
		return nil
	}

	Expect(createZkPath(conn, "/single")).To(BeNil())
	Expect(paths).To(Equal([]string{
		"/single",
	}))
}

func (s *ZkSuite) TestCreateZkPathMultiple(t sweet.T) {
	var (
		conn  = NewMockZkConn()
		paths = []string{}
	)

	conn.create = func(path string, data []byte) error {
		paths = append(paths, path)
		return nil
	}

	Expect(createZkPath(conn, "/root/middle/leaf")).To(BeNil())
	Expect(paths).To(Equal([]string{
		"/root",
		"/root/middle",
		"/root/middle/leaf",
	}))
}

func (s *ZkSuite) TestCreateZkPathError(t sweet.T) {
	var (
		conn  = NewMockZkConn()
		paths = []string{}
	)

	conn.create = func(path string, data []byte) error {
		paths = append(paths, path)
		if len(paths) == 2 {
			return zk.ErrUnknown
		}

		return nil
	}

	Expect(createZkPath(conn, "/root/middle/leaf")).To(Equal(zk.ErrUnknown))
	Expect(paths).To(Equal([]string{
		"/root",
		"/root/middle",
	}))
}

func (s *ZkSuite) TestCreateZkPathPartiallyExists(t sweet.T) {
	var (
		conn  = NewMockZkConn()
		paths = []string{}
	)

	conn.create = func(path string, data []byte) error {
		paths = append(paths, path)
		if len(paths) < 3 {
			return zk.ErrNodeExists
		}

		return nil
	}

	Expect(createZkPath(conn, "/root/middle/leaf")).To(BeNil())
	Expect(paths).To(Equal([]string{
		"/root",
		"/root/middle",
		"/root/middle/leaf",
	}))
}

//
// Mocks

type mockZkConn struct {
	create          func(path string, data []byte) error
	createEphemeral func(path string, data []byte) error
	get             func(path string) ([]byte, error)
	children        func(path string) ([]string, error)
	childrenW       func(path string) ([]string, <-chan struct{}, error)
}

func NewMockZkConn() *mockZkConn {
	return &mockZkConn{
		create:          func(path string, data []byte) error { return nil },
		createEphemeral: func(path string, data []byte) error { return nil },
		get:             func(path string) ([]byte, error) { return nil, nil },
		children:        func(path string) ([]string, error) { return nil, nil },
		childrenW:       func(path string) ([]string, <-chan struct{}, error) { return nil, nil, nil },
	}
}

func (m *mockZkConn) Create(path string, data []byte) error {
	return m.create(path, data)
}

func (m *mockZkConn) CreateEphemeral(path string, data []byte) error {
	return m.createEphemeral(path, data)
}

func (m *mockZkConn) Get(path string) ([]byte, error) {
	return m.get(path)
}

func (m *mockZkConn) Children(path string) ([]string, error) {
	return m.children(path)
}

func (m *mockZkConn) ChildrenW(path string) ([]string, <-chan struct{}, error) {
	return m.childrenW(path)
}
