package reception

import (
	"errors"
	"time"

	"github.com/aphistic/sweet"
	etcd "github.com/coreos/etcd/client"
	"github.com/efritz/glock"
	. "github.com/onsi/gomega"
	"golang.org/x/net/context"
)

type EtcdSuite struct{}

type setCall struct {
	path  string
	value string
	opts  *etcd.SetOptions
}

func (s *EtcdSuite) TestRegister(t sweet.T) {
	var (
		api    = newMockKeysAPI()
		clock  = glock.NewMockClock()
		client = newEtcdClient(
			api,
			WithEtcdPrefix("prefix"),
			WithClock(clock),
			WithTTL(time.Minute*5),
			WithRefreshInterval(time.Minute),
		)

		setCalls = make(chan *setCall)
		result   = make(chan error)
		rootCall *setCall
		leafCall *setCall
	)

	api.set = func(path, value string, opts *etcd.SetOptions) error {
		setCalls <- &setCall{
			path:  path,
			value: value,
			opts:  opts,
		}

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

	Eventually(setCalls).Should(Receive(&rootCall))
	Eventually(setCalls).Should(Receive(&leafCall))
	Eventually(result).Should(Receive(BeNil()))

	Expect(rootCall.path).To(Equal("/prefix/service"))
	Expect(rootCall.opts.Dir).To(BeTrue())

	Expect(leafCall.path).To(Equal("/prefix/service/node-a"))
	Expect(leafCall.opts.TTL).To(Equal(time.Minute * 5))
	Expect(leafCall.value).To(MatchJSON(`{
		"address": "localhost",
		"port": 1234,
		"attributes": {
			"foo": "bar",
			"baz": "bonk"
		}
	}`))

	for i := 0; i < 5; i++ {
		Consistently(setCalls).ShouldNot(Receive())
		clock.Advance(time.Minute)
		Eventually(setCalls).Should(Receive(&leafCall))

		Expect(leafCall.path).To(Equal("/prefix/service/node-a"))
		Expect(leafCall.opts.Refresh).To(BeTrue())
		Expect(leafCall.opts.TTL).To(Equal(time.Minute * 5))
	}
}

func (s *EtcdSuite) TestRegisterError(t sweet.T) {
	var (
		api    = newMockKeysAPI()
		client = newEtcdClient(api, WithEtcdPrefix("prefix"))
	)

	api.set = func(path, value string, opts *etcd.SetOptions) error {
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

func (s *EtcdSuite) TestListServices(t sweet.T) {
	var (
		api        = newMockKeysAPI()
		client     = newEtcdClient(api, WithEtcdPrefix("prefix"))
		calledPath string
	)

	api.get = func(path string) (*etcd.Response, error) {
		calledPath = path

		return makeResponse([]*etcd.Node{
			&etcd.Node{Key: "node-d", Value: `{"address": "localhost", "port": 5004, "attributes": {"foo": "d"}}`, CreatedIndex: 4},
			&etcd.Node{Key: "node-b", Value: `{"address": "localhost", "port": 5002, "attributes": {"foo": "b"}}`, CreatedIndex: 2},
			&etcd.Node{Key: "node-a", Value: `{"address": "localhost", "port": 5001, "attributes": {"foo": "a"}}`, CreatedIndex: 1},
			&etcd.Node{Key: "node-c", Value: `{"address": "localhost", "port": 5003, "attributes": {"foo": "c"}}`, CreatedIndex: 3},
		}), nil
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

func (s *EtcdSuite) TestListServicesError(t sweet.T) {
	var (
		api    = newMockKeysAPI()
		client = newEtcdClient(api, WithEtcdPrefix("prefix"))
	)

	api.get = func(path string) (*etcd.Response, error) {
		return nil, errors.New("utoh")
	}

	_, err := client.ListServices("service")
	Expect(err).To(MatchError("utoh"))
}

func (s *EtcdSuite) TestWatcher(t sweet.T) {
	var (
		api          = newMockKeysAPI()
		client       = newEtcdClient(api, WithEtcdPrefix("prefix"))
		watcher      = client.NewWatcher("service")
		etcdWatcher  = newMockEtcdWatcher()
		update       = make(chan struct{})
		responseChan = make(chan *etcd.Response, 3)
		children     = []*etcd.Node{
			&etcd.Node{Key: "node-a", Value: `{"address": "localhost", "port": 5001, "attributes": {"foo": "a"}}`, CreatedIndex: 1},
			&etcd.Node{Key: "node-b", Value: `{"address": "localhost", "port": 5002, "attributes": {"foo": "b"}}`, CreatedIndex: 2},
			&etcd.Node{Key: "node-c", Value: `{"address": "localhost", "port": 5003, "attributes": {"foo": "c"}}`, CreatedIndex: 3},
			&etcd.Node{Key: "node-d", Value: `{"address": "localhost", "port": 5004, "attributes": {"foo": "d"}}`, CreatedIndex: 4},
		}
	)

	for _, i := range []int{1, 2, 4} {
		responseChan <- makeResponse(children[:i])
	}

	defer close(responseChan)

	api.watcher = func(path string) etcd.Watcher {
		return etcdWatcher
	}

	etcdWatcher.next = func(ctx context.Context) (*etcd.Response, error) {
		select {
		case <-ctx.Done():
			return nil, errors.New("Canceled")

		case <-update:
			return nil, nil
		}
	}

	api.get = func(path string) (*etcd.Response, error) {
		return <-responseChan, nil
	}

	ch, err := watcher.Start()
	Expect(err).To(BeNil())

	Eventually(ch).Should(Receive(Equal(&ServiceState{Services: []*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
	}})))

	update <- struct{}{}
	Eventually(ch).Should(Receive(Equal(&ServiceState{Services: []*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
		&Service{ID: "node-b", Name: "service", Address: "localhost", Port: 5002, Attributes: map[string]string{"foo": "b"}},
	}})))

	update <- struct{}{}
	Eventually(ch).Should(Receive(Equal(&ServiceState{Services: []*Service{
		&Service{ID: "node-a", Name: "service", Address: "localhost", Port: 5001, Attributes: map[string]string{"foo": "a"}},
		&Service{ID: "node-b", Name: "service", Address: "localhost", Port: 5002, Attributes: map[string]string{"foo": "b"}},
		&Service{ID: "node-c", Name: "service", Address: "localhost", Port: 5003, Attributes: map[string]string{"foo": "c"}},
		&Service{ID: "node-d", Name: "service", Address: "localhost", Port: 5004, Attributes: map[string]string{"foo": "d"}},
	}})))

	watcher.Stop()
	Eventually(ch).Should(BeClosed())
}

func (s *EtcdSuite) TestMapEtcdServices(t sweet.T) {
	resp := makeResponse([]*etcd.Node{
		&etcd.Node{Key: "a", Value: `{"address": "localhost", "port": 5001, "attributes": {"name": "a"}}`, CreatedIndex: 3},
		&etcd.Node{Key: "b", Value: `{"address": "localhost", "port": 5002, "attributes": {"name": "b"}}`, CreatedIndex: 2},
		&etcd.Node{Key: "c", Value: `{"address": "localhost", "port": 5003, "attributes": {"name": "c"}}`, CreatedIndex: 1},
		&etcd.Node{Key: "d", Value: `{"address": "localhost", "port": 5004, "attributes": {"name": "d"}}`, CreatedIndex: 5},
		&etcd.Node{Key: "e", Value: `{"address": "localhost", "port": 5005, "attributes": {"name": 'e'}}`, CreatedIndex: 4},
	})

	services := mapEtcdServices(resp, "test-service")
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

func (s *EtcdSuite) TestEtcdEventChannel(t sweet.T) {
	var (
		stop      = make(chan struct{})
		api       = newMockKeysAPI()
		watcher   = newMockEtcdWatcher()
		responses = make(chan *etcd.Response)
	)

	defer close(responses)

	api.watcher = func(path string) etcd.Watcher {
		return watcher
	}

	watcher.next = func(ctx context.Context) (*etcd.Response, error) {
		select {
		case <-ctx.Done():
			return nil, errors.New("Canceled")

		case r := <-responses:
			return r, nil
		}
	}

	ch := makeEtcdEventChannel(api, "test-path", stop)

	// One event immediately on startup
	Eventually(ch).Should(Receive())
	Consistently(ch).ShouldNot(Receive())

	for i := 0; i < 5; i++ {
		responses <- &etcd.Response{}
		Eventually(ch).Should(Receive())
		Consistently(ch).ShouldNot(Receive())
	}

	close(stop)
	Eventually(ch).Should(BeClosed())
}

//
// Mocks

type mockKeysAPI struct {
	get     func(path string) (*etcd.Response, error)
	set     func(path, value string, opts *etcd.SetOptions) error
	watcher func(path string) etcd.Watcher
}

func newMockKeysAPI() *mockKeysAPI {
	return &mockKeysAPI{
		get:     func(path string) (*etcd.Response, error) { return nil, nil },
		set:     func(path, value string, opts *etcd.SetOptions) error { return nil },
		watcher: func(path string) etcd.Watcher { return newMockEtcdWatcher() },
	}
}

func (a *mockKeysAPI) Get(path string) (*etcd.Response, error) {
	return a.get(path)
}

func (a *mockKeysAPI) Set(path, value string, opts *etcd.SetOptions) error {
	return a.set(path, value, opts)
}

func (a *mockKeysAPI) Watcher(path string) etcd.Watcher {
	return a.watcher(path)
}

//
//

type mockEtcdWatcher struct {
	next func(context.Context) (*etcd.Response, error)
}

func newMockEtcdWatcher() *mockEtcdWatcher {
	return &mockEtcdWatcher{
		next: func(context.Context) (*etcd.Response, error) { return nil, nil },
	}
}

func (w *mockEtcdWatcher) Next(ctx context.Context) (*etcd.Response, error) {
	return w.next(ctx)
}

//
// Helpers

func makeResponse(nodes []*etcd.Node) *etcd.Response {
	return &etcd.Response{Node: &etcd.Node{Nodes: nodes}}
}
