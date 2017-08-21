package reception

import (
	etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

type (
	keysAPI interface {
		Get(path string) (*etcd.Response, error)
		Set(path, value string, opts *etcd.SetOptions) error
		Watcher(path string) etcd.Watcher
	}

	etcdShim struct {
		api etcd.KeysAPI
	}
)

func (s *etcdShim) Get(path string) (*etcd.Response, error) {
	return s.api.Get(context.Background(), path, &etcd.GetOptions{
		Recursive: true,
	})
}

func (s *etcdShim) Set(path, value string, opts *etcd.SetOptions) error {
	_, err := s.api.Set(context.Background(), path, value, opts)
	return err
}

func (s *etcdShim) Watcher(path string) etcd.Watcher {
	return s.api.Watcher(path, &etcd.WatcherOptions{Recursive: true})
}
