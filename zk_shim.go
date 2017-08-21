package reception

import "github.com/samuel/go-zookeeper/zk"

type (
	zkConn interface {
		Create(path string, data []byte) error
		CreateEphemeral(path string, data []byte) error
		Get(path string) ([]byte, error)
		Children(path string) ([]string, error)
		ChildrenW(path string) ([]string, <-chan struct{}, error)
	}

	zkShim struct {
		conn *zk.Conn
	}
)

func (s *zkShim) Create(path string, data []byte) error {
	_, err := s.conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	return err
}

func (s *zkShim) CreateEphemeral(path string, data []byte) error {
	_, err := s.conn.CreateProtectedEphemeralSequential(path, data, zk.WorldACL(zk.PermAll))
	return err
}

func (s *zkShim) Get(path string) ([]byte, error) {
	data, _, err := s.conn.Get(path)
	return data, err
}

func (s *zkShim) Children(path string) ([]string, error) {
	data, _, err := s.conn.Children(path)
	return data, err
}

func (s *zkShim) ChildrenW(path string) ([]string, <-chan struct{}, error) {
	data, _, events, err := s.conn.ChildrenW(path)
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan struct{})

	go func() {
		<-events
		close(ch)
	}()

	return data, ch, nil
}
