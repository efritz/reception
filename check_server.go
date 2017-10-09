package reception

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/efritz/glock"
)

type checkServer struct {
	host      string
	port      int
	addr      string
	listeners []chan error
	err       error
	mutex     sync.Mutex
	logger    Logger
	clock     glock.Clock
}

// ErrZkDisconnect occurs when a health check server is created with an illegal host value.
var ErrIllegalHost = errors.New("illegal host")

func newCheckServer(host string, port int, logger Logger, clock glock.Clock) *checkServer {
	return &checkServer{
		host:      host,
		port:      port,
		listeners: []chan error{},
		logger:    logger,
		clock:     clock,
	}
}

func (s *checkServer) start() {
	if s.host == "" {
		s.signal(ErrIllegalHost)
		return
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", s.port))
	if err != nil {
		s.signal(err)
		return
	}

	s.addr = fmt.Sprintf(
		"http://%s:%d/health",
		s.host,
		listener.Addr().(*net.TCPAddr).Port,
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handler)

	s.logger.Printf("Running health check at %s\n", s.addr)

	go func() {
		s.signal(http.Serve(listener, mux))
	}()
}

func (s *checkServer) handler(w http.ResponseWriter, r *http.Request) {
	s.logger.Printf("Consul performing health check\n")

	s.signal(nil)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"alive": true}`))
}

func (s *checkServer) register() <-chan error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	ch := make(chan error, 1)

	if s.err != nil {
		ch <- s.err
		close(ch)
		return ch
	}

	s.listeners = append(s.listeners, ch)
	return ch
}

func (s *checkServer) signal(err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, ch := range s.listeners {
		ch <- err
	}

	if err == nil {
		return
	}

	for _, ch := range s.listeners {
		close(ch)
	}

	s.err = err
	s.listeners = nil
}
