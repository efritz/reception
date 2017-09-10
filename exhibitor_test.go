package reception

import (
	"net/http"
	"net/http/httptest"

	"github.com/aphistic/sweet"
	. "github.com/onsi/gomega"
)

type ExhibitorSuite struct{}

func (s *ExhibitorSuite) TestChoosesRandomServer(t sweet.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"port": 3000, "servers": ["foo", "bar", "baz"]}`))
	}))

	defer server.Close()

	frequency := map[string]int{}

	for i := 0; i < 1500; i++ {
		addr, err := chooseRandomServer(server.URL)
		Expect(err).To(BeNil())
		frequency[addr] = frequency[addr] + 1
	}

	Expect(frequency["foo:3000"]).To(BeNumerically("~", 500, 50))
	Expect(frequency["bar:3000"]).To(BeNumerically("~", 500, 50))
	Expect(frequency["baz:3000"]).To(BeNumerically("~", 500, 50))
	Expect(frequency["foo:3000"] + frequency["bar:3000"] + frequency["baz:3000"]).To(Equal(1500))
}

func (s *ExhibitorSuite) TestChoosesRandomServerEmptyList(t sweet.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"port": 3000, "servers": []}`))
	}))

	defer server.Close()

	_, err := chooseRandomServer(server.URL)
	Expect(err).To(Equal(ErrEmptyServerList))
}

func (s *ExhibitorSuite) TestGetServerAddrs(t sweet.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"port": 3000, "servers": ["foo", "bar", "baz"]}`))
	}))

	defer server.Close()

	addrs, err := getServerAddrs(server.URL)
	Expect(err).To(BeNil())
	Expect(addrs).To(Equal([]string{
		"foo:3000",
		"bar:3000",
		"baz:3000",
	}))
}

func (s *ExhibitorSuite) TestGetServerAddrsEmpty(t sweet.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"port": 3000, "servers": []}`))
	}))

	defer server.Close()

	addrs, err := getServerAddrs(server.URL)
	Expect(err).To(BeNil())
	Expect(addrs).To(BeEmpty())
}

func (s *ExhibitorSuite) TestGetServerBadResponse(t sweet.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	defer server.Close()

	_, err := getServerAddrs(server.URL)
	Expect(err).To(Equal(ErrBadResponse))
}
