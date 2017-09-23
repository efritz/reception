package reception

import (
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/aphistic/sweet"
	"github.com/efritz/glock"
	. "github.com/onsi/gomega"
)

type CheckServerSuite struct{}

func (s *CheckServerSuite) TestDynamicPort(t sweet.T) {
	addrs := []string{}
	for i := 0; i < 10; i++ {
		server := newCheckServer(
			"localhost",
			0,
			NewNilLogger(),
			glock.NewRealClock(),
		)

		server.start()
		addrs = append(addrs, server.addr)
	}

	for i := 0; i < 10; i++ {
		for j := i + 1; j < 10; j++ {
			Expect(addrs[i]).NotTo(Equal(addrs[j]))
		}
	}
}

func (s *CheckServerSuite) TestMultipleListeners(t sweet.T) {
	server := newCheckServer("localhost", 0, NewNilLogger(), glock.NewRealClock())
	server.start()

	chans := []<-chan error{}
	for i := 0; i < 5; i++ {
		chans = append(chans, server.register())
	}

	for i := 0; i < 5; i++ {
		Consistently(chans[i]).ShouldNot(Receive())
	}

	checkConsulHealthEndpoint(server.addr)

	for i := 0; i < 5; i++ {
		Eventually(chans[i]).Should(Receive(BeNil()))
		Consistently(chans[i]).ShouldNot(Receive())
	}
}

func (s *CheckServerSuite) TestNoHost(t sweet.T) {
	server := newCheckServer("", 0, NewNilLogger(), glock.NewRealClock())
	server.start()
	ch := server.register()
	Eventually(ch).Should(Receive(Equal(ErrIllegalHost)))
	Eventually(ch).Should(BeClosed())
}

func (s *CheckServerSuite) TestPortBound(t sweet.T) {
	server1 := newCheckServer(
		"localhost",
		0,
		NewNilLogger(),
		glock.NewRealClock(),
	)

	server1.start()
	Consistently(server1.register()).ShouldNot(Receive(Equal(ErrIllegalHost)))

	addr, err := url.Parse(server1.addr)
	Expect(err).To(BeNil())

	port, err := strconv.Atoi(strings.Split(addr.Host, ":")[1])
	Expect(err).To(BeNil())

	server2 := newCheckServer(
		"localhost",
		port,
		NewNilLogger(),
		glock.NewRealClock(),
	)

	server2.start()
	ch := server2.register()
	Eventually(ch).Should(Receive(&err))
	Expect(err).NotTo(BeNil())
	Expect(err.Error()).To(ContainSubstring("address already in use"))
	Eventually(ch).Should(BeClosed())
}

//
// Helpers

func checkConsulHealthEndpoint(addr string) {
	req, err := http.NewRequest("GET", addr, nil)
	Expect(err).To(BeNil())

	resp, err := http.DefaultClient.Do(req)
	Expect(err).To(BeNil())
	defer resp.Body.Close()

	data, _ := ioutil.ReadAll(resp.Body)
	Expect(resp.StatusCode).To(Equal(http.StatusOK))
	Expect(data).To(MatchJSON(`{"alive": true}`))
}
