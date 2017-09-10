package reception

import (
	"context"
	"errors"
	"fmt"

	"github.com/aphistic/sweet"
	. "github.com/onsi/gomega"
)

type UtilSuite struct{}

func (s *UtilSuite) TestWithContextSuccess(t sweet.T) {
	again, err := withContext(make(chan struct{}), func(ctx context.Context) error {
		return nil
	})

	Expect(again).To(BeTrue())
	Expect(err).To(BeNil())
}

func (s *UtilSuite) TestWithContextError(t sweet.T) {
	again, err := withContext(make(chan struct{}), func(ctx context.Context) error {
		return errors.New("foobar")
	})

	Expect(again).To(BeTrue())
	Expect(err).To(MatchError("foobar"))
}

func (s *UtilSuite) TestWithContextCanceled(t sweet.T) {
	var (
		stop   = make(chan struct{})
		result = make(chan bool)
	)

	go func() {
		defer close(result)

		again, _ := withContext(stop, func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		})

		result <- again
	}()

	Consistently(result).ShouldNot(Receive())
	close(stop)
	Eventually(result).Should(Receive(BeTrue()))
}

func (s *UtilSuite) TestCancelOnClose(t sweet.T) {
	var (
		stop        = make(chan struct{})
		ctx, cancel = context.WithCancel(context.Background())
		sync        = make(chan struct{})
	)

	defer cancel()

	go func() {
		cancelOnClose(ctx, func() { close(sync) }, stop)
	}()

	Consistently(sync).ShouldNot(BeClosed())
	close(stop)
	Eventually(sync).Should(BeClosed())
}

func (s *UtilSuite) TestCancelOnCloseNoClose(t sweet.T) {
	var (
		stop        = make(chan struct{})
		ctx, cancel = context.WithCancel(context.Background())
		called      = false
	)

	defer close(stop)

	cancel()
	cancelOnClose(ctx, func() { called = true }, stop)
	Expect(called).To(BeFalse())
}

func (s *UtilSuite) TestIsCanceled(t sweet.T) {
	ctx, cancel := context.WithCancel(context.Background())

	Consistently(isCanceled(ctx)).Should(BeFalse())
	cancel()
	Consistently(isCanceled(ctx)).Should(BeTrue())
}

func (s *UtilSuite) TestSendOrStop(t sweet.T) {
	var (
		ch    = make(chan *ServiceState, 1)
		stop  = make(chan struct{})
		err   = errors.New("test")
		state *ServiceState
	)

	defer close(ch)

	Expect(sendOrStop(ch, stop, &ServiceState{Err: err})).To(BeTrue())
	Expect(ch).To(Receive(&state))
	Expect(state.Err).To(MatchError("test"))

	close(stop)
	Expect(sendOrStop(ch, stop, &ServiceState{Err: err})).To(BeFalse())
	Expect(ch).NotTo(Receive())
}

func (s *UtilSuite) TestMakePath(t sweet.T) {
	Expect(makePath()).To(Equal("/"))
	Expect(makePath("foo")).To(Equal("/foo"))
	Expect(makePath("foo", "bar", "baz")).To(Equal("/foo/bar/baz"))
	Expect(makePath("/foo/", "/bar/", "/baz/")).To(Equal("/foo/bar/baz"))
	Expect(makePath("", "foo", "", "bar", "")).To(Equal("/foo/bar"))
}

func (s *UtilSuite) TestSortServiceMap(t sweet.T) {
	serviceMap := map[int]*Service{}
	for i := 0; i < 1000; i++ {
		serviceMap[i] = &Service{
			Name:       fmt.Sprintf("Service-%d", i+1),
			ID:         fmt.Sprintf("ID-%d", i+1),
			Attributes: nil,
		}
	}

	services := sortServiceMap(serviceMap)

	for i := 0; i < 1000; i++ {
		Expect(services[i].ID).To(Equal(fmt.Sprintf("ID-%d", i+1)))
	}
}
