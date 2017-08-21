package reception

import (
	"sort"
	"strings"

	"golang.org/x/net/context"
)

func withContext(stop <-chan struct{}, f func(ctx context.Context) error) (bool, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go cancelOnClose(ctx, cancel, stop)

	if err := f(ctx); err != nil {
		if isCanceled(ctx) {
			return false, nil
		}

		return true, err
	}

	return true, nil
}

func cancelOnClose(ctx context.Context, cancel func(), stop <-chan struct{}) {
	select {
	case <-stop:
		cancel()

	case <-ctx.Done():
	}
}

func isCanceled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}

	return false
}

func makePath(parts ...string) string {
	for i := 0; i < len(parts); i++ {
		parts[i] = strings.Trim(parts[i], "/")
	}

	return "/" + strings.Join(parts, "/")
}

func sortServiceMap(serviceMap map[int]*Service) []*Service {
	sequences := []int{}
	for k := range serviceMap {
		sequences = append(sequences, k)
	}

	sort.Ints(sequences)

	services := []*Service{}
	for _, sequenceNumber := range sequences {
		services = append(services, serviceMap[sequenceNumber])
	}

	return services
}
