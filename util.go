package reception

import (
	"sort"
	"strings"
)

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
