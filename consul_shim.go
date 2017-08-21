package reception

import (
	consul "github.com/hashicorp/consul/api"
	"golang.org/x/net/context"
)

type (
	consulAPI interface {
		Register(registration *consul.AgentServiceRegistration) error
		List(name string) ([]*consul.CatalogService, error)
		Watch(name string, index uint64, ctx context.Context) ([]*consul.CatalogService, uint64, error)
	}

	consulShim struct {
		agent   *consul.Agent
		catalog *consul.Catalog
	}
)

func (c *consulShim) Register(registration *consul.AgentServiceRegistration) error {
	return c.agent.ServiceRegister(registration)
}

func (c *consulShim) List(name string) ([]*consul.CatalogService, error) {
	services, _, err := c.catalog.Service(name, "", nil)
	return services, err
}

func (c *consulShim) Watch(name string, index uint64, ctx context.Context) ([]*consul.CatalogService, uint64, error) {
	options := &consul.QueryOptions{
		WaitIndex: index,
	}

	services, meta, err := c.catalog.Service(name, "", options.WithContext(ctx))
	if err != nil {
		return nil, 0, err
	}

	return services, meta.LastIndex, nil
}
