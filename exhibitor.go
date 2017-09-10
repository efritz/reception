package reception

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
)

var (
	ErrBadResponse     = errors.New("could not decode exhibitor response")
	ErrEmptyServerList = errors.New("exhibitor is alive but returned no servers")
)

type (
	serverList struct {
		Port    int      `json:"port" required:"true"`
		Servers []string `json:"servers" required:"true"`
	}
)

func chooseRandomServer(url string) (string, error) {
	servers, err := getServerAddrs(url)
	if err != nil {
		return "", err
	}

	if len(servers) == 0 {
		return "", ErrEmptyServerList
	}

	return servers[rand.Intn(len(servers))], nil
}

func getServerAddrs(url string) ([]string, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		return nil, ErrBadResponse
	}

	servers := serverList{}
	if err := json.NewDecoder(r.Body).Decode(&servers); err != nil {
		return nil, ErrBadResponse
	}

	addrs := []string{}
	for _, server := range servers.Servers {
		addrs = append(addrs, fmt.Sprintf("%s:%d", server, servers.Port))
	}

	return addrs, nil
}
