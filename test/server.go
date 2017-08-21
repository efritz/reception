package main

import (
	"fmt"

	"github.com/efritz/reception"
)

func main() {
	client, err := reception.DialConsul("http://localhost:2379")
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return
	}

	services, err := client.ListServices("test-service")
	if err != nil {
		fmt.Printf("Error: %#v\n", err.Error())
		return
	}

	for _, service := range services {
		fmt.Printf("%#v\n", service)
	}
}
