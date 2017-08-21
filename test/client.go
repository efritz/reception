package main

import (
	"fmt"
	"os"

	"github.com/efritz/reception"
	uuid "github.com/satori/go.uuid"
)

func main() {
	client, err := reception.DialConsul("http://localhost:2379")
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return
	}

	fmt.Printf("Connected\n")

	service := &reception.Service{
		ID:   uuid.NewV4().String(),
		Name: "test-service",
		Metadata: map[string]string{
			"pid": fmt.Sprintf("%d", os.Getpid()),
		},
	}

	if err := client.Register(service); err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return
	}

	w := client.NewWatcher("test-service")
	ch, err := w.Start()
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return
	}

	for update := range ch {
		fmt.Printf("Live nodes:\n")

		for _, service := range update {
			fmt.Printf("  - %s: %#v\n", service.ID, service.Metadata)
		}

		fmt.Printf("\n")
	}
}
