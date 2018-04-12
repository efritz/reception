# Reception

[![GoDoc](https://godoc.org/github.com/efritz/reception?status.svg)](https://godoc.org/github.com/efritz/reception)
[![Build Status](https://secure.travis-ci.org/efritz/reception.png)](http://travis-ci.org/efritz/reception)
[![codecov.io](http://codecov.io/github/efritz/reception/coverage.svg?branch=master)](http://codecov.io/github/efritz/reception?branch=master)

Service discovery and cluster state reporter for Go services.

## Example

A client can be created by dialing the backing service discovery service. A
list of available backing services are shown below.

```go
client, err := DialConsul("localhost:8500")
client, err := DialEtcd("localhost:2379")
client, err := DialExhibitor("localhost:8080/")
client, err := DialZk("localhost:2181")
```

Each dial function takes a set of optional configuration parameters. For example,
a consul client can be configured with specific health check interval values. See
the godoc for a complete list of options for each client.

```go
client, err := DialConsul(
    "localhost:8500",
    WithCheckInterval(time.Second),
    WithCheckTimeout(time.Second * 5),
    WithCheckDeregisterTimeout(time.Second * 30),
)
```

### Registration

Given a client, the current process can register itself as an instance of a service.
In the following, an API instance registers itself with a unique identifier, a means
of accessing it (host and port pair), and a set of attributes which may be useful to
other instances of the service.

```go
service, err := MakeService(
    "api",                      // service name
    "mesos-agent1.example.com", // host
    23423,                      // port
    map[string]string{          // attributes
        "external": "true",
    },
)

if err != nil {
    //
}

if err := client.Register(service, onDisconnect); err != nil {
    //
}
```

The on-disconnect function is called whenever the connection to the backing service
discovery service is in question. This may be nil or a function that simply logs
the state, but in some extreme circumstances the instance may need to stop processing
input immediately (things that require exactly one active process). In this case, the
on-disconnect function may take extreme action:

```go
func onDisconnect(err error) {
    fmt.Printf("Disconnected from cluster, resigning!\n")
    os.Exit(1)
}
```

### Listing

A client may list all available instances of a service by name. This gives the current
state of a service but may include unhealthy instances that have not been yet removed
from the list (if they are still within their deregister timeout).

```go
services, err := client.ListServices("api")
if err != nil {
    // handle error
}

addrs := []string{}
for _, service := range services {
    addrs = append(addrs, fmt.Sprintf("%s:%d", service.Host, service.Port))
}

fmt.Printf("There are %d active services: %s\n", len(addrs), strings.Join(addrs, ", "))
```

### Watching

A client may inform you whenever the instances of a service changes. This is an efficient
way of keeping up-to-date on a service state without repeatedly calling ListServices.

```go
watcher := client.NewWatcher("api")
ch, err := watcher.Start()
if err != nil {
    // handle error
}

for update := range ch {
    if ch.Err != nil {
        // handle error
    } else {
        for _, service := range update.Services {
            fmt.Printf("%s is live\n", service.ID)
        }
    }
}
```

A watcher can be stopped by calling its `Stop` method from another goroutine.

### Election

A client also provides a mechanism for single-leader elections. The `Elect` method
blocks until the current process is elected leader. An error may be returned when
the election is cancelled, or if there is an error connecting to the backing service.

```go
elector := NewElector(
    client,
    "reaper",
    WithDisconnectionCallback(onDisconnect),
)

if err := elector.Elect(); err == nil {
    fmt.Printf("I'm the leader!\n")
}
```

A process can withdraw itself from the election process by calling the elector's
`Cancel` method from another goroutine.

## License

Copyright (c) 2017 Eric Fritz

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
