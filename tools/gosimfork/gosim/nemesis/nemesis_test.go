//go:build sim

package nemesis_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/netip"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/jellevandenhooff/gosim"
	"github.com/jellevandenhooff/gosim/nemesis"
)

func makeAddresses() []string {
	var addresses []string
	for i := 0; i < 3; i++ {
		addresses = append(addresses, fmt.Sprintf("10.0.0.%d", i+1))
	}
	return addresses
}

// request makes a HTTP GET request with a 1 second timeout to
// http://<addr>/ping and prints the result
func request(addr string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://%s/ping", addr), nil)
	if err != nil {
		log.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("pinging %s: got error: %v", addr, err)
		return
	}
	defer resp.Body.Close()
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("pinging %s: got error: %v", addr, err)
		return
	}
	log.Printf("pinging %s: got response %q", addr, string(bytes))
}

func pingerMain() {
	log.Printf("starting pinger")

	// figure out our own address
	// TODO: make another API work
	addr := gosim.CurrentMachine().Label()

	// run http server
	// TODO: make ":80" without addr work
	go http.ListenAndServe(addr+":80", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "hello there")
	}))

	addrs := makeAddresses()

	// ping others every 5 seconds
	t := time.NewTicker(5 * time.Second)
	for range t.C {
		// ping in parallel
		var g errgroup.Group
		for _, other := range addrs {
			if addr != other {
				g.Go(func() error {
					request(other)
					return nil
				})
			}
		}
		g.Wait()
	}
}

func TestPartition(t *testing.T) {
	addrs := makeAddresses()

	// run ping on several machines
	var machines []gosim.Machine
	for _, addr := range addrs {
		m := gosim.NewMachine(gosim.MachineConfig{
			Label:    addr, // fmt.Sprintf("server-%d", i),
			Addr:     netip.MustParseAddr(addr),
			MainFunc: pingerMain,
		})
		machines = append(machines, m)
	}

	// let machines communicate for a while, then randomly partition, then
	// repair, then crash one machine, and repair again
	scenario := nemesis.Sequence(
		nemesis.Sleep{
			Duration: 10 * time.Second,
		},
		nemesis.PartitionMachines{
			// TODO: make this API based of machines?
			Addresses: addrs,
			Duration:  10 * time.Second,
		},
		nemesis.Sleep{
			Duration: 10 * time.Second,
		},
		nemesis.RestartRandomly{
			Machines: machines,
			Downtime: 10 * time.Second,
		},
		nemesis.Sleep{
			Duration: 10 * time.Second,
		},
	)

	scenario.Run()
}
