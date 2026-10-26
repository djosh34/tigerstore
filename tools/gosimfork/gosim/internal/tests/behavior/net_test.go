//go:build sim

package behavior_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"os"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/jellevandenhooff/gosim"
	"github.com/jellevandenhooff/gosim/internal/tests/testpb"
)

// TODO: at the end of each machine, also check that there are no more running goroutines/events/...?
// TODO: what happens if you message to something non-existent?
// TODO: what if the buffer overflows?

var (
	aAddr = netip.AddrFrom4([4]byte{10, 1, 0, 1}).String()
	bAddr = netip.AddrFrom4([4]byte{10, 1, 0, 2}).String()
)

func TestNetTcpDial(t *testing.T) {
	a := gosim.NewMachine(gosim.MachineConfig{
		Label: "a",
		Addr:  netip.MustParseAddr(aAddr),
		MainFunc: func() {
			listener, err := net.Listen("tcp", aAddr+":1234")
			if err != nil {
				t.Fatal(err)
			}

			conn, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}

			log.Println(conn.LocalAddr())
			if conn.LocalAddr().String() != "10.1.0.1:1234" {
				t.Error("bad addr")
			}
			log.Println(conn.RemoteAddr())
			if conn.RemoteAddr().String() != "10.1.0.2:10000" {
				t.Error("bad addr")
			}

			resp1 := make([]byte, 3)
			if _, err := io.ReadFull(conn, resp1); err != nil {
				t.Fatal(err)
			}

			resp2 := make([]byte, 2)
			if _, err := io.ReadFull(conn, resp2); err != nil {
				t.Fatal(err)
			}

			resp := string(resp1) + string(resp2)

			if resp != "hello" {
				t.Fatal(resp)
			}

			if _, err := conn.Write([]byte("world")); err != nil {
				t.Fatal(err)
			}

			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}

			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
		},
	})

	b := gosim.NewMachine(gosim.MachineConfig{
		Label: "b",
		Addr:  netip.MustParseAddr(bAddr),
		MainFunc: func() {
			// give the other a second to start listening
			time.Sleep(time.Second)

			conn, err := net.Dial("tcp", aAddr+":1234")
			if err != nil {
				t.Fatal(err)
			}

			log.Println(conn.LocalAddr())
			if conn.LocalAddr().String() != "10.1.0.2:10000" {
				t.Error("bad addr")
			}
			log.Println(conn.RemoteAddr())
			if conn.RemoteAddr().String() != "10.1.0.1:1234" {
				t.Error("bad addr")
			}
			if _, err := conn.Write([]byte("hello")); err != nil {
				t.Fatal(err)
			}

			resp1 := make([]byte, 3)
			if _, err := io.ReadFull(conn, resp1); err != nil {
				t.Fatal(err)
			}

			resp2 := make([]byte, 2)
			if _, err := io.ReadFull(conn, resp2); err != nil {
				t.Fatal(err)
			}

			resp := string(resp1) + string(resp2)

			if resp != "world" {
				t.Fatal(resp)
			}

			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
		},
	})

	a.Wait()
	b.Wait()
}

func TestNetTcpDisconnectBasic(t *testing.T) {
	a := gosim.NewMachine(gosim.MachineConfig{
		Label: "a",
		Addr:  netip.MustParseAddr(aAddr),
		MainFunc: func() {
			listener, err := net.Listen("tcp", aAddr+":1234")
			if err != nil {
				t.Fatal(err)
			}

			go func() {
				time.Sleep(10 * time.Second)
				listener.Close()
			}()

			for {
				conn, err := listener.Accept()
				if err != nil {
					if errors.Is(err, net.ErrClosed) {
						return
					}
					t.Fatal(err)
				}

				log.Println(conn.LocalAddr())
				log.Println(conn.RemoteAddr())

				go func() {
					buffer := make([]byte, 128)
					for {
						n, err := conn.Read(buffer)
						if err != nil {
							return
						}
						conn.Write(buffer[:n])
					}
				}()
			}
		},
	})
	b := gosim.NewMachine(gosim.MachineConfig{
		Label: "b",
		Addr:  netip.MustParseAddr(bAddr),
		MainFunc: func() {
			// give the other a second to start listening
			time.Sleep(time.Second)

			// connect works
			conn, err := net.Dial("tcp", aAddr+":1234")
			if err != nil {
				t.Fatal(err)
			}

			// echo works
			if _, err := conn.Write([]byte("hello")); err != nil {
				t.Fatal(err)
			}
			resp := make([]byte, 5)
			if _, err := io.ReadFull(conn, resp); err != nil {
				t.Fatal(err)
			}
			respStr := string(resp)
			if respStr != "hello" {
				t.Fatal(respStr)
			}

			// do write, don't read yet
			if _, err := conn.Write([]byte("hello")); err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)

			gosim.SetConnected(aAddr, bAddr, false)

			// reading buffered data works
			resp = make([]byte, 5)
			if _, err := io.ReadFull(conn, resp); err != nil {
				t.Fatal(err)
			}
			respStr = string(resp)
			if respStr != "hello" {
				t.Fatal(respStr)
			}

			// another read times out, because there is no keep alive
			resp = make([]byte, 5)
			conn.SetReadDeadline(time.Now().Add(time.Second))
			if n, err := conn.Read(resp); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatal(n, err)
			}
			conn.SetReadDeadline(time.Time{})

			// write works, because there is no keep alive
			if _, err := conn.Write([]byte("hello")); err != nil {
				t.Fatal(err)
			}
			// read without deadline now fails (because it blocks and the write above will cause an ack time out)
			resp = make([]byte, 5)
			if n, err := conn.Read(resp); n != 0 || !errors.Is(err, syscall.EPIPE) {
				t.Fatal(err)
			}
			// writing now fails because the connection has timed out
			if _, err := conn.Write([]byte("hello")); !errors.Is(err, syscall.EPIPE) {
				t.Fatal(err)
			}

			// a new connect now fails
			if _, err := net.Dial("tcp", aAddr+":1234"); !errors.Is(err, syscall.ETIMEDOUT) {
				t.Fatal(err)
			}

			gosim.SetConnected(aAddr, bAddr, true)

			// new connection works again
			conn, err = net.Dial("tcp", aAddr+":1234")
			if err != nil {
				t.Fatal(err)
			}

			if _, err := conn.Write([]byte("hello")); err != nil {
				t.Fatal(err)
			}
			resp = make([]byte, 5)
			if _, err := io.ReadFull(conn, resp); err != nil {
				t.Fatal(err)
			}
			respStr = string(resp)
			if respStr != "hello" {
				t.Fatal(respStr)
			}
		},
	})

	a.Wait()
	b.Wait()
}

func TestNetTcpHttp(t *testing.T) {
	a := gosim.NewMachine(gosim.MachineConfig{
		Label: "a",
		Addr:  netip.MustParseAddr(aAddr),
		MainFunc: func() {
			listener, err := net.Listen("tcp", aAddr+":1234")
			if err != nil {
				t.Fatal(err)
			}

			http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte("hello world"))
			})

			srv := &http.Server{Handler: nil}

			go func() {
				time.Sleep(10 * time.Second)
				srv.Shutdown(context.Background())
			}()

			if err := srv.Serve(listener); err != nil {
				if err != http.ErrServerClosed {
					t.Fatal(err)
				}
			}
		},
	})

	b := gosim.NewMachine(gosim.MachineConfig{
		Label: "b",
		Addr:  netip.MustParseAddr(bAddr),
		MainFunc: func() {
			// give the other a second to start listening
			time.Sleep(time.Second)

			resp, err := http.Get(fmt.Sprintf("http://%s:1234/", aAddr))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			log.Println(resp.StatusCode)

			bytes, err := io.ReadAll(resp.Body)

			log.Println(string(bytes), err)

			if string(bytes) != "hello world" {
				t.Fatal(string(bytes))
			}
		},
	})

	a.Wait()
	b.Wait()
}

type testServer struct{}

func (s *testServer) Echo(ctx context.Context, req *testpb.EchoRequest) (*testpb.EchoResponse, error) {
	return &testpb.EchoResponse{
		Message: req.Message,
	}, nil
}

func TestNetTcpGrpc(t *testing.T) {
	a := gosim.NewMachine(gosim.MachineConfig{
		Label: "a",
		Addr:  netip.MustParseAddr(aAddr),
		MainFunc: func() {
			listener, err := net.Listen("tcp", aAddr+":1234")
			if err != nil {
				t.Fatal(err)
			}

			server := grpc.NewServer()
			testpb.RegisterEchoServerServer(server, &testServer{})

			go func() {
				time.Sleep(10 * time.Second)
				server.GracefulStop()
			}()

			if err := server.Serve(listener); err != nil {
				t.Fatal(err)
			}
		},
	})

	b := gosim.NewMachine(gosim.MachineConfig{
		Label: "b",
		Addr:  netip.MustParseAddr(bAddr),
		MainFunc: func() {
			// give the other a second to start listening
			time.Sleep(time.Second)

			client, err := grpc.Dial(fmt.Sprintf("%s:1234", aAddr), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}

			testClient := testpb.NewEchoServerClient(client)

			resp, err := testClient.Echo(context.Background(), &testpb.EchoRequest{Message: "hello world"})
			if err != nil {
				t.Fatal(err)
			}
			if resp.Message != "hello world" {
				t.Fatal(resp.Message)
			}

			client.Close()
		},
	})

	a.Wait()
	b.Wait()
}

// TODO: test hosts crash?
// TODO: port tests below to TCP?

/*
func TestNetBasic(t *testing.T) {
	a := gosim.NewMachineWithLabel("a", aAddr, func() {})
	b := gosim.NewMachineWithLabel("b", bAddr, func() {})

	a.Run(func() {
		if err := gosim.Send(bAddr, []byte("hello")); err != nil {
			t.Fatal(err)
		}
	})

	b.Run(func() {
		data, from, err := gosim.Receive()
		if err != nil {
			t.Fatal(err)
		}

		if string(data) != "hello" {
			t.Fatal(string(data))
		}

		if from != aAddr {
			t.Fatal(from)
		}
	})

	a.Wait()
	b.Wait()
}

func TestNetPingPong(t *testing.T) {
	a := gosim.NewMachineWithLabel("a", aAddr, func() {})
	b := gosim.NewMachineWithLabel("b", bAddr, func() {})

	a.Run(func() {
		for i := 0; i < 10; i++ {
			if err := gosim.Send(bAddr, []byte("ping")); err != nil {
				t.Fatal(err)
			}

			data, from, err := gosim.Receive()
			if err != nil {
				t.Fatal(err)
			}

			if string(data) != "pong" {
				t.Fatal(string(data))
			}

			if from != bAddr {
				t.Fatal(from)
			}
		}
	})

	b.Run(func() {
		for i := 0; i < 10; i++ {
			data, from, err := gosim.Receive()
			if err != nil {
				t.Fatal(err)
			}

			if string(data) != "ping" {
				t.Fatal(string(data))
			}

			if from != aAddr {
				t.Fatal(from)
			}
			if err := gosim.Send(aAddr, []byte("pong")); err != nil {
				t.Fatal(err)
			}
		}
	})

	a.Wait()
	b.Wait()
}

func TestNetChain(t *testing.T) {
	var machines []*gosim.Machine

	addr := func(i int) netip.Addr {
		return netip.AddrFrom4([4]byte{10, 1, 0, 1 + byte(i)})
	}

	for i := 0; i < 10; i++ {
		machines = append(machines, gosim.NewMachineWithLabel(fmt.Sprint(i), addr(i), func() {}))
	}

	machines[0].Run(func() {
		if err := gosim.Send(addr(1), []byte("ping from 0")); err != nil {
			t.Fatal(err)
		}
	})

	for i := 1; i < 9; i++ {
		i := i
		machines[i].Run(func() {
			data, from, err := gosim.Receive()
			if err != nil {
				t.Fatal(err)
			}

			if string(data) != "ping from "+fmt.Sprint(i-1) {
				t.Fatal(string(data))
			}

			if from != addr(i-1) {
				t.Fatal(from)
			}

			if err := gosim.Send(addr(i+1), []byte("ping from "+fmt.Sprint(i))); err != nil {
				t.Fatal(err)
			}
		})
	}

	machines[9].Run(func() {
		data, from, err := gosim.Receive()
		if err != nil {
			t.Fatal(err)
		}

		if string(data) != "ping from 8" {
			t.Fatal(string(data))
		}

		if from != addr(8) {
			t.Fatal(from)
		}
	})

	for i := 0; i < 10; i++ {
		machines[i].Wait()
	}
}

func TestNetAsync(t *testing.T) {
	a := gosim.NewMachineWithLabel("a", aAddr, func() {})
	b := gosim.NewMachineWithLabel("b", bAddr, func() {})

	a.Run(func() {
		go func() {
			data, from, err := gosim.Receive()
			if err != nil {
				t.Fatal(err)
			}

			if string(data) != "goodbye" {
				t.Fatal(string(data))
			}

			if from != bAddr {
				t.Fatal(from)
			}
		}()

		time.Sleep(time.Second)

		if err := gosim.Send(bAddr, []byte("hello")); err != nil {
			t.Fatal(err)
		}
	})

	b.Run(func() {
		data, from, err := gosim.Receive()
		if err != nil {
			t.Fatal(err)
		}

		if string(data) != "hello" {
			t.Fatal(string(data))
		}

		if from != aAddr {
			t.Fatal(from)
		}

		if err := gosim.Send(aAddr, []byte("goodbye")); err != nil {
			t.Fatal(err)
		}
	})

	a.Wait()
	b.Wait()
}
*/

/*

func TestDoubleListen(t *testing.T) {
	listener, err := gosim.OpenListener(80)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := gosim.OpenListener(80); err == nil || err.Error() != gosim.ErrPortAlreadyInUse.Error() {
		t.Fatal(err)
	}

	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	listener2, err := gosim.OpenListener(80)
	if err != nil {
		t.Fatal(err)
	}

	if err := listener2.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNetStreamOpenClose(t *testing.T) {
	a := gosim.NewMachineWithLabel("a", aAddr, func() {})
	b := gosim.NewMachineWithLabel("b", bAddr, func() {})

	a.Run(func() {
		listener, err := gosim.OpenListener(80)
		if err != nil {
			t.Fatal(err)
		}

		stream, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}

		if stream.Peer().Addr() != bAddr {
			t.Fatal(stream.Peer())
		}

		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}

		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	})

	b.Run(func() {
		time.Sleep(time.Second)

		stream, err := gosim.Dial(netip.AddrPortFrom(aAddr, 80))
		if err != nil {
			t.Fatal(err)
		}

		if stream.Peer().Addr() != aAddr {
			t.Fatal(stream.Peer())
		}

		if _, err := stream.Recv(); err == nil || err.Error() != gosim.ErrStreamClosed.Error() {
			t.Fatal(err)
		}
	})

	a.Wait()
	b.Wait()
}

func TestNetStreamData(t *testing.T) {
	a := gosim.NewMachineWithLabel("a", aAddr, func() {})
	b := gosim.NewMachineWithLabel("b", bAddr, func() {})

	a.Run(func() {
		listener, err := gosim.OpenListener(80)
		if err != nil {
			t.Fatal(err)
		}

		stream, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}

		if err := stream.Send([]byte("ping")); err != nil {
			t.Fatal(err)
		}

		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}

		if string(resp) != "pong" {
			t.Fatal(resp)
		}

		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}

		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	})

	b.Run(func() {
		time.Sleep(time.Second)

		stream, err := gosim.Dial(netip.AddrPortFrom(aAddr, 80))
		if err != nil {
			t.Fatal(err)
		}

		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}

		if string(resp) != "ping" {
			t.Fatal(resp)
		}

		if err := stream.Send([]byte("pong")); err != nil {
			t.Fatal(err)
		}

		if _, err := stream.Recv(); err == nil || err.Error() != gosim.ErrStreamClosed.Error() {
			t.Fatal(err)
		}
	})

	a.Wait()
	b.Wait()
}

func TestNetStreamBackpressure(t *testing.T) {
	a := gosim.NewMachineWithLabel("a", aAddr, func() {})
	b := gosim.NewMachineWithLabel("b", bAddr, func() {})

	a.Run(func() {
		listener, err := gosim.OpenListener(80)
		if err != nil {
			t.Fatal(err)
		}

		stream, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}

		last := time.Now()

		for {
			if err := stream.Send([]byte("ping")); err != nil {
				t.Fatal(err)
			}
			if now := time.Now(); !now.Equal(last) {
				gap := now.Sub(last)
				if gap != time.Second {
					s.Error("bad gap", gap)
				}
				break
			}
		}

		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}

		if string(resp) != "pong" {
			t.Fatal(resp)
		}

		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}

		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	})

	b.Run(func() {
		time.Sleep(time.Second)

		stream, err := gosim.Dial(netip.AddrPortFrom(aAddr, 80))
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(time.Second)

		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if string(resp) != "ping" {
			t.Fatal(resp)
		}

		if err := stream.Send([]byte("pong")); err != nil {
			t.Fatal(err)
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				if err.Error() == gosim.ErrStreamClosed.Error() {
					break
				}
				t.Fatal(err)
			}
			if string(resp) != "ping" {
				t.Fatal(resp)
			}
		}
	})

	a.Wait()
	b.Wait()
}

func TestNetStreamBlockRecvThenSend(t *testing.T) {
	a := gosim.NewMachineWithLabel("a", aAddr, func() {})
	b := gosim.NewMachineWithLabel("b", bAddr, func() {})

	a.Run(func() {
		listener, err := gosim.OpenListener(80)
		if err != nil {
			t.Fatal(err)
		}

		stream, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}

		done := make(chan struct{})

		go func() {
			resp, err := stream.Recv()
			if err != nil {
				t.Fatal(err)
			}

			if string(resp) != "pong" {
				t.Fatal(resp)
			}

			close(done)
		}()

		time.Sleep(time.Second)

		if err := stream.Send([]byte("ping")); err != nil {
			t.Fatal(err)
		}

		<-done

		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}

		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	})

	b.Run(func() {
		time.Sleep(time.Second)

		stream, err := gosim.Dial(netip.AddrPortFrom(aAddr, 80))
		if err != nil {
			t.Fatal(err)
		}

		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}

		if string(resp) != "ping" {
			t.Fatal(resp)
		}

		if err := stream.Send([]byte("pong")); err != nil {
			t.Fatal(err)
		}

		if _, err := stream.Recv(); err == nil || err.Error() != gosim.ErrStreamClosed.Error() {
			t.Fatal(err)
		}
	})

	a.Wait()
	b.Wait()
}

func TestNetStreamBlockSendThenRecv(t *testing.T) {
	a := gosim.NewMachineWithLabel("a", aAddr, func() {})
	b := gosim.NewMachineWithLabel("b", bAddr, func() {})

	a.Run(func() {
		listener, err := gosim.OpenListener(80)
		if err != nil {
			t.Fatal(err)
		}

		stream, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}

		done := make(chan struct{})

		go func() {
			last := time.Now()

			for {
				if err := stream.Send([]byte("ping")); err != nil {
					t.Fatal(err)
				}
				if now := time.Now(); !now.Equal(last) {
					gap := now.Sub(last)
					if gap != time.Second {
						s.Error("bad gap", gap)
					}
					break
				}
			}

			close(done)
		}()

		time.Sleep(time.Second)

		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}

		if string(resp) != "pong" {
			t.Fatal(resp)
		}

		<-done

		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}

		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	})

	b.Run(func() {
		time.Sleep(time.Second)

		stream, err := gosim.Dial(netip.AddrPortFrom(aAddr, 80))
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(time.Second)

		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if string(resp) != "ping" {
			t.Fatal(resp)
		}

		if err := stream.Send([]byte("pong")); err != nil {
			t.Fatal(err)
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				if err.Error() == gosim.ErrStreamClosed.Error() {
					break
				}
				t.Fatal(err)
			}
			if string(resp) != "ping" {
				t.Fatal(resp)
			}
		}
	})

	a.Wait()
	b.Wait()
}

func TestNetStreamCloseListenerClosesStream(t *testing.T) {
	a := gosim.NewMachineWithLabel("a", aAddr, func() {})
	b := gosim.NewMachineWithLabel("b", bAddr, func() {})

	a.Run(func() {
		listener, err := gosim.OpenListener(80)
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(time.Second)

		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	})

	b.Run(func() {
		time.Sleep(time.Second)

		stream, err := gosim.Dial(netip.AddrPortFrom(aAddr, 80))
		if err != nil {
			t.Fatal(err)
		}

		if _, err := stream.Recv(); err == nil || err.Error() != gosim.ErrStreamClosed.Error() {
			t.Fatal(err)
		}
	})

	a.Wait()
	b.Wait()
}

func TestNetStreamCloseListenerSide(t *testing.T) {
	a := gosim.NewMachineWithLabel("a", aAddr, func() {})
	b := gosim.NewMachineWithLabel("b", bAddr, func() {})

	a.Run(func() {
		listener, err := gosim.OpenListener(80)
		if err != nil {
			t.Fatal(err)
		}

		stream, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}

		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}

		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	})

	b.Run(func() {
		time.Sleep(time.Second)

		stream, err := gosim.Dial(netip.AddrPortFrom(aAddr, 80))
		if err != nil {
			t.Fatal(err)
		}

		if _, err := stream.Recv(); err == nil || err.Error() != gosim.ErrStreamClosed.Error() {
			t.Fatal(err)
		}
	})

	a.Wait()
	b.Wait()
}

func TestNetStreamCloseDialSide(t *testing.T) {
	a := gosim.NewMachineWithLabel("a", aAddr, func() {})
	b := gosim.NewMachineWithLabel("b", bAddr, func() {})

	a.Run(func() {
		listener, err := gosim.OpenListener(80)
		if err != nil {
			t.Fatal(err)
		}

		stream, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}

		if _, err := stream.Recv(); err == nil || err.Error() != gosim.ErrStreamClosed.Error() {
			t.Fatal(err)
		}

		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	})

	b.Run(func() {
		time.Sleep(time.Second)

		stream, err := gosim.Dial(netip.AddrPortFrom(aAddr, 80))
		if err != nil {
			t.Fatal(err)
		}

		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
	})

	a.Wait()
	b.Wait()
}

func TestNetStreamTwoClientsQueue(t *testing.T) {
	a := gosim.NewMachineWithLabel("a", aAddr, func() {})
	bs := []*gosim.Machine{
		gosim.NewMachineWithLabel("b1", bAddr, func() {}),
		gosim.NewMachineWithLabel("b2", bAddr.Next(), func() {}),
	}

	a.Run(func() {
		listener, err := gosim.OpenListener(80)
		if err != nil {
			t.Fatal(err)
		}

		// make sure streams are queued in listener
		time.Sleep(time.Second)

		done := make(chan struct{}, 2)

		for i := 0; i < 2; i++ {
			stream, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}

			go func() {
				// make sure both streams exist for a while concurrently
				time.Sleep(time.Second)

				if err := stream.Send([]byte("ping")); err != nil {
					t.Fatal(err)
				}

				resp, err := stream.Recv()
				if err != nil {
					t.Fatal(err)
				}

				if string(resp) != "pong" {
					t.Fatal(resp)
				}

				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}

				done <- struct{}{}
			}()
		}

		for i := 0; i < 2; i++ {
			<-done
		}

		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	})

	for _, b := range bs {
		b.Run(func() {
			// make sure it's listening??
			time.Sleep(100 * time.Millisecond)

			stream, err := gosim.Dial(netip.AddrPortFrom(aAddr, 80))
			if err != nil {
				t.Fatal(err)
			}

			resp, err := stream.Recv()
			if err != nil {
				t.Fatal(err)
			}

			if string(resp) != "ping" {
				t.Fatal(resp)
			}

			if err := stream.Send([]byte("pong")); err != nil {
				t.Fatal(err)
			}

			if _, err := stream.Recv(); err == nil || err.Error() != gosim.ErrStreamClosed.Error() {
				t.Fatal(err)
			}
		})
	}

	a.Wait()
	for _, b := range bs {
		b.Wait()
	}
}

func TestNetStreamListenerOverflow(t *testing.T) {
	a := gosim.NewMachineWithLabel("a", aAddr, func() {})
	b := gosim.NewMachineWithLabel("b", bAddr, func() {})

	a.Run(func() {
		_, err := gosim.OpenListener(80)
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(100 * time.Second)
	})

	b.Run(func() {
		time.Sleep(time.Second)

		var mu sync.Mutex
		var streams []*gosim.Stream
		var done bool

		for {
			mu.Lock()
			if done {
				mu.Unlock()
				return
			}
			mu.Unlock()

			go func() {
				stream, err := gosim.Dial(netip.AddrPortFrom(aAddr, 80))
				if err != nil {
					t.Fatal(err)
				}

				mu.Lock()
				if done {
					mu.Unlock()
					return
				}
				streams = append(streams, stream)
				mu.Unlock()

				if _, err := stream.Recv(); err != nil {
					if err.Error() == gosim.ErrStreamClosed.Error() {
						mu.Lock()
						done = true
						for _, stream := range streams {
							stream.Close()
						}
						mu.Unlock()
					}
				}
			}()

			time.Sleep(100 * time.Millisecond)
		}
	})

	a.Wait()
	b.Wait()
}

func TestNetStreamWrongPort(t *testing.T) {
	a := gosim.NewMachineWithLabel("a", aAddr, func() {})
	b := gosim.NewMachineWithLabel("b", bAddr, func() {})

	a.Run(func() {
		_, err := gosim.OpenListener(80)
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(100 * time.Second)
	})

	b.Run(func() {
		time.Sleep(time.Second)

		stream, err := gosim.Dial(netip.AddrPortFrom(aAddr, 81))
		if err != nil {
			t.Fatal(err)
		}

		if _, err := stream.Recv(); err == nil || err.Error() != gosim.ErrStreamClosed.Error() {
			t.Fatal(err)
		}
	})

	a.Wait()
	b.Wait()
}

func TestNetStreamWrongHost(t *testing.T) {
	a := gosim.NewMachineWithLabel("a", aAddr, func() {})
	b := gosim.NewMachineWithLabel("b", bAddr, func() {})

	a.Run(func() {
		_, err := gosim.OpenListener(80)
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(100 * time.Second)
	})

	b.Run(func() {
		time.Sleep(time.Second)

		stream, err := gosim.Dial(netip.AddrPortFrom(netip.AddrFrom4([4]byte{1, 2, 3, 4}), 80))
		if err != nil {
			t.Fatal(err)
		}

		if _, err := stream.Recv(); err == nil || err.Error() != gosim.ErrStreamClosed.Error() {
			t.Fatal(err)
		}
	})

	a.Wait()
	b.Wait()
}
*/
