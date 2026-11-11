// udpprobe: real UDP tests from a Codex Cloud VM (#630). Throwaway.
//
//	udpprobe stun host:port...      send a STUN binding request to each, print the answer or timeout
//	udpprobe echo :port             UDP echo server
//	udpprobe send host:port n       send n datagrams, print replies (and RTT)
//	udpprobe tcp host:port...       raw TCP dial, no proxy
package main

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("usage: udpprobe stun|echo|send|tcp ...")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "stun":
		for _, a := range os.Args[2:] {
			stun(a)
		}
	case "echo":
		echo(os.Args[2])
	case "send":
		n, _ := strconv.Atoi(os.Args[3])
		send(os.Args[2], n)
	case "tcp":
		for _, a := range os.Args[2:] {
			t0 := time.Now()
			c, err := net.DialTimeout("tcp", a, 5*time.Second)
			if err != nil {
				fmt.Printf("tcp %s: %v (%v)\n", a, err, time.Since(t0).Round(time.Millisecond))
				continue
			}
			fmt.Printf("tcp %s: connected local=%s remote=%s in %v\n", a, c.LocalAddr(), c.RemoteAddr(), time.Since(t0).Round(time.Millisecond))
			c.Close()
		}
	}
}

func stun(addr string) {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		fmt.Printf("stun %s: resolve: %v\n", addr, err)
		return
	}
	c, err := net.ListenUDP("udp", nil)
	if err != nil {
		fmt.Printf("stun %s: listen: %v\n", addr, err)
		return
	}
	defer c.Close()
	req := make([]byte, 20)
	binary.BigEndian.PutUint16(req[0:], 0x0001) // Binding Request
	binary.BigEndian.PutUint16(req[2:], 0)
	binary.BigEndian.PutUint32(req[4:], 0x2112A442)
	rand.Read(req[8:20])
	buf := make([]byte, 1500)
	for try := 0; try < 3; try++ {
		t0 := time.Now()
		if _, err := c.WriteToUDP(req, ua); err != nil {
			fmt.Printf("stun %s (%s): write: %v\n", addr, ua, err)
			return
		}
		c.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, from, err := c.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("stun %s (%s) try %d: %v\n", addr, ua, try, err)
			continue
		}
		fmt.Printf("stun %s (%s): reply %d bytes from %s in %v, local %s, mapped %s\n", addr, ua, n, from, time.Since(t0).Round(time.Microsecond), c.LocalAddr(), mapped(buf[:n]))
		return
	}
}

func mapped(b []byte) string {
	if len(b) < 20 {
		return "?"
	}
	for p := 20; p+4 <= len(b); {
		t := binary.BigEndian.Uint16(b[p:])
		l := int(binary.BigEndian.Uint16(b[p+2:]))
		v := b[p+4 : min(p+4+l, len(b))]
		if (t == 0x0020 || t == 0x0001) && len(v) >= 8 && v[1] == 1 {
			port := binary.BigEndian.Uint16(v[2:])
			ip := net.IP(append([]byte(nil), v[4:8]...))
			if t == 0x0020 {
				port ^= 0x2112
				for i := range ip {
					ip[i] ^= []byte{0x21, 0x12, 0xA4, 0x42}[i]
				}
			}
			return fmt.Sprintf("%s:%d", ip, port)
		}
		p += 4 + (l+3)&^3
	}
	return "?"
}

func echo(addr string) {
	ua, _ := net.ResolveUDPAddr("udp", addr)
	c, err := net.ListenUDP("udp", ua)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("echo on", c.LocalAddr())
	buf := make([]byte, 65536)
	for {
		n, from, err := c.ReadFromUDP(buf)
		if err != nil {
			continue
		}
		fmt.Printf("%s got %d from %s\n", time.Now().Format(time.RFC3339), n, from)
		c.WriteToUDP(buf[:n], from)
	}
}

func send(addr string, n int) {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	c, err := net.ListenUDP("udp", nil)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	got := 0
	buf := make([]byte, 1500)
	for i := 0; i < n; i++ {
		t0 := time.Now()
		c.WriteToUDP([]byte(fmt.Sprintf("ping %d", i)), ua)
		c.SetReadDeadline(time.Now().Add(1 * time.Second))
		m, from, err := c.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("send %d to %s: %v\n", i, ua, err)
			continue
		}
		got++
		fmt.Printf("send %d to %s: reply %q from %s in %v\n", i, ua, buf[:m], from, time.Since(t0).Round(time.Microsecond))
	}
	fmt.Printf("%d/%d replies\n", got, n)
}
