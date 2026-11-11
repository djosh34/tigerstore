// Command connectproxy is a crude HTTP CONNECT-only proxy, standing in for
// the "HTTP CONNECT proxy at proxy:8088" that Codex Cloud environments put in
// front of all egress. It logs each CONNECT target. Throwaway (#630).
package main

import (
	"bufio"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:8088", "listen address")
	flag.Parse()
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("connectproxy on %v", ln.Addr())
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go handle(c)
	}
}

func handle(c net.Conn) {
	defer c.Close()
	br := bufio.NewReader(c)
	req, err := http.ReadRequest(br)
	if err != nil {
		return
	}
	if req.Method != http.MethodConnect {
		log.Printf("refused %s %s (CONNECT only)", req.Method, req.URL)
		io.WriteString(c, "HTTP/1.1 405 Method Not Allowed\r\n\r\n")
		return
	}
	up, err := net.Dial("tcp", req.Host)
	if err != nil {
		log.Printf("CONNECT %s: %v", req.Host, err)
		io.WriteString(c, "HTTP/1.1 502 Bad Gateway\r\n\r\n")
		return
	}
	defer up.Close()
	log.Printf("CONNECT %s from %v", req.Host, c.RemoteAddr())
	io.WriteString(c, "HTTP/1.1 200 Connection established\r\n\r\n")
	go func() {
		io.Copy(up, br)
		up.(*net.TCPConn).CloseWrite()
	}()
	io.Copy(c, up)
}
