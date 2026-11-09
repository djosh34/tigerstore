package main

// Research-only boundary snapshots. The server binary remains exactly 407cf5ec.
import (
	"fmt"
	"os"
	"os/exec"
)

func research573Boundary(i int) error {
	script := os.Getenv("RESEARCH573_BOUNDARY")
	if script == "" {
		return nil
	}
	c := exec.Command(script, fmt.Sprint(i))
	c.Stdout, c.Stderr = os.Stdout, os.Stderr
	return c.Run()
}
