//go:build never

package main

import (
	// Janky plan to ensure that a tools import of the CLI also pulls in
	// everything else
	_ "github.com/jellevandenhooff/gosim"
)
