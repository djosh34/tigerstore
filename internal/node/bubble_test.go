//go:build !sim

package node

import (
	"testing"
	"testing/synctest"
)

func bubble(t *testing.T, f func(*testing.T)) { synctest.Test(t, f) }
