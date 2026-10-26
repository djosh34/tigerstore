package gosimlog_test

import (
	"syscall"
	"testing"

	"github.com/jellevandenhooff/gosim/internal/gosimlog"
)

func TestBitflags(t *testing.T) {
	f := &gosimlog.BitflagFormatter{
		Choices: []gosimlog.BitflagChoice{
			{
				Mask: 0x3,
				Values: map[int]string{
					syscall.O_RDWR:   "O_RDWR",
					syscall.O_RDONLY: "O_RDONLY",
					syscall.O_WRONLY: "O_WRONLY",
				},
			},
		},
		Flags: []gosimlog.BitflagValue{
			{
				Value: syscall.O_TRUNC,
				Name:  "O_TRUNC",
			},
			{
				Value: syscall.O_CREAT,
				Name:  "O_CREAT",
			},
			{
				Value: syscall.O_APPEND,
				Name:  "O_APPEND",
			},
			{
				Value: syscall.O_EXCL,
				Name:  "O_EXCL",
			},
		},
	}

	testCases := []struct {
		value    int
		expected string
	}{
		{
			value:    syscall.O_RDONLY | syscall.O_CREAT | syscall.O_EXCL,
			expected: "O_RDONLY|O_CREAT|O_EXCL",
		},
		{
			value:    3 | syscall.O_CREAT,
			expected: "3|O_CREAT",
		},
		{
			value:    syscall.O_RDONLY | syscall.O_CREAT | 8192,
			expected: "O_RDONLY|O_CREAT|8192",
		},
	}

	for _, testCase := range testCases {
		got := f.Format(testCase.value)
		if got != testCase.expected {
			t.Errorf("format %d: got %s, expected %s", testCase.value, got, testCase.expected)
		}
	}
}
