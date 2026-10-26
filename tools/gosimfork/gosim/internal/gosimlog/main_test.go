package gosimlog_test

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/jellevandenhooff/gosim/internal/gosimlog"
)

func TestUnmarshalLog(t *testing.T) {
	testcases := []struct {
		json     string
		expected *gosimlog.Log
	}{
		{
			json: `{
				"msg": "hello",
				"extra": "1",
				"extra2": "2"
			}`,
			expected: &gosimlog.Log{
				Msg: "hello",
				Unknown: []gosimlog.UnknownField{
					{
						Key:   "extra",
						Value: `"1"`,
					},
					{
						Key:   "extra2",
						Value: `"2"`,
					},
				},
			},
		},
	}

	for _, tc := range testcases {
		var got gosimlog.Log
		if err := json.Unmarshal([]byte(tc.json), &got); err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(&got, tc.expected); diff != "" {
			t.Errorf("unexpected diff unmarshaling %q: %s", tc.json, diff)
		}
	}
}
