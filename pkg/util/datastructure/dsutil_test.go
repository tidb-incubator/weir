package datastructure

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringSliceToSet(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want map[string]struct{}
	}{
		{name: "nil", args: nil, want: map[string]struct{}{}},
		{name: "one", args: []string{"db0"}, want: map[string]struct{}{"db0": {}}},
		{name: "two", args: []string{"db0", "db1"}, want: map[string]struct{}{"db0": {}, "db1": {}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ret := StringSliceToSet(tt.args)
			assert.Equal(t, ret, tt.want)
		})
	}
}
