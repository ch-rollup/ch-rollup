package buildinfo

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInfo_String(t *testing.T) {
	t.Parallel()

	zone, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	date := time.Date(2009, time.November, 10, 23, 0, 0, 0, zone)

	tests := []struct {
		name string
		info *Info
		want string
	}{
		{
			name: "Simple",
			info: &Info{
				Version:   "1.0.0",
				GoVersion: "go 1.12",
				Commit:    "qwerty",
				Time:      date,
			},
			want: "ch-rollup version 1.0.0-qwerty (built with go 1.12 at Tue, 10 Nov 2009 23:00:00 EST)",
		},
		{
			name: "Nil",
			info: nil,
			want: "",
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, strings.TrimSuffix(tt.info.String(), " "+runtime.GOOS+"/"+runtime.GOARCH))
		})
	}
}
