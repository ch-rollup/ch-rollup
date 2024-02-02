package duration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDuration_MarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		duration time.Duration
		want     []byte
	}{
		{
			name:     "Simple",
			duration: time.Minute * 30,
			want:     []byte(`"30m0s"`),
		},
		{
			name:     "Nil",
			duration: 0,
			want:     []byte(`"0s"`),
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := Duration{
				Duration: tt.duration,
			}

			bytes, _ := d.MarshalJSON()

			assert.Equal(t, tt.want, bytes)
		})
	}
}

func TestDuration_UnmarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    time.Duration
		wantErr bool
	}{
		{
			name:  "Simple",
			input: `"10m"`,
			want:  time.Minute * 10,
		},
		{
			name:    "Bad",
			input:   `10m"`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var d Duration

			err := d.UnmarshalJSON([]byte(tt.input))

			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.want, d.Duration)
		})
	}
}
