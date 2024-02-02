package slice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertFuncWithSkip(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name  string
		slice []string
		want  []string
	}
	tests := []testCase{
		{
			name: "Simple",
			slice: []string{
				"s1", "s2", "s3",
			},
			want: []string{
				"s1", "s2", "s3",
			},
		},
		{
			name:  "Nil",
			slice: nil,
			want:  nil,
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(
				t,
				tt.want,
				ConvertFuncWithSkip(tt.slice,
					func(elem string) (string, bool) {
						return elem, false
					},
				),
			)
		})
	}
}

func TestConvertFunc(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name  string
		slice []string
		want  []string
	}
	tests := []testCase{
		{
			name: "Simple",
			slice: []string{
				"s1", "s2", "s3",
			},
			want: []string{
				"s1", "s2", "s3",
			},
		},
		{
			name:  "Nil",
			slice: nil,
			want:  nil,
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(
				t,
				tt.want,
				ConvertFunc(tt.slice,
					func(elem string) string {
						return elem
					},
				),
			)
		})
	}
}
