// Package duration is a utils for working with duration in json.
package duration

import (
	"encoding/json"
	"errors"
	"time"
)

// Duration ...
type Duration struct {
	time.Duration
}

// MarshalJSON ...
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// ErrInvalidDuration ...
var ErrInvalidDuration = errors.New("invalid duration")

// UnmarshalJSON ...
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
		return nil
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return ErrInvalidDuration
	}
}
