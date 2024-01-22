package scheduler

import (
	"context"
	"github.com/ch-rollup/ch-rollup/pkg/database"
	"github.com/ch-rollup/ch-rollup/pkg/types"
	"sync"
	"time"
)

type dataBase interface {
	RollUp(ctx context.Context, opts database.RollUpOptions) error
}

const (
	defaultSchedulerInterval = time.Hour
)

type Scheduler struct {
	tasks []types.Task
	db    dataBase
	lock  sync.RWMutex
}

func New(ctx context.Context, db dataBase, tasks types.Tasks) (*Scheduler, error) {
	if err := tasks.Validate(); err != nil {
		return nil, err
	}

	s := &Scheduler{
		tasks: tasks,
		db:    db,
	}

	if err := s.rollUp(ctx); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Scheduler) Run(ctx context.Context) (<-chan Event, error) {
	eventChan := make(chan Event)

	go func() {
		defer close(eventChan)

		ticker := time.NewTicker(defaultSchedulerInterval)

		for {
			select {
			case <-ticker.C:
				err := s.rollUp(ctx)

				eventChan <- Event{
					Type:  EventTypeRollUp,
					Error: err,
				}

				ticker.Reset(defaultSchedulerInterval)
			case <-ctx.Done():
				return
			}
		}
	}()

	return eventChan, nil
}

func (s *Scheduler) getTasks() []types.Task {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.tasks
}
