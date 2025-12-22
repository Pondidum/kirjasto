package goes

import (
	"context"
	"fmt"
	"iter"

	"github.com/google/uuid"
)

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		Aggregates: map[uuid.UUID][]EventDescriptor{},
	}
}

type MemoryStore struct {
	Aggregates map[uuid.UUID][]EventDescriptor
}

func (s *MemoryStore) Save(ctx context.Context, aggregate Aggregate) error {
	state := aggregate.state()
	storedEvents, found := s.Aggregates[state.id]
	if found {
		dbSequence := storedEvents[len(storedEvents)-1].Sequence
		if dbSequence > state.sequence {
			return fmt.Errorf("aggregate has new events in the database. db: %v, memory: %v", dbSequence, state.sequence)
		}
	}

	s.Aggregates[state.id] = append(storedEvents, state.pendingEvents...)
	return nil
}

func (s *MemoryStore) Load(ctx context.Context, aggregateID uuid.UUID) iter.Seq2[EventDescriptor, error] {
	return func(yield func(EventDescriptor, error) bool) {
	}
}
