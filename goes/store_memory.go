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

func (s *MemoryStore) Save(ctx context.Context, aggregateID uuid.UUID, sequence int, events []EventDescriptor) error {
	storedEvents, found := s.Aggregates[aggregateID]
	if found {
		dbSequence := storedEvents[len(storedEvents)-1].Sequence
		if dbSequence > sequence {
			return fmt.Errorf("aggregate has new events in the database. db: %v, memory: %v", dbSequence, sequence)
		}
	}

	s.Aggregates[aggregateID] = append(storedEvents, events...)
	return nil
}

func (s *MemoryStore) Load(ctx context.Context, aggregateID uuid.UUID) iter.Seq2[EventDescriptor, error] {
	return func(yield func(EventDescriptor, error) bool) {
	}
}
