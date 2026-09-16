// Copyright 2026 Ayesh Almeida
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disruptor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type signalFirstWait struct {
	inner      WaitStrategy
	firstReady chan struct{}
	once       sync.Once
}

func (s *signalFirstWait) waitFor(ctx context.Context, desired int64, cursor, dependent sequenceReader, state waitState) (int64, error) {
	available, err := s.inner.waitFor(ctx, desired, cursor, dependent, state)
	if err == nil {
		s.once.Do(func() { close(s.firstReady) })
	}
	return available, err
}

func (s *signalFirstWait) signalAll() { s.inner.signalAll() }

func TestBatchProcessorValidatesBatchTimeout(t *testing.T) {
	ring := newTestRing(t, 8, SingleProducer, YieldingWait())
	handler := func(*testEvent, int64, bool) error { return nil }
	for _, timeout := range []time.Duration{0, -time.Nanosecond} {
		if _, err := NewBatchProcessor(ring, ring.NewBarrier(), handler, WithBatchTimeout(timeout)); !errors.Is(err, ErrInvalidBatchTimeout) {
			t.Fatalf("timeout %s: got %v", timeout, err)
		}
	}
}

func TestBatchProcessorTimedAcquisitionCollectsPublishedEvents(t *testing.T) {
	wait := &signalFirstWait{inner: BlockingWait(), firstReady: make(chan struct{})}
	ring := newTestRing(t, 8, SingleProducer, wait)
	seen := make([]int64, 0, 2)
	ends := make([]bool, 0, 2)
	var processor *BatchProcessor[*testEvent]
	var err error
	processor, err = NewBatchProcessor(
		ring,
		ring.NewBarrier(),
		func(event *testEvent, _ int64, endOfBatch bool) error {
			seen = append(seen, event.Value)
			ends = append(ends, endOfBatch)
			if endOfBatch {
				processor.Halt()
			}
			return nil
		},
		WithMaxBatchSize(2),
		WithBatchTimeout(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() { result <- processor.Run(context.Background()) }()
	if err := ring.Publish(context.Background(), func(event *testEvent, _ int64) error {
		event.Value = 1
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-wait.firstReady
	if err := ring.Publish(context.Background(), func(event *testEvent, _ int64) error {
		event.Value = 2
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := receiveProcessorResult(t, result); err != nil {
		t.Fatalf("processor: %v", err)
	}
	if len(seen) != 2 || seen[0] != 1 || seen[1] != 2 {
		t.Fatalf("events: %v", seen)
	}
	if len(ends) != 2 || ends[0] || !ends[1] {
		t.Fatalf("end markers: %v", ends)
	}
}

func TestBatchProcessorTimedAcquisitionExpiresNormally(t *testing.T) {
	wait := &signalFirstWait{inner: BlockingWait(), firstReady: make(chan struct{})}
	ring := newTestRing(t, 8, SingleProducer, wait)
	seen := make([]int64, 0, 1)
	var processor *BatchProcessor[*testEvent]
	var err error
	processor, err = NewBatchProcessor(
		ring,
		ring.NewBarrier(),
		func(event *testEvent, _ int64, endOfBatch bool) error {
			seen = append(seen, event.Value)
			if !endOfBatch {
				return errors.New("single-event batch was not terminated")
			}
			processor.Halt()
			return nil
		},
		WithMaxBatchSize(2),
		WithBatchTimeout(20*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() { result <- processor.Run(context.Background()) }()
	if err := ring.Publish(context.Background(), func(event *testEvent, _ int64) error {
		event.Value = 1
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-wait.firstReady

	if err := receiveProcessorResult(t, result); err != nil {
		t.Fatalf("processor: %v", err)
	}
	if len(seen) != 1 || seen[0] != 1 {
		t.Fatalf("events: %v", seen)
	}
}

func TestBatchProcessorTimedAcquisitionPreservesCancellation(t *testing.T) {
	wait := &signalFirstWait{inner: BlockingWait(), firstReady: make(chan struct{})}
	ring := newTestRing(t, 8, SingleProducer, wait)
	seen := make([]int64, 0, 1)
	var processor *BatchProcessor[*testEvent]
	var err error
	processor, err = NewBatchProcessor(
		ring,
		ring.NewBarrier(),
		func(event *testEvent, _ int64, endOfBatch bool) error {
			seen = append(seen, event.Value)
			if !endOfBatch {
				return errors.New("single-event batch was not terminated")
			}
			return nil
		},
		WithMaxBatchSize(2),
		WithBatchTimeout(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- processor.Run(ctx) }()
	if err := ring.Publish(context.Background(), func(event *testEvent, _ int64) error {
		event.Value = 1
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-wait.firstReady
	cancel()

	if err := receiveProcessorResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("processor: got %v", err)
	}
	if len(seen) != 1 || seen[0] != 1 || processor.Sequence().Load() != 0 {
		t.Fatalf("selected batch: events=%v sequence=%d", seen, processor.Sequence().Load())
	}
}
