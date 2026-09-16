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
	"testing"
	"time"
)

func TestBatchProcessorTimedAcquisitionDoesNotCrossPublicationGap(t *testing.T) {
	ring := newTestRing(t, 8, MultiProducer, BlockingWait())
	first, err := ring.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	gap, err := ring.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	later, err := ring.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ring.PublishSequence(first)
	ring.PublishSequence(later)

	seen := make([]int64, 0, 2)
	var processor *BatchProcessor[*testEvent]
	processor, err = NewBatchProcessor(
		ring,
		ring.NewBarrier(),
		func(_ *testEvent, sequence int64, endOfBatch bool) error {
			seen = append(seen, sequence)
			if endOfBatch {
				processor.Halt()
			}
			return nil
		},
		WithBatchTimeout(20*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() { result <- processor.Run(context.Background()) }()
	if err := receiveProcessorResult(t, result); err != nil {
		t.Fatalf("processor: %v", err)
	}
	if len(seen) != 1 || seen[0] != first {
		t.Fatalf("events across gap: %v", seen)
	}
	if got := processor.Sequence().Load(); got != first {
		t.Fatalf("processor sequence: got %d, want %d", got, first)
	}
	ring.DiscardSequence(gap)
}
