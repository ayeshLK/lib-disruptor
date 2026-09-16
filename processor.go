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
	"sync/atomic"
	"time"
)

// EventHandler processes one published event in sequence order.
type EventHandler[T any] func(event T, sequence int64, endOfBatch bool) error

const maxSequence = int64(^uint64(0) >> 1)

type processorConfig struct {
	maxBatchSize int64
	batchTimeout time.Duration
}

// ProcessorOption configures a BatchProcessor or EventPoller. Batch timeout
// acquisition is used only by BatchProcessor because EventPoller never waits.
type ProcessorOption func(*processorConfig) error

// WithMaxBatchSize limits the number of events acknowledged as one batch.
func WithMaxBatchSize(size int64) ProcessorOption {
	return func(config *processorConfig) error {
		if size < 1 {
			return ErrInvalidBatchSize
		}
		config.maxBatchSize = size
		return nil
	}
}

// WithBatchTimeout allows a BatchProcessor to collect newly published
// contiguous events for at most timeout after the first event is available.
// EventPoller accepts the option but ignores it because Poll never waits.
func WithBatchTimeout(timeout time.Duration) ProcessorOption {
	return func(config *processorConfig) error {
		if timeout <= 0 {
			return ErrInvalidBatchTimeout
		}
		config.batchTimeout = timeout
		return nil
	}
}

const (
	processorIdle int32 = iota
	processorStarting
	processorRunning
	processorHalted
)

// BatchProcessor waits on a barrier, handles selected contiguous published
// events in order, skips discarded claims, then advances its consumer sequence
// once per batch. WithBatchTimeout can extend acquisition after the first
// available event. A BatchProcessor must not be copied after first use; pass
// it by pointer.
type BatchProcessor[T any] struct {
	ring         *RingBuffer[T]
	barrier      *SequenceBarrier
	handler      EventHandler[T]
	sequence     *Sequence
	maxBatchSize int64
	batchTimeout time.Duration
	state        atomic.Int32
}

// NewBatchProcessor creates an ordered consumer for ring using barrier.
func NewBatchProcessor[T any](ring *RingBuffer[T], barrier *SequenceBarrier, handler EventHandler[T], options ...ProcessorOption) (*BatchProcessor[T], error) {
	if handler == nil {
		return nil, ErrNilHandler
	}
	config := processorConfig{maxBatchSize: maxSequence}
	for _, option := range options {
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	return &BatchProcessor[T]{ring: ring, barrier: barrier, handler: handler, sequence: NewSequence(InitialSequence), maxBatchSize: config.maxBatchSize, batchTimeout: config.batchTimeout}, nil
}

// Sequence returns the processor's last fully acknowledged batch position.
func (p *BatchProcessor[T]) Sequence() *Sequence { return p.sequence }

// Running reports whether Run is ready to process events or is finishing after
// Halt. It remains true after Halt until that Run call returns.
func (p *BatchProcessor[T]) Running() bool {
	state := p.state.Load()
	return state == processorRunning || state == processorHalted
}

// Run processes events until Halt is called, the context is cancelled, the ring
// is closed, or a handler returns an error. Ring closure returns ErrClosed. On
// handler error or panic the current batch is not acknowledged, so restarting
// the processor replays that batch. Handler failures are returned as
// *HandlerError and recovered handler panics as *HandlerPanicError. With
// WithBatchTimeout, cancellation, alerts, and close finish the already selected
// range before Run returns; timeout expiry itself is normal completion.
func (p *BatchProcessor[T]) Run(ctx context.Context) error {
	if !p.state.CompareAndSwap(processorIdle, processorStarting) {
		return ErrAlreadyRunning
	}
	defer p.state.Store(processorIdle)
	p.barrier.ClearAlert()
	if !p.state.CompareAndSwap(processorStarting, processorRunning) {
		return nil
	}

	next := p.sequence.Load() + 1
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		available, err := p.barrier.WaitFor(ctx, next)
		if err != nil {
			if errors.Is(err, ErrAlerted) && p.state.Load() == processorHalted {
				return nil
			}
			return err
		}
		maxEnd := maxSequence
		if p.maxBatchSize <= maxSequence-next {
			maxEnd = next + p.maxBatchSize - 1
		}
		end := available
		if end > maxEnd {
			end = maxEnd
		}
		var acquisitionErr error
		if p.batchTimeout > 0 && end < maxEnd {
			end, acquisitionErr = p.acquireBatch(ctx, end, maxEnd)
		}
		if err := processBatch(p.ring, p.handler, next, end); err != nil {
			return err
		}
		p.sequence.Store(end)
		if acquisitionErr != nil {
			if errors.Is(acquisitionErr, ErrAlerted) && p.state.Load() == processorHalted {
				return nil
			}
			return acquisitionErr
		}
		next = end + 1
	}
}

func (p *BatchProcessor[T]) acquireBatch(ctx context.Context, end, maxEnd int64) (int64, error) {
	deadline := time.Now().Add(p.batchTimeout)
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	for end < maxEnd {
		available, err := p.barrier.WaitFor(waitCtx, end+1)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				return end, nil
			}
			return end, err
		}
		if !time.Now().Before(deadline) {
			return end, nil
		}
		if available > maxEnd {
			return maxEnd, nil
		}
		end = available
	}
	return end, nil
}

func processBatch[T any](ring *RingBuffer[T], handler EventHandler[T], next, end int64) error {
	if !ring.hasDiscardedClaims() {
		for sequence := next; sequence <= end; sequence++ {
			if err := handleEvent(ring, handler, sequence, sequence == end); err != nil {
				return err
			}
		}
		return nil
	}

	lastDelivered := end
	for lastDelivered >= next && ring.IsDiscarded(lastDelivered) {
		lastDelivered--
	}
	for sequence := next; sequence <= end; sequence++ {
		if ring.IsDiscarded(sequence) {
			continue
		}
		if err := handleEvent(ring, handler, sequence, sequence == lastDelivered); err != nil {
			return err
		}
	}
	return nil
}

func handleEvent[T any](ring *RingBuffer[T], handler EventHandler[T], sequence int64, endOfBatch bool) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = &HandlerPanicError{Sequence: sequence, Value: value}
		}
	}()
	if err := handler(ring.Get(sequence), sequence, endOfBatch); err != nil {
		return &HandlerError{Sequence: sequence, Err: err}
	}
	return nil
}

// Halt alerts the barrier and causes the active Run call to return after its
// current handler invocation and selected batch complete. Halt is a no-op when
// the processor is idle.
func (p *BatchProcessor[T]) Halt() {
	for {
		switch state := p.state.Load(); state {
		case processorIdle, processorHalted:
			return
		case processorStarting, processorRunning:
			if p.state.CompareAndSwap(state, processorHalted) {
				p.barrier.Alert()
				return
			}
		default:
			panic("disruptor: invalid processor state")
		}
	}
}
