package service

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestSubPub(t *testing.T) {
	defer goleak.VerifyNone(t)

	subPub, err := NewSubPub(256, 0, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	defer subPub.Close(context.Background()) // nolint

	var counter int64

	sub1, err := subPub.Subscribe("subject1", func(msg any) {
		atomic.AddInt64(&counter, 1)
		assert.Equal(t, "value", msg)
	})
	require.NoError(t, err)
	defer sub1.Unsubscribe()

	sub2, err := subPub.Subscribe("subject2", func(msg any) {
		atomic.AddInt64(&counter, 1)
		assert.Equal(t, "value", msg)
	})
	require.NoError(t, err)
	defer sub2.Unsubscribe()

	err = subPub.Publish("subject1", "value")
	require.NoError(t, err)

	err = subPub.Publish("subject2", "value")
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int64(2), atomic.LoadInt64(&counter))
}

func TestSubPubUnsubscribe(t *testing.T) {
	defer goleak.VerifyNone(t)

	subPub, err := NewSubPub(256, 0, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	defer subPub.Close(context.Background()) // nolint

	var counter int64

	sub1, err := subPub.Subscribe("subject1", func(msg any) {
		atomic.AddInt64(&counter, 1)
	})
	require.NoError(t, err)
	sub1.Unsubscribe()

	sub2, err := subPub.Subscribe("subject1", func(msg any) {
		atomic.AddInt64(&counter, 1)
	})
	require.NoError(t, err)
	defer sub2.Unsubscribe()

	err = subPub.Publish("subject1", "value")
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int64(1), atomic.LoadInt64(&counter))
}

func TestSubPubClose(t *testing.T) {
	defer goleak.VerifyNone(t)

	subPub, err := NewSubPub(256, 0, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	defer subPub.Close(context.Background()) // nolint

	var counter int64

	sub1, err := subPub.Subscribe("subject1", func(msg any) {
		atomic.AddInt64(&counter, 1)
	})
	require.NoError(t, err)
	defer sub1.Unsubscribe()
	defer sub1.Unsubscribe()

	err = subPub.Close(context.Background())
	require.NoError(t, err)
	err = subPub.Close(context.Background())
	require.ErrorIs(t, err, ErrSubPubAlreadyClosed)

	err = subPub.Publish("subject1", "value")
	require.ErrorIs(t, err, ErrSubPubAlreadyClosed)

	_, err = subPub.Subscribe("subject1", func(msg any) {
		atomic.AddInt64(&counter, 1)
	})
	require.ErrorIs(t, err, ErrSubPubAlreadyClosed)

	assert.Equal(t, int64(0), atomic.LoadInt64(&counter))
}

// BenchmarkSubPub-11       1244530               935.0 ns/op           367 B/op          15 allocs/op
func BenchmarkSubPub(b *testing.B) {
	subPub, _ := NewSubPub(256, 0, slog.New(slog.DiscardHandler))
	defer subPub.Close(context.Background()) // nolint
	const workers = 10
	wg := sync.WaitGroup{}
	wg.Add(workers)
	for worker := range workers {
		subject := "subject" + strconv.Itoa(worker)
		sub, _ := subPub.Subscribe(subject, func(any) {})
		defer sub.Unsubscribe()
		go func() {
			defer wg.Done()
			for i := range b.N {
				_ = subPub.Publish(subject, i)
			}
		}()
	}
	wg.Wait()
}
