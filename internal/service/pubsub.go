package service

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DaniilZ77/vk-task/internal/common"
)

const (
	opened             = 0
	closed             = 1
	defaultQueueSize   = 256
	defaultSendTimeout = 500 * time.Millisecond
)

type subPubImpl struct {
	wg        sync.WaitGroup
	queueSize int
	mutex     sync.RWMutex
	data      map[string][]*subscriptionImpl
	closed    int32
	log       *slog.Logger
}

type subscriptionImpl struct {
	messages     chan any
	mutex        sync.RWMutex
	unsubscribed bool
	log          *slog.Logger
}

func (s *subscriptionImpl) Unsubscribe() {
	common.WithLock(&s.mutex, func() {
		if s.unsubscribed {
			return
		}
		s.unsubscribed = true
		close(s.messages)
	})
}

func (s *subscriptionImpl) send(msg any) {
	common.WithLock(s.mutex.RLocker(), func() {
		if s.unsubscribed {
			return
		}
		select {
		case s.messages <- msg:
		default:
			s.log.Warn("failed to publish message", slog.Any("message", msg))
		}
	})
}

func (s *subPubImpl) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&s.closed, opened, closed) {
		return ErrSubPubAlreadyClosed
	}

	common.WithLock(&s.mutex, func() {
		for _, subscriptions := range s.data {
			for _, subscription := range subscriptions {
				subscription.Unsubscribe()
			}
		}
		s.data = nil
	})

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.log.Info("pub sub closed")
	case <-ctx.Done():
		s.log.Warn("context cancelled")
		return ctx.Err()
	}

	return nil
}

func (s *subPubImpl) Publish(subject string, msg any) error {
	if atomic.LoadInt32(&s.closed) == closed {
		return ErrSubPubAlreadyClosed
	}

	common.WithLock(s.mutex.RLocker(), func() {
		for _, subscription := range s.data[subject] {
			subscription.send(msg)
		}
	})

	return nil
}

func (s *subPubImpl) Subscribe(subject string, cb MessageHandler) (Subscription, error) {
	if atomic.LoadInt32(&s.closed) == closed {
		return nil, ErrSubPubAlreadyClosed
	}

	subscription := &subscriptionImpl{
		messages: make(chan any, s.queueSize),
		log:      s.log,
	}
	var err error
	common.WithLock(&s.mutex, func() {
		if s.data == nil {
			err = ErrSubPubAlreadyClosed
			return
		}
		s.data[subject] = append(s.data[subject], subscription)
	})
	if err != nil {
		return nil, err
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for msg := range subscription.messages {
			cb(msg)
		}
	}()

	return subscription, nil
}

func NewSubPub(queueSize int, log *slog.Logger) (SubPub, error) {
	if queueSize <= 0 {
		queueSize = defaultQueueSize
	}
	if log == nil {
		return nil, errors.New("logger cannot be nil")
	}

	return &subPubImpl{
		data:      make(map[string][]*subscriptionImpl),
		queueSize: queueSize,
		log:       log,
	}, nil
}
