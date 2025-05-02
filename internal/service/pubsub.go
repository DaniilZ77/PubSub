package service

import (
	"context"
	"errors"
	"log/slog"
	"sync"
)

const (
	opened           = 0
	closed           = 1
	defaultQueueSize = 256
)

type subPubImpl struct {
	wg        sync.WaitGroup
	queueSize int
	mutex     sync.RWMutex
	data      map[string][]*subscriptionImpl
	closed    bool
	log       *slog.Logger
}

type subscriptionImpl struct {
	messages     chan any
	stream       chan any
	buffer       []any
	mutex        sync.RWMutex
	unsubscribed bool
	log          *slog.Logger
}

func (s *subscriptionImpl) start() {
	defer close(s.stream)
	var msg any
	var ok bool
	for {
		if len(s.buffer) > 0 {
			msg = s.buffer[0]
		} else {
			msg, ok = <-s.messages
			if !ok {
				s.log.Info("subscription closed")
				return
			}
			s.buffer = append(s.buffer, msg)
		}

		select {
		case msg, ok = <-s.messages:
			if !ok {
				for _, msg := range s.buffer {
					s.stream <- msg
				}
				s.log.Info("subscription closed")
				return
			}
			s.buffer = append(s.buffer, msg)
		case s.stream <- msg:
			s.buffer = s.buffer[1:]
		}
	}
}

func (s *subscriptionImpl) Unsubscribe() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.unsubscribed {
		return
	}
	s.unsubscribed = true
	close(s.messages)
}

func (s *subscriptionImpl) send(msg any) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if s.unsubscribed {
		return
	}
	s.messages <- msg
}

func (s *subPubImpl) Close(ctx context.Context) (err error) {
	withLock(&s.mutex, func() {
		if s.closed {
			err = ErrSubPubAlreadyClosed
			return
		}
		for _, subscriptions := range s.data {
			for _, subscription := range subscriptions {
				subscription.Unsubscribe()
			}
		}
		s.closed = true
	})
	if err != nil {
		return err
	}

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

func (s *subPubImpl) Publish(subject string, msg any) (err error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if s.closed {
		return ErrSubPubAlreadyClosed
	}
	for _, subscription := range s.data[subject] {
		subscription.send(msg)
	}
	return nil
}

func (s *subPubImpl) Subscribe(subject string, cb MessageHandler) (Subscription, error) {
	subscription := &subscriptionImpl{
		messages: make(chan any, s.queueSize),
		stream:   make(chan any),
		log:      s.log,
	}
	var err error
	withLock(&s.mutex, func() {
		if s.closed {
			err = ErrSubPubAlreadyClosed
			return
		}
		s.data[subject] = append(s.data[subject], subscription)
	})
	if err != nil {
		return nil, err
	}

	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		subscription.start()
	}()
	go func() {
		defer s.wg.Done()
		for msg := range subscription.stream {
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
