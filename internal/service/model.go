package service

import (
	"context"
	"errors"
)

var (
	ErrSubPubAlreadyClosed = errors.New("sub pub already closed")
)

type MessageHandler func(msg any)

//go:generate mockery --name=Subscription --case=snake --inpackage --inpackage-suffix --with-expecter
type Subscription interface {
	Unsubscribe()
}

//go:generate mockery --name=SubPub --case=snake --inpackage --inpackage-suffix --with-expecter
type SubPub interface {
	Subscribe(subject string, cb MessageHandler) (Subscription, error)
	Publish(subject string, msg any) error
	Close(ctx context.Context) error
}
