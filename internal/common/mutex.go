package common

import "sync"

func WithLock(mutex sync.Locker, action func()) {
	mutex.Lock()
	defer mutex.Unlock()
	action()
}
