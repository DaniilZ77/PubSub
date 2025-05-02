package service

import "sync"

func withLock(mutex sync.Locker, action func()) {
	mutex.Lock()
	defer mutex.Unlock()
	action()
}
