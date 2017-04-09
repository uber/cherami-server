// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common

import (
	"fmt"
	"runtime"
	"sync"
)

var PanicOnAutoLockFinalize = false

// AutoLock provides a local object wrapper for lock management. It also provides a failsafe
// finalizer that will unlock an autoLock when the object is garbage collected, which is similar
// to, but less-performant than a C++ style auto-unlock destructor
//
// Use the finalizing version if you don't want to defer for performance reasons, but you want assurance
// that your lock must be freed
//
// * B..b..but I need an RWMutex auto locker!
//
//   Pass your sync.RWMutex as-is for a W-autoLock, and use the sync.RWMutex.RLocker() for an R-autoLock
// 
// Example:
// 
// type foo struct {
// 	l sync.RWMutex
// }
// 
// func (f *foo) someFunc() {
// 	l := AutoLock(&f.l.RLocker())
// 	defer l.Unlock()
// 	
// 	// Do some reading
// 	
// 	if !needWrite {
// 		return
// 	}
// 	
// 	l.Unlock()
// 	l.LockWithNew(&f.l) // Write lock
// 	
// 	// Do the write
// 
// 	if somethingHappens {
// 		l.Unlock()
// 		// Do stuff outside lock
// 		l.Lock()
// 		// Do stuff inside lock
// 	}
// 	
// 	// Don't need to worry about unlocking
// }
// 

type autoLock struct {
	locked bool
	l      sync.Locker
}

func AutoLock(l sync.Locker) *autoLock {
	l.Lock()
	return &autoLock{
		locked: true,
		l:      l,
	}
}

func AutoLockWithFinalizer(l sync.Locker) *autoLock {
	a := AutoLock(l)
	runtime.SetFinalizer(a, lockFinalizer)
	return a
}

func (a *autoLock) Unlock() {
	// Unlock will panic if the state is not locked; 
	// need to check since we may be called in a defer
	if a.locked { 
		a.l.Unlock()
	}
	a.locked = false
}

func (a *autoLock) Lock() {
	a.l.Lock()
	a.locked = true
}

func (a *autoLock) LockWithNew(l sync.Locker) {
	a.l = l
	a.l.Lock()
	a.locked = true
}

func lockFinalizer(a *autoLock) {
	if a.locked {
		if PanicOnAutoLockFinalize {
			panic(fmt.Sprintf("lock not released: %v", a))
		}
		a.Unlock()
	}
}
