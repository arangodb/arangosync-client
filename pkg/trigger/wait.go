//
// Copyright 2020-2021 ArangoDB GmbH, Cologne, Germany
//
// The Programs (which include both the software and documentation) contain
// proprietary information of ArangoDB GmbH; they are provided under a license
// agreement containing restrictions on use and disclosure and are also
// protected by copyright, patent and other intellectual and industrial
// property laws. Reverse engineering, disassembly or decompilation of the
// Programs, except to the extent required to obtain interoperability with
// other independently created software or as specified by law, is prohibited.
//
// It shall be the licensee's responsibility to take all appropriate fail-safe,
// backup, redundancy, and other measures to ensure the safe use of
// applications if the Programs are used for purposes such as nuclear,
// aviation, mass transit, medical, or other inherently dangerous applications,
// and ArangoDB GmbH disclaims liability for any damages caused by such use of
// the Programs.
//
// This software is the confidential and proprietary information of ArangoDB
// GmbH. You shall not disclose such confidential and proprietary information
// and shall use it only in accordance with the terms of the license agreement
// you entered into with ArangoDB GmbH.
//

package trigger

import (
	"context"
	"time"
)

type Action int

const (
	ContextDone Action = 0
	Triggered   Action = 1
	Expired     Action = 2
)

// WaitWithContextAndTrigger waits for the first action which will occur.
// It handles time.After function which must be stopped in some circumstances otherwise memory leaks occur.
// Possible actions: trigger is launched, context is finished, timeout occurs.
func WaitWithContextAndTrigger(ctx context.Context, duration time.Duration, trigger *Trigger) Action {
	idleDelay := time.NewTimer(duration)

	select {
	case <-trigger.Done():
		if !idleDelay.Stop() {
			<-idleDelay.C
		}
		return Triggered
	case <-idleDelay.C:
		return Expired
	case <-ctx.Done():
		if !idleDelay.Stop() {
			<-idleDelay.C
		}
		return ContextDone
	}
}

// WaitWithContext waits for the first action which will occur.
// It handles time.After function which must be stopped in some circumstances otherwise memory leaks occur.
// Possible actions: context is finished, timeout occurs.
func WaitWithContext(ctx context.Context, duration time.Duration) Action {
	idleDelay := time.NewTimer(duration)

	select {
	case <-idleDelay.C:
		return Expired
	case <-ctx.Done():
		if !idleDelay.Stop() {
			<-idleDelay.C
		}
		return ContextDone
	}
}

// WaitWithTrigger waits for the first action which will occur.
// It handles time.After function which must be stopped in some circumstances otherwise memory leaks occur.
// Possible actions: trigger is launched, timeout occurs.
func WaitWithTrigger(duration time.Duration, trigger *Trigger) Action {
	idleDelay := time.NewTimer(duration)

	select {
	case <-trigger.Done():
		if !idleDelay.Stop() {
			<-idleDelay.C
		}
		return Triggered
	case <-idleDelay.C:
		return Expired
	}
}

// WaitWithContextAndChannel waits for the first action which will occur.
// It handles time.After function which must be stopped in some circumstances otherwise memory leaks occur.
// Possible actions: channel is closed, context is finished, timeout occurs.
func WaitWithContextAndChannel(ctx context.Context, duration time.Duration, channel <-chan struct{}) Action {
	idleDelay := time.NewTimer(duration)

	select {
	case <-channel:
		if !idleDelay.Stop() {
			<-idleDelay.C
		}
		return Triggered
	case <-idleDelay.C:
		return Expired
	case <-ctx.Done():
		if !idleDelay.Stop() {
			<-idleDelay.C
		}
		return ContextDone
	}
}

// WaitWithContextAndChannelExt waits for the first action which will occur.
// It handles time.After function which must be stopped in some circumstances otherwise memory leaks occur.
// Possible actions: channel is closed, context is finished, timeout occurs.
// It returns action which was triggered and the value which was written into the channel if the channel was triggered.
// It also returns false when the given channel is closed.
func WaitWithContextAndChannelExt(ctx context.Context, duration time.Duration,
	channel <-chan interface{}) (Action, interface{}, bool) {
	idleDelay := time.NewTimer(duration)

	select {
	case value, ok := <-channel:
		if !idleDelay.Stop() {
			<-idleDelay.C
		}
		// If there are no more values in the channel and channel is closed then `ok` should be false,
		// and `value` should be nil.
		return Triggered, value, ok
	case <-idleDelay.C:
		// Expired, but channel still exist.
		return Expired, nil, true
	case <-ctx.Done():
		// Context done, but channel still exist.
		if !idleDelay.Stop() {
			<-idleDelay.C
		}
		return ContextDone, nil, true
	}
}
