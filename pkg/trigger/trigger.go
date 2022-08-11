//
// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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

import "sync"

// Trigger is a synchronization utility used to wait (in a select statement)
// until someone triggers it.
type Trigger struct {
	mu              sync.Mutex
	done            chan struct{}
	pendingTriggers int
}

// Done returns the channel to use in a select case.
// This channel is closed when someone calls Trigger.
func (t *Trigger) Done() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.done == nil {
		t.done = make(chan struct{})
	}
	if t.pendingTriggers > 0 {
		t.pendingTriggers = 0
		d := t.done
		close(t.done)
		t.done = nil
		return d
	}
	return t.done
}

// Trigger closes any Done channel.
func (t *Trigger) Trigger() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pendingTriggers++
	if t.done != nil {
		close(t.done)
		t.done = nil
	}
}
