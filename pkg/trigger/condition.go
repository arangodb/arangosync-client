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

// Condition is a synchronization utility used to wait until someone sets it.
// Once set, it cannot be reset.
type Condition struct {
	mu   sync.Mutex
	cond *sync.Cond
	set  bool
}

// Set the condition to true.
func (c *Condition) Set() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.set = true
	if c.cond != nil {
		c.cond.Broadcast()
	}
}

// Wait blocks until the condition is set.
func (c *Condition) Wait() {
	c.mu.Lock()
	for !c.set {
		if c.cond == nil {
			c.cond = sync.NewCond(&c.mu)
		}
		c.cond.Wait()
	}
	c.mu.Unlock()
}
