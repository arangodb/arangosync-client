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

// TriggerGroup is a set of Trigger objects, all sharing a single trigger,
// but having individual Done() channels.
type TriggerGroup struct {
	mu       sync.Mutex
	elements []*Trigger
}

// Add a Trigger element.
func (g *TriggerGroup) Add() *Trigger {
	g.mu.Lock()
	defer g.mu.Unlock()

	t := &Trigger{}
	g.elements = append(g.elements, t)

	return t
}

// Remove a Trigger element.
func (g *TriggerGroup) Remove(t *Trigger) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for i := range g.elements {
		if g.elements[i] == t {
			g.elements = append(g.elements[:i], g.elements[i+1:]...)
			break
		}
	}
}

// Trigger all entries
func (g *TriggerGroup) Trigger() {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, t := range g.elements {
		t.Trigger()
	}
}
