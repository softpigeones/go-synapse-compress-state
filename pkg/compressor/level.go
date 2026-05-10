// Copyright 2018 New Vector Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compressor

// Level holds information about a particular level.
type Level struct {
	// The maximum size this level is allowed to be
	MaxLength int
	// The (approximate) current chain length of this level. This is equivalent
	// to recursively following `Head`
	CurrentChainLength int
	// The head of this level
	Head *int64
}

// NewLevel creates a new Level with the given maximum length
func NewLevel(maxLength int) *Level {
	return &Level{
		MaxLength:          maxLength,
		CurrentChainLength: 0,
		Head:               nil,
	}
}

// RestoreLevel creates a new level from stored state
func RestoreLevel(maxLength, currentChainLength int, head *int64) *Level {
	return &Level{
		MaxLength:          maxLength,
		CurrentChainLength: currentChainLength,
		Head:               head,
	}
}

// Update updates the current head of this level. If delta is true then it means
// that given state group will (probably) reference the previous head.
//
// Panics if `delta` is true and the level is already full.
func (l *Level) Update(newHead int64, delta bool) {
	l.Head = &newHead

	if delta {
		// If we're referencing the previous head then increment our chain
		// length estimate
		if !l.HasSpace() {
			panic("Tried to add to an already full level")
		}
		l.CurrentChainLength++
	} else {
		// Otherwise, we've started a new chain with a single entry.
		l.CurrentChainLength = 1
	}
}

// GetMaxLength gets the max length of the level
func (l *Level) GetMaxLength() int {
	return l.MaxLength
}

// GetCurrentLength gets the current length of the level
func (l *Level) GetCurrentLength() int {
	return l.CurrentChainLength
}

// GetHead gets the current head of the level
func (l *Level) GetHead() *int64 {
	return l.Head
}

// HasSpace returns whether there is space in the current chain at this level. If not then a
// new chain should be started.
func (l *Level) HasSpace() bool {
	return l.CurrentChainLength < l.MaxLength
}
