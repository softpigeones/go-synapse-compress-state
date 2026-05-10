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

package state_map

// StateMap is a type alias for StateMap[string] for backward compatibility
type StateMap = StateMapString

// StateMapString is a state map with string values
type StateMapString struct {
	data map[Key]string
}

// NewStateMap creates a new empty StateMap with string values
func NewStateMap() *StateMapString {
	return &StateMapString{
		data: make(map[Key]string),
	}
}

// Set sets a value in the state map
func (sm *StateMapString) Set(eventType, stateKey, value string) {
	sm.data[Key{Type: eventType, StateKey: stateKey}] = value
}

// Get retrieves a value from the state map
func (sm *StateMapString) Get(eventType, stateKey string) (string, bool) {
	value, ok := sm.data[Key{Type: eventType, StateKey: stateKey}]
	return value, ok
}

// Contains checks if a key exists in the state map
func (sm *StateMapString) Contains(eventType, stateKey string) bool {
	_, ok := sm.data[Key{Type: eventType, StateKey: stateKey}]
	return ok
}

// Len returns the number of entries in the state map
func (sm *StateMapString) Len() int {
	return len(sm.data)
}

// Copy creates a shallow copy of the state map
func (sm *StateMapString) Copy() *StateMapString {
	newMap := &StateMapString{
		data: make(map[Key]string),
	}
	for k, v := range sm.data {
		newMap.data[k] = v
	}
	return newMap
}

// Iterator returns an iterator over the state map
func (sm *StateMapString) Iterator() *StateMapStringIterator {
	keys := make([]Key, 0, len(sm.data))
	for k := range sm.data {
		keys = append(keys, k)
	}
	return &StateMapStringIterator{
		keys: keys,
		data: sm.data,
		idx:  -1,
	}
}

// Range iterates over all entries in the state map
func (sm *StateMapString) Range(f func(Key, string) bool) {
	for k, v := range sm.data {
		if !f(k, v) {
			return
		}
	}
}

// StateMapStringIterator is an iterator for StateMapString
type StateMapStringIterator struct {
	keys []Key
	data map[Key]string
	idx  int
}

// Next advances the iterator and returns true if there are more elements
func (it *StateMapStringIterator) Next() bool {
	it.idx++
	return it.idx < len(it.keys)
}

// Key returns the current key
func (it *StateMapStringIterator) Key() Key {
	if it.idx < 0 || it.idx >= len(it.keys) {
		panic("iterator out of bounds")
	}
	return it.keys[it.idx]
}

// Value returns the current value
func (it *StateMapStringIterator) Value() string {
	if it.idx < 0 || it.idx >= len(it.keys) {
		return ""
	}
	return it.data[it.keys[it.idx]]
}

// Key represents a composite key for the state map (type, state_key)
type Key struct {
	Type     string
	StateKey string
}
