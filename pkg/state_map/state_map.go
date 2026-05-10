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

// Key represents a composite key for the state map (type, state_key)
type Key struct {
	Type     string
	StateKey string
}

// StateMap is a map from (type, state_key) to value V
type StateMap[V any] struct {
	data map[Key]V
}

// New creates a new empty StateMap
func New[V any]() *StateMap[V] {
	return &StateMap[V]{
		data: make(map[Key]V),
	}
}

// Set sets a value in the state map
func (sm *StateMap[V]) Set(eventType, stateKey string, value V) {
	sm.data[Key{Type: eventType, StateKey: stateKey}] = value
}

// Get retrieves a value from the state map
func (sm *StateMap[V]) Get(eventType, stateKey string) (V, bool) {
	value, ok := sm.data[Key{Type: eventType, StateKey: stateKey}]
	return value, ok
}

// Contains checks if a key exists in the state map
func (sm *StateMap[V]) Contains(eventType, stateKey string) bool {
	_, ok := sm.data[Key{Type: eventType, StateKey: stateKey}]
	return ok
}

// Len returns the number of entries in the state map
func (sm *StateMap[V]) Len() int {
	return len(sm.data)
}

// Copy creates a shallow copy of the state map
func (sm *StateMap[V]) Copy() *StateMap[V] {
	newMap := New[V]()
	for k, v := range sm.data {
		newMap.data[k] = v
	}
	return newMap
}

// Iterator returns an iterator over the state map
func (sm *StateMap[V]) Iterator() *StateMapIterator[V] {
	keys := make([]Key, 0, len(sm.data))
	for k := range sm.data {
		keys = append(keys, k)
	}
	return &StateMapIterator[V]{
		keys: keys,
		data: sm.data,
		idx:  -1,
	}
}

// Range iterates over all entries in the state map
func (sm *StateMap[V]) Range(f func(Key, V) bool) {
	for k, v := range sm.data {
		if !f(k, v) {
			return
		}
	}
}

// StateMapIterator is an iterator for StateMap
type StateMapIterator[V any] struct {
	keys []Key
	data map[Key]V
	idx  int
}

// Next advances the iterator and returns true if there are more elements
func (it *StateMapIterator[V]) Next() bool {
	it.idx++
	return it.idx < len(it.keys)
}

// Key returns the current key
func (it *StateMapIterator[V]) Key() Key {
	if it.idx < 0 || it.idx >= len(it.keys) {
		panic("iterator out of bounds")
	}
	return it.keys[it.idx]
}

// Value returns the current value
func (it *StateMapIterator[V]) Value() V {
	if it.idx < 0 || it.idx >= len(it.keys) {
		var zero V
		return zero
	}
	return it.data[it.keys[it.idx]]
}
