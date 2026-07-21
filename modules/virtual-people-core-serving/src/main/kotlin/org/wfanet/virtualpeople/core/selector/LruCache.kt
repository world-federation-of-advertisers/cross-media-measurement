// Copyright 2023 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.virtualpeople.core.selector

/**
 * Implementation of LruCache. It supports the principle of least recently used. Items that haven't
 * been accessed or used for the longest are evicted first when the cache is full.
 */
class LruCache<K, V>(private val capacity: Int) : LinkedHashMap<K, V>(capacity, 0.75f, true) {

  /**
   * Each time a new element is added to the LinkedHashedMap, this method determines whether the
   * eldest entry must be removed from the map or not based on the size of the map itself.
   */
  override fun removeEldestEntry(eldest: MutableMap.MutableEntry<K, V>?): Boolean {
    return size > capacity
  }
}
