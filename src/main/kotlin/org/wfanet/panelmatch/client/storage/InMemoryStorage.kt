// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.storage

import com.google.protobuf.ByteString
import java.util.concurrent.ConcurrentHashMap

/**
 * Stores everything in memory. Nothing is persistent. Use with caution. Uses a simple hashmap to
 * storage everything where path is the key.
 */
class InMemoryStorage(private val keyPrefix: String) : Storage {
  private var inMemoryStorage = ConcurrentHashMap<String, ByteString>()

  private fun getKey(keyPrefix: String, path: String): String {
    return "$keyPrefix$path"
  }

  override suspend fun read(path: String): ByteString {
    val key = getKey(keyPrefix = keyPrefix, path = path)
    return requireNotNull(inMemoryStorage[key]) { "Key does not exist in storage: $key" }
  }

  override suspend fun write(path: String, data: ByteString) {
    val key = getKey(keyPrefix = keyPrefix, path = path)
    require(inMemoryStorage.putIfAbsent(key, data) == null) {
      "Cannot write to an existing key: $key"
    }
  }
}
