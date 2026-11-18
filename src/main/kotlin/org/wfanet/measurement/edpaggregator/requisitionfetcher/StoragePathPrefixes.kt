// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.requisitionfetcher

/** Utilities for validating storage-object path prefixes. */
object StoragePathPrefixes {
  data class Namespace(val storageUriPrefix: String, val pathPrefix: String, val owner: String)

  /** Whether either prefix contains the other at a path-segment boundary. */
  fun overlap(first: String, second: String): Boolean {
    fun normalize(value: String): String =
      value.split('/').filter(String::isNotEmpty).joinToString("/")

    val normalizedFirst = normalize(first)
    val normalizedSecond = normalize(second)
    if (normalizedFirst.isEmpty() || normalizedSecond.isEmpty()) {
      return true
    }
    return normalizedFirst == normalizedSecond ||
      normalizedFirst.startsWith("$normalizedSecond/") ||
      normalizedSecond.startsWith("$normalizedFirst/")
  }

  /** Requires every path prefix in the same storage namespace to be disjoint. */
  fun requireDisjoint(namespaces: List<Namespace>) {
    for ((index, first) in namespaces.withIndex()) {
      for (second in namespaces.drop(index + 1)) {
        if (first.storageUriPrefix != second.storageUriPrefix) continue
        require(!overlap(first.pathPrefix, second.pathPrefix)) {
          "Storage path prefixes for ${first.owner} and ${second.owner} overlap in " +
            "${first.storageUriPrefix}: '${first.pathPrefix}' and '${second.pathPrefix}'"
        }
      }
    }
  }
}
