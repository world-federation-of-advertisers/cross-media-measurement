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

/** Utilities for validating GCS object-key prefixes. */
object StoragePathPrefixes {
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
}
