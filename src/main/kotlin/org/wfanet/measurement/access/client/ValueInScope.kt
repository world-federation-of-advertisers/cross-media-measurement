/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.access.client

import java.util.function.Predicate

/**
 * Predicate for testing whether a given value is in [scopes].
 *
 * Scopes are strings delimited by [DELIMITER] where the final segment may be [WILDCARD]. A value is
 * considered to match a scope if the segments before [WILDCARD] match. For example, the value
 * "foo.bar" is in scopes "foo.bar", "foo.*", and "*".
 */
class ValueInScope(private val scopes: Set<String>) : Predicate<String> {
  constructor(vararg scopes: String) : this(scopes.toSet())

  override fun test(value: String): Boolean {
    if (value in scopes) {
      return true
    }

    // Optimization.
    if (WILDCARD in scopes) {
      return true
    }

    val parts = value.split(DELIMITER).toMutableList()
    do {
      parts[parts.size - 1] = "*"
      if (parts.joinToString(DELIMITER) in scopes) {
        return true
      }
      parts.removeLast()
    } while (parts.isNotEmpty())

    return false
  }

  companion object {
    const val WILDCARD = "*"
    const val DELIMITER = "."
  }
}
