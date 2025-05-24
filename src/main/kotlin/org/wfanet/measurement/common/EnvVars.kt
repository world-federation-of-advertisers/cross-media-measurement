/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common

import java.nio.file.InvalidPathException
import java.nio.file.Paths

/*
 * Util functions for EDP Aggregator Cloud Functions
 */
object EnvVars {
  fun checkNotNullOrEmpty(envVar: String): String {
    val value = System.getenv(envVar)
    checkNotNull(value) { "Missing env var: $envVar" }
    check(value.isNotBlank())
    return value
  }

  fun checkIsPath(envVar: String): String {
    val value = System.getenv(envVar)
    try {
      Paths.get(value)
    } catch (e: InvalidPathException) {
      throw IllegalStateException("Path $value is not a valid path", e)
    }
    return value
  }
}
