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

package org.wfanet.measurement.common

import java.io.File
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import org.jetbrains.annotations.Blocking

/** An object which exposes whether it is healthy. */
interface Health {
  /** Whether this instance is healthy. */
  val healthy: Boolean

  /** Suspends while [healthy] is `false`. */
  suspend fun waitUntilHealthy()
}

/** [Health] with the ability to set the value of [healthy]. */
open class SettableHealth(healthy: Boolean = false) : Health {
  private val healthState = MutableStateFlow(healthy)

  override val healthy: Boolean
    get() = healthState.value

  /**
   * Sets the value of [healthy] to [value].
   *
   * The base implementation does not block, but implementations may override this to be blocking.
   */
  @Blocking
  open fun setHealthy(value: Boolean) {
    healthState.value = value
  }

  override suspend fun waitUntilHealthy() {
    healthState.first { it }
  }
}

/**
 * [Health] where [file] is used to track whether [healthy] is `true`.
 *
 * This assumes that the existence of [file] never changes while this object exists other than by
 * [setHealthy].
 */
class FileExistsHealth @Blocking constructor(private val file: File) :
  SettableHealth(file.exists()) {
  override fun setHealthy(value: Boolean) {
    if (value) {
      file.createNewFile()
    } else {
      file.delete()
    }
    super.setHealthy(value)
  }
}
