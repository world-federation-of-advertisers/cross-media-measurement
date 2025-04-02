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

import kotlin.random.Random

fun interface IdGenerator {
  /** Returns a non-zero ID. */
  fun generateId(): Long

  companion object {
    val Default: IdGenerator = RandomIdGenerator()
  }
}

class RandomIdGenerator(private val random: Random = Random.Default) : IdGenerator {
  override fun generateId(): Long {
    var nextId = 0L
    while (nextId == 0L) {
      nextId = random.nextLong()
    }
    return nextId
  }
}

inline fun IdGenerator.generateNewId(idExists: (id: Long) -> Boolean): Long {
  var id = generateId()
  while (idExists(id)) {
    id = generateId()
  }
  return id
}
