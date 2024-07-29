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

package org.wfanet.measurement.common.k8s

import kotlin.random.Random
import kotlin.random.nextULong

/** Utilities for Kubernetes object names. */
internal object Names {
  /** Set of characters for name generation. */
  private const val NAME_SUFFIX_ALPHABET = "bcdfghjklmnpqrstvwxz2456789"
  /** Length of the name suffix. */
  private const val NAME_SUFFIX_LENGTH = 5
  /** Number of bits needed to index into [NAME_SUFFIX_ALPHABET] */
  private const val NAME_SUFFIX_INDEX_BITS = 5
  /** Bit mask for getting index from the rightmost bits of a [ULong]. */
  private val NAME_SUFFIX_INDEX_MASK = ULong.MAX_VALUE.shl(NAME_SUFFIX_INDEX_BITS).inv()

  /**
   * Generates a random suffix for a Kubernetes object name.
   *
   * This is similar to the Kubernetes `generateName` functionality. See
   * https://github.com/kubernetes/apimachinery/blob/master/pkg/util/rand/rand.go
   */
  fun generateNameSuffix(random: Random): String {
    var suffix = ""
    var source = random.nextULong()
    while (suffix.length < NAME_SUFFIX_LENGTH) {
      val index = source.and(NAME_SUFFIX_INDEX_MASK).toInt().mod(NAME_SUFFIX_ALPHABET.length)
      suffix += NAME_SUFFIX_ALPHABET[index]
      source = source.shr(NAME_SUFFIX_INDEX_BITS)
    }
    return suffix
  }
}
