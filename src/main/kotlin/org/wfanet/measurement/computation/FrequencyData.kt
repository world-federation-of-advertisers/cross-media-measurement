// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.computation

/**
 * A read-only view of per-VID frequencies. The backing array must remain unchanged while in use.
 */
interface FrequencyData {
  val size: Int

  operator fun get(index: Int): Int

  companion object {
    /** Views bytes as unsigned frequencies without copying. */
    fun from(data: ByteArray): FrequencyData =
      object : FrequencyData {
        override val size: Int
          get() = data.size

        override fun get(index: Int): Int = data[index].toInt() and 0xFF
      }

    /** Views integer frequencies without copying. */
    fun from(data: IntArray): FrequencyData =
      object : FrequencyData {
        override val size: Int
          get() = data.size

        override fun get(index: Int): Int = data[index]
      }
  }
}
