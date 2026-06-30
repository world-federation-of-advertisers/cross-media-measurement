/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.vidrankbuilder

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class EventIdDigestBytesTest {
  @Test
  fun `read round-trips the high and low halves written`() {
    val buffer = ByteArray(EventIdDigestBytes.WIDTH)
    EventIdDigestBytes.writeHi(buffer, 0, 0x0102030405060708L)
    EventIdDigestBytes.writeLo(buffer, 8, 0x090A0B0C)
    val bytes = ByteString.copyFrom(buffer)

    assertThat(EventIdDigestBytes.readHi(bytes, 0)).isEqualTo(0x0102030405060708L)
    assertThat(EventIdDigestBytes.readLo(bytes, 8)).isEqualTo(0x090A0B0C)
  }

  @Test
  fun `read decodes big-endian byte order`() {
    // High = bytes 0..7, low = bytes 8..11, most-significant byte first.
    val bytes = ByteString.copyFrom(byteArrayOf(0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 9))

    assertThat(EventIdDigestBytes.readHi(bytes, 0)).isEqualTo(5L)
    assertThat(EventIdDigestBytes.readLo(bytes, 8)).isEqualTo(9)
  }

  @Test
  fun `handles the maximum unsigned byte pattern without sign extension`() {
    val buffer = ByteArray(EventIdDigestBytes.WIDTH)
    EventIdDigestBytes.writeHi(buffer, 0, -1L) // all 0xFF
    EventIdDigestBytes.writeLo(buffer, 8, -1) // all 0xFF
    val bytes = ByteString.copyFrom(buffer)

    assertThat(EventIdDigestBytes.readHi(bytes, 0)).isEqualTo(-1L)
    assertThat(EventIdDigestBytes.readLo(bytes, 8)).isEqualTo(-1)
  }
}
