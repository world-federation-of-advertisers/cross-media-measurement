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

package org.wfanet.measurement.edpaggregator.rawimpressions

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.nio.ByteBuffer
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class EventIdDigestExtractorTest {
  private val extractor = EventIdDigestExtractor()

  @Test
  fun `extract matches pinned SHA-256 golden vectors`() {
    // Pinned 12-byte truncation of SHA-256(input), split big-endian into
    // high (bytes 0..7) + low (bytes 8..11). These vectors freeze the digest
    // algorithm, truncation, and byte ordering: the value is the key of the
    // subpool-assigner and rank-index maps (the latter persisted in GCS across
    // dispatches), and Phase 1 (writer) and Phase 2 (lookup) must agree on it.
    // It is independent of the C++ labeler's acting_fingerprint (FarmHash64).
    assertThat(extractor.extract(utf8("")))
      .isEqualTo(EventIdDigest(high = -2039914840885289964L, low = -1694763832))
    assertThat(extractor.extract(utf8("event-123")))
      .isEqualTo(EventIdDigest(high = -5833074307448107152L, low = -190123180))
    assertThat(extractor.extract(utf8("abc")))
      .isEqualTo(EventIdDigest(high = -5010229573455851542L, low = 1094795486))
  }

  @Test
  fun `ByteString and ByteBuffer overloads agree`() {
    val bytes = "event-123".toByteArray(Charsets.UTF_8)
    assertThat(extractor.extract(ByteBuffer.wrap(bytes)))
      .isEqualTo(extractor.extract(ByteString.copyFrom(bytes)))
  }

  private fun utf8(value: String): ByteString = ByteString.copyFromUtf8(value)
}
