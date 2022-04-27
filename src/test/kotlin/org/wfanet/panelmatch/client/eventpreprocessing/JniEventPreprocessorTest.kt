// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.eventpreprocessing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.eventpreprocessing.testing.AbstractEventPreprocessorTest
import org.wfanet.panelmatch.common.JniException
import org.wfanet.panelmatch.common.compression.CompressionParametersKt.noCompression
import org.wfanet.panelmatch.common.compression.compressionParameters

private val REQUEST = preprocessEventsRequest {
  cryptoKey = "some-crypto-key".toByteStringUtf8()
  hkdfPepper = "some-hkdf-pepper".toByteStringUtf8()
  identifierHashPepper = "some-identifier-hash-pepper".toByteStringUtf8()
  compressionParameters = compressionParameters { uncompressed = noCompression {} }
  unprocessedEvents += unprocessedEvent {
    id = "some-id".toByteStringUtf8()
    data = "some-data".toByteStringUtf8()
  }
}

@RunWith(JUnit4::class)
class JniEventPreprocessorTest : AbstractEventPreprocessorTest() {
  override val eventPreprocessor: EventPreprocessor = JniEventPreprocessor()

  @Test
  fun missingIdentifierHashPepper() {
    val request = REQUEST.copy { clearIdentifierHashPepper() }
    val e = assertFailsWith(JniException::class) { eventPreprocessor.preprocess(request) }
    assertThat(e).hasMessageThat().contains("Empty Identifier Hash Pepper")
  }

  @Test
  fun missingHkdfPepper() {
    val request = REQUEST.copy { clearHkdfPepper() }
    val e = assertFailsWith<JniException> { eventPreprocessor.preprocess(request) }
    assertThat(e).hasMessageThat().contains("Empty HKDF Pepper")
  }

  @Test
  fun missingCryptoKey() {
    val request = REQUEST.copy { clearCryptoKey() }
    assertFailsWith<JniException> { eventPreprocessor.preprocess(request) }
  }

  @Test
  fun missingCompressionParameters() {
    val request = REQUEST.copy { clearCompressionParameters() }
    assertFailsWith<JniException> { eventPreprocessor.preprocess(request) }
  }
}
