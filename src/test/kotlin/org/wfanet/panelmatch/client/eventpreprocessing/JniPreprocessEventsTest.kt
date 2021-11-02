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
import org.wfanet.panelmatch.client.PreprocessEventsRequestKt.unprocessedEvent
import org.wfanet.panelmatch.client.eventpreprocessing.testing.AbstractPreprocessEventsTest
import org.wfanet.panelmatch.client.preprocessEventsRequest
import org.wfanet.panelmatch.common.JniException

@RunWith(JUnit4::class)
class JniPreprocessEventsTest : AbstractPreprocessEventsTest() {
  override val preprocessEvents: PreprocessEvents = JniPreprocessEvents()

  @Test
  fun testJniWrapExceptionIdentifierHashPepper() {
    val request = preprocessEventsRequest {
      cryptoKey = "arbitrary-cryptokey".toByteStringUtf8()
      hkdfPepper = "arbitrary-hkdf-pepper".toByteStringUtf8()
      unprocessedEvents +=
        unprocessedEvent {
          id = "arbitrary-id".toByteStringUtf8()
          data = "arbitrary-data".toByteStringUtf8()
        }
    }
    val noPepper = assertFailsWith(JniException::class) { preprocessEvents.preprocess(request) }
    assertThat(noPepper.message).contains("Empty Identifier Hash Pepper")
  }

  @Test
  fun testJniWrapExceptionHkdfPepper() {
    val request = preprocessEventsRequest {
      cryptoKey = "arbitrary-cryptokey".toByteStringUtf8()
      identifierHashPepper = "arbitrary-identifier-hash-pepper".toByteStringUtf8()
      unprocessedEvents +=
        unprocessedEvent {
          id = "arbitrary-id".toByteStringUtf8()
          data = "arbitrary-data".toByteStringUtf8()
        }
    }
    val noPepper = assertFailsWith<JniException> { preprocessEvents.preprocess(request) }
    assertThat(noPepper.message).contains("Empty HKDF Pepper")
  }

  @Test
  fun testJniWrapExceptionCryptoKey() {
    assertFailsWith(JniException::class) {
      val request = preprocessEventsRequest {
        identifierHashPepper = "arbitrary-identifier-hash-pepper".toByteStringUtf8()
        hkdfPepper = "arbitrary-hkdf-pepper".toByteStringUtf8()
        unprocessedEvents +=
          unprocessedEvent {
            id = "arbitrary-id".toByteStringUtf8()
            data = "arbitrary-data".toByteStringUtf8()
          }
      }
      preprocessEvents.preprocess(request)
    }
  }
}
