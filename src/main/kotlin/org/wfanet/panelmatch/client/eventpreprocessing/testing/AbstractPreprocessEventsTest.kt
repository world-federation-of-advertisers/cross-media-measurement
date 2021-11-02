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

package org.wfanet.panelmatch.client.eventpreprocessing.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import org.junit.Test
import org.wfanet.panelmatch.client.PreprocessEventsRequestKt.unprocessedEvent
import org.wfanet.panelmatch.client.eventpreprocessing.PreprocessEvents
import org.wfanet.panelmatch.client.preprocessEventsRequest

/** Abstract base class for testing implementations of [PreprocessEvents]. */
abstract class AbstractPreprocessEventsTest {
  abstract val preprocessEvents: PreprocessEvents

  @Test
  fun testPreprocessEvents() {
    val arbitraryId: ByteString = "arbitrary-id".toByteStringUtf8()
    val arbitraryData: ByteString = "arbitrary-data".toByteStringUtf8()
    val arbitraryCryptoKey: ByteString = "arbitrary-crypto-key".toByteStringUtf8()
    val arbitraryHkdfPepper: ByteString = "arbitrary-hkdf-pepper".toByteStringUtf8()
    val arbitraryIdentifierHashPepper: ByteString =
      "arbitrary-identifier-hash-pepper".toByteStringUtf8()

    val request = preprocessEventsRequest {
      cryptoKey = arbitraryCryptoKey
      identifierHashPepper = arbitraryIdentifierHashPepper
      hkdfPepper = arbitraryHkdfPepper
      unprocessedEvents +=
        unprocessedEvent {
          id = arbitraryId
          data = arbitraryData
        }
    }
    assertThat(preprocessEvents.preprocess(request)).isNotNull()
  }
}
