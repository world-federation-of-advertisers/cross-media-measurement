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

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.PreprocessEventsRequestKt.unprocessedEvent
import org.wfanet.panelmatch.client.preprocessEventsRequest
import org.wfanet.panelmatch.common.toByteString

@RunWith(JUnit4::class)
class EncryptEventsTest {

  @Test
  fun test() {
    val request = preprocessEventsRequest {
      cryptoKey = "crypto-key".toByteString()
      identifierHashPepper = "identifier-hash-pepper".toByteString()
      hkdfPepper = "hkdf-pepper".toByteString()
      unprocessedEvents +=
        unprocessedEvent {
          id = "identifier".toByteString()
          data = "event-data".toByteString()
        }
    }
    val encryptEvents = EncryptEvents()

    // TODO(@efoxepstein): once we have a JNIed way to decrypt, this should check roundtrips.
    encryptEvents.apply(request) // Does not throw
  }
}
