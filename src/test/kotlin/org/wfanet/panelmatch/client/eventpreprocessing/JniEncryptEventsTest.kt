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

import com.google.protobuf.kotlin.toByteStringUtf8
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.common.compression.CompressionParametersKt.noCompression
import org.wfanet.panelmatch.common.compression.compressionParameters

@RunWith(JUnit4::class)
class JniEncryptEventsTest {

  @Test
  fun test() {
    val request = preprocessEventsRequest {
      cryptoKey = "crypto-key".toByteStringUtf8()
      identifierHashPepper = "identifier-hash-pepper".toByteStringUtf8()
      hkdfPepper = "hkdf-pepper".toByteStringUtf8()
      compressionParameters = compressionParameters { uncompressed = noCompression {} }
      unprocessedEvents += unprocessedEvent {
        id = "identifier".toByteStringUtf8()
        data = "event-data".toByteStringUtf8()
      }
    }
    // TODO(@efoxepstein): once we have a JNIed way to decrypt, this should check roundtrips.
    JniEventPreprocessor().preprocess(request)
  }
}
