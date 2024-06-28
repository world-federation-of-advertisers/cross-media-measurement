// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.common

import com.google.common.hash.Hashing
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.common.Fingerprinters.farmHashFingerprint64
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.exchangeIdentifiers
import org.wfanet.panelmatch.client.internal.exchangeWorkflow

@RunWith(JUnit4::class)
class FingerprintersTest {

  @Test
  fun farmHashFingerprint64BytesReturnsCorrectHashForRawBytes() {
    val payload = "the quick brown fox".toByteStringUtf8()

    val fingerprint = farmHashFingerprint64(payload)

    assertThat(fingerprint)
      .isEqualTo(
        Hashing.farmHashFingerprint64().hashBytes(payload.toByteArray()).asBytes().toByteString()
      )
  }

  @Test
  fun farmHashFingerprint64BytesReturnsCorrectHashForProtobufMessage() {
    val message = exchangeWorkflow {
      exchangeIdentifiers = exchangeIdentifiers {
        dataProviderId = "some-data-provider"
        modelProviderId = "some-model-provider"
      }
    }

    val fingerprint = message.farmHashFingerprint64()

    assertThat(fingerprint)
      .isEqualTo(
        Hashing.farmHashFingerprint64().hashBytes(message.toByteArray()).asBytes().toByteString()
      )
  }
}
