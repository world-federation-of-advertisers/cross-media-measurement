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

package org.wfanet.panelmatch.client.exchangetasks

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.client.PreprocessEventsRequestKt.unprocessedEvent
import org.wfanet.panelmatch.client.common.joinKeyAndIdOf
import org.wfanet.panelmatch.client.eventpreprocessing.JniEventPreprocessor
import org.wfanet.panelmatch.client.preprocessEventsRequest
import org.wfanet.panelmatch.common.compression.CompressionParametersKt.noCompression
import org.wfanet.panelmatch.common.compression.compressionParameters
import org.wfanet.panelmatch.common.crypto.JniDeterministicCommutativeCipher
import org.wfanet.panelmatch.common.storage.createBlob
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class GenerateLookupKeysTaskTest {

  @Test
  fun alignsWithPreprocessor() = runBlockingTest {
    val deterministicCommutativeCipher = JniDeterministicCommutativeCipher()
    val key = JniDeterministicCommutativeCipher().generateKey()

    val identifierHashPepper = "some-pepper".toByteStringUtf8()

    val unencryptedId = "unencrypted-id".toByteStringUtf8()
    val encryptedId = deterministicCommutativeCipher.encrypt(key, listOf(unencryptedId)).single()

    val joinKeyIdentifier = "id-1".toByteStringUtf8()
    val joinKeys = joinKeyAndIdCollection {
      joinKeysAndIds += joinKeyAndIdOf(encryptedId, joinKeyIdentifier)
    }
    val storage = InMemoryStorageClient()
    val taskOutputs =
      GenerateLookupKeysTask()
        .execute(
          mapOf(
            "pepper" to storage.createBlob("pepper", identifierHashPepper),
            "join-keys" to storage.createBlob("join-keys", joinKeys.toByteString())
          )
        )
    assertThat(taskOutputs.keys).containsExactly("lookup-keys")
    val lookupKeys = JoinKeyAndIdCollection.parseFrom(taskOutputs.getValue("lookup-keys").flatten())
    assertThat(lookupKeys.joinKeysAndIdsList).hasSize(1)
    val lookupKeyAndId = lookupKeys.joinKeysAndIdsList.single()
    assertThat(lookupKeyAndId.joinKeyIdentifier.id).isEqualTo(joinKeyIdentifier)
    val lookupKey = lookupKeyAndId.joinKey.key

    val preprocessEventsRequest = preprocessEventsRequest {
      cryptoKey = key
      hkdfPepper = "some-hkdf-pepper".toByteStringUtf8()
      this.identifierHashPepper = identifierHashPepper
      compressionParameters = compressionParameters { uncompressed = noCompression {} }

      unprocessedEvents +=
        unprocessedEvent {
          id = unencryptedId
          data = "irrelevant-for-this-test".toByteStringUtf8()
        }
    }
    val preprocessEventsResponse = JniEventPreprocessor().preprocess(preprocessEventsRequest)
    val preprocessedLookupKey = preprocessEventsResponse.processedEventsList.single().encryptedId
    val buffer =
      ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(preprocessedLookupKey)
    val preprocessedLookupKeyBytes = buffer.array().toByteString()

    assertThat(lookupKey).isEqualTo(preprocessedLookupKeyBytes)
  }
}
