// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.config.DuchyPublicKeyConfig
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequest

@RunWith(JUnit4::class)
class ProtoUtilsTest {
  @Test
  fun `truncateByteFields truncates if longer than threshold`() {
    val message = AdvanceComputationRequest.newBuilder().apply {
      bodyChunkBuilder.partialData = ByteString.copyFromUtf8("1234567890")
    }.build()

    val result = message.truncateByteFields(4)

    assertThat(result.bodyChunk.partialData.toStringUtf8()).isEqualTo("1234")
  }

  @Test
  fun `truncateByteFields does not truncate if not longer than threshold`() {
    val message = AdvanceComputationRequest.newBuilder().apply {
      bodyChunkBuilder.partialData = ByteString.copyFromUtf8("123456")
    }.build()

    val result = message.truncateByteFields(10)

    assertThat(result).isEqualTo(message)
  }

  @Test
  fun `truncateByteFields truncates in embedded proto field`() {
    val message = AdvanceComputationRequest.newBuilder().apply {
      bodyChunkBuilder.partialData = ByteString.copyFromUtf8("1234567890")
    }.build()

    val result = message.truncateByteFields(4)

    assertThat(result.bodyChunk.partialData.toStringUtf8()).isEqualTo("1234")
  }

  @Test
  fun `truncateByteFields truncates in map field`() {
    val originalBytes = ByteString.copyFromUtf8("1234567890")
    val combinedPublicKeyId = "combined-public-key-1"
    val message = DuchyPublicKeyConfig.newBuilder().apply {
      putEntries(
        combinedPublicKeyId,
        DuchyPublicKeyConfig.Entry.newBuilder().apply {
          putElGamalElements("duchy-1", originalBytes)
          putElGamalElements("duchy-2", originalBytes)
        }.build()
      )
    }

    val results = message.truncateByteFields(5)

    val expectedBytes = ByteString.copyFromUtf8("12345")
    val entry = checkNotNull(results.entriesMap[combinedPublicKeyId])
    assertThat(entry.elGamalElementsMap.values).containsExactly(expectedBytes, expectedBytes)
  }
}
