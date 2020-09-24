// Copyright 2020 The Measurement System Authors
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
import com.google.protobuf.ByteString
import kotlin.test.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.config.DuchyPublicKeyConfig
import org.wfanet.measurement.internal.duchy.AddNoiseToSketchRequest
import org.wfanet.measurement.internal.duchy.HandleConcatenatedSketchRequest

@RunWith(JUnit4::class)
class ProtoUtilsTest {

  @Test
  fun `truncated if longer than threshold`() {
    assertEquals(
      HandleConcatenatedSketchRequest.newBuilder()
        .setComputationId("id")
        .setPartialSketch(ByteString.copyFromUtf8("1234"))
        .build(),
      HandleConcatenatedSketchRequest.newBuilder()
        .setComputationId("id")
        .setPartialSketch(ByteString.copyFromUtf8("1234567890"))
        .build()
        .truncateByteFields(4)
    )
  }

  @Test
  fun `not truncated if no longer than threshold`() {
    assertEquals(
      HandleConcatenatedSketchRequest.newBuilder()
        .setComputationId("id")
        .setPartialSketch(ByteString.copyFromUtf8("123456"))
        .build(),
      HandleConcatenatedSketchRequest.newBuilder()
        .setComputationId("id")
        .setPartialSketch(ByteString.copyFromUtf8("123456"))
        .build()
        .truncateByteFields(10)
    )
  }

  @Test
  fun `truncate bytes in embedded proto field`() {
    assertEquals(
      AddNoiseToSketchRequest.newBuilder().apply {
        compositeElGamalKeysBuilder.apply {
          elGamalG = ByteString.copyFromUtf8("123456")
        }
      }.build(),
      AddNoiseToSketchRequest.newBuilder().apply {
        compositeElGamalKeysBuilder.apply {
          elGamalG = ByteString.copyFromUtf8("1234567890")
        }
      }.build()
        .truncateByteFields(6)
    )
  }

  @Test
  fun `truncate bytes in map field`() {
    val originalBytes = ByteString.copyFromUtf8("1234567890")
    val combinedPublicKeyId = "combined-public-key-1"
    val message = DuchyPublicKeyConfig.newBuilder().apply {
      putEntries(combinedPublicKeyId, DuchyPublicKeyConfig.Entry.newBuilder().apply {
        putElGamalElements("duchy-1", originalBytes)
        putElGamalElements("duchy-2", originalBytes)
      }.build())
    }

    val results = message.truncateByteFields(5)

    val expectedBytes = ByteString.copyFromUtf8("12345")
    val entry = checkNotNull(results.entriesMap[combinedPublicKeyId])
    assertThat(entry.elGamalElementsMap.values).containsExactly(expectedBytes, expectedBytes)
  }
}
