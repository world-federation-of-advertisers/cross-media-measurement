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

package org.wfanet.measurement.duchy.service.system.v1alpha

import com.google.protobuf.ByteString
import com.google.protobuf.Message
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onStart
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.system.v1alpha.ComputationProcessRequestHeader
import org.wfanet.measurement.system.v1alpha.ProcessConcatenatedSketchRequest
import org.wfanet.measurement.system.v1alpha.ProcessEncryptedFlagsAndCountsRequest
import org.wfanet.measurement.system.v1alpha.ProcessNoisedSketchRequest

class ComputationControlRequests(
  private val requestChunkSizeBytes: Int = 1024 * 32 // 32 KiB
) {
  data class Filler<B : Message.Builder>(
    val newBuilder: () -> B,
    private val headerBuilder: B.() -> ComputationProcessRequestHeader.Builder,
    val fillBodyChunk: B.(ByteString) -> Unit
  ) {
    fun B.fillHeader(globalComputationId: String) {
      headerBuilder().keyBuilder.globalComputationId = globalComputationId
    }
  }

  fun buildNoisedSketchRequests(
    globalComputationId: String,
    content: Flow<ByteString>
  ): Flow<ProcessNoisedSketchRequest> {
    return noisedSketchFiller.mapSendRequests(globalComputationId, content).map { it.build() }
  }

  fun buildConcatenatedSketchRequests(
    globalComputationId: String,
    content: Flow<ByteString>
  ): Flow<ProcessConcatenatedSketchRequest> {
    return concatenatedSketchFiller.mapSendRequests(globalComputationId, content).map { it.build() }
  }

  fun buildEncryptedFlagsAndCountsRequests(
    globalComputationId: String,
    content: Flow<ByteString>
  ): Flow<ProcessEncryptedFlagsAndCountsRequest> {
    return encryptedFlagsAndCountsFiller.mapSendRequests(globalComputationId, content)
      .map { it.build() }
  }

  /**
   * Maps [globalComputationId] and [content] to a [Flow] that produces request
   * [Message.Builder]s.
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `onStart`.
  private fun <B : Message.Builder> Filler<B>.mapSendRequests(
    globalComputationId: String,
    content: Flow<ByteString>
  ): Flow<B> {
    // Resize flow items for sending to another Duchy.
    val bodyContent = content.asBufferedFlow(requestChunkSizeBytes)

    val headerRequest: B = newBuilder().apply { fillHeader(globalComputationId) }
    val bodyRequests: Flow<B> = bodyContent.map {
      newBuilder().apply { fillBodyChunk(it) }
    }

    return bodyRequests.onStart { emit(headerRequest) }
  }

  companion object {
    val noisedSketchFiller = Filler(
      ProcessNoisedSketchRequest::newBuilder,
      { headerBuilder },
      { bodyChunkBuilder.partialSketch = it }
    )

    val concatenatedSketchFiller = Filler(
      ProcessConcatenatedSketchRequest::newBuilder,
      { headerBuilder },
      { bodyChunkBuilder.partialSketch = it }
    )

    val encryptedFlagsAndCountsFiller = Filler(
      ProcessEncryptedFlagsAndCountsRequest::newBuilder,
      { headerBuilder },
      { bodyChunkBuilder.partialFlagsAndCounts = it }
    )
  }
}
