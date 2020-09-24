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

package org.wfanet.measurement.duchy.testing

import com.google.protobuf.ByteString
import com.google.protobuf.Message
import org.wfanet.measurement.duchy.ComputationControlRequests
import org.wfanet.measurement.internal.duchy.HandleConcatenatedSketchRequest
import org.wfanet.measurement.internal.duchy.HandleEncryptedFlagsAndCountsRequest
import org.wfanet.measurement.internal.duchy.HandleNoisedSketchRequest

fun buildNoisedSketchRequests(
  globalComputationId: String,
  vararg chunkContents: String
): Sequence<HandleNoisedSketchRequest> {
  return ComputationControlRequests.noisedSketchFiller.mapSendRequests(
    globalComputationId,
    chunkContents.asSequence()
  ).map { it.build() }
}

fun buildConcatenatedSketchRequests(
  globalComputationId: String,
  vararg chunkContents: String
): Sequence<HandleConcatenatedSketchRequest> {
  return ComputationControlRequests.concatenatedSketchFiller.mapSendRequests(
    globalComputationId,
    chunkContents.asSequence()
  ).map { it.build() }
}

fun buildEncryptedFlagsAndCountsRequests(
  globalComputationId: String,
  vararg chunkContents: String
): Sequence<HandleEncryptedFlagsAndCountsRequest> {
  return ComputationControlRequests.encryptedFlagsAndCountsFiller.mapSendRequests(
    globalComputationId,
    chunkContents.asSequence()
  ).map { it.build() }
}

/**
 * Maps [globalComputationId] and [content] to a [Sequence] of send request
 * [Message.Builder]s.
 */
private fun <B : Message.Builder> ComputationControlRequests.Filler<B>.mapSendRequests(
  globalComputationId: String,
  content: Sequence<String>
): Sequence<B> {
  val headerRequest: B = newBuilder().apply { fillHeader(globalComputationId) }
  val bodyRequests = content.map {
    newBuilder().apply { fillBodyChunk(ByteString.copyFromUtf8(it)) }
  }
  return sequenceOf(headerRequest).plus(bodyRequests)
}
