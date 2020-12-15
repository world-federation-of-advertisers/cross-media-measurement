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
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.ConsumedFlowItem
import org.wfanet.measurement.common.consumeFirst
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.duchyIdentityFromContext
import org.wfanet.measurement.internal.duchy.AdvanceComputationRequest as AsyncAdvanceComputationRequest
import org.wfanet.measurement.internal.duchy.AsyncComputationControlGrpcKt.AsyncComputationControlCoroutineStub
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequest
import org.wfanet.measurement.system.v1alpha.AdvanceComputationResponse
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineImplBase

class ComputationControlService(
  private val asyncComputationControlClient: AsyncComputationControlCoroutineStub,
  private val storageClient: StorageClient,
  private val duchyIdentityProvider: () -> DuchyIdentity = ::duchyIdentityFromContext
) : ComputationControlCoroutineImplBase() {

  override suspend fun advanceComputation(
    requests: Flow<AdvanceComputationRequest>
  ): AdvanceComputationResponse {
    grpcRequireNotNull(requests.consumeFirst()) { "Empty request stream" }
      .use { consumed: ConsumedFlowItem<AdvanceComputationRequest> ->
        val header = consumed.item.header
        val id = header.key.globalComputationId
        logger.info("[id=$id]: Received request with header $header")
        grpcRequire(id.isNotEmpty()) { "Cannot advance computation, missing computation ID" }
        grpcRequire(consumed.hasRemaining) { "Request stream has no body" }
        if (header.isForAsyncComputation()) {
          handleAsyncRequest(header, consumed.remaining.map { it.bodyChunk.partialData }, id)
        } else {
          failGrpc { "Synchronous computations are not yet supported." }
        }
      }
    return AdvanceComputationResponse.getDefaultInstance()
  }

  /** Write the payload data stream as a blob, and advance the stage via the async service. */
  private suspend fun handleAsyncRequest(
    header: AdvanceComputationRequest.Header,
    blob: Flow<ByteString>,
    globalId: String
  ) {
    val sender = duchyIdentityProvider().id
    val blobKey = generateBlobKey(header, sender)
    if (storageClient.getBlob(blobKey) == null) {
      storageClient.createBlob(blobKey, blob)
    }
    val advanceAsyncComputationRequest =
      AsyncAdvanceComputationRequest.newBuilder().apply {
        globalComputationId = globalId
        computationStage = header.stageExpectingInput()
        blobPath = blobKey
        dataOrigin = duchyIdentityProvider().id
      }.build()
    asyncComputationControlClient.advanceComputation(advanceAsyncComputationRequest)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    fun generateBlobKey(
      header: AdvanceComputationRequest.Header,
      sender: String
    ): String = with(header) {
      return listOf(
        "/computations",
        key.globalComputationId,
        protocolCase.name,
        when (protocolCase) {
          AdvanceComputationRequest.Header.ProtocolCase.LIQUID_LEGIONS_V1 ->
            liquidLegionsV1.description.name
          AdvanceComputationRequest.Header.ProtocolCase.LIQUID_LEGIONS_V2 ->
            liquidLegionsV2.description.name
          else -> failGrpc { "Unknown protocol $protocolCase" }
        },
        sender
      ).joinToString("/")
    }
  }
}
