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

package org.wfanet.measurement.duchy.service.system.v1alpha

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.ConsumedFlowItem
import org.wfanet.measurement.common.consumeFirst
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.duchyIdentityFromContext
import org.wfanet.measurement.duchy.storage.ComputationBlobContext
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.internal.duchy.AsyncComputationControlGrpcKt.AsyncComputationControlCoroutineStub
import org.wfanet.measurement.internal.duchy.advanceComputationRequest
import org.wfanet.measurement.internal.duchy.getOutputBlobMetadataRequest
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequest
import org.wfanet.measurement.system.v1alpha.AdvanceComputationResponse
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationKey

class ComputationControlService(
  private val asyncComputationControlClient: AsyncComputationControlCoroutineStub,
  private val computationStore: ComputationStore,
  private val duchyIdentityProvider: () -> DuchyIdentity = ::duchyIdentityFromContext
) : ComputationControlCoroutineImplBase() {

  constructor(
    asyncComputationControlClient: AsyncComputationControlCoroutineStub,
    storageClient: StorageClient,
    duchyIdentityProvider: () -> DuchyIdentity = ::duchyIdentityFromContext
  ) : this(asyncComputationControlClient, ComputationStore(storageClient), duchyIdentityProvider)

  override suspend fun advanceComputation(
    requests: Flow<AdvanceComputationRequest>
  ): AdvanceComputationResponse {
    grpcRequireNotNull(requests.consumeFirst()) { "Empty request stream" }
      .use { consumed: ConsumedFlowItem<AdvanceComputationRequest> ->
        val header = consumed.item.header
        val globalComputationId =
          grpcRequireNotNull(ComputationKey.fromName(header.name)?.computationId) {
            "Resource name unspecified or invalid."
          }
        grpcRequire(consumed.hasRemaining) { "Request stream has no body" }
        if (header.isForAsyncComputation()) {
          handleAsyncRequest(
            header,
            consumed.remaining.map { it.bodyChunk.partialData },
            globalComputationId
          )
        } else {
          failGrpc { "Synchronous computations are not yet supported." }
        }
      }
    return AdvanceComputationResponse.getDefaultInstance()
  }

  /** Write the payload data stream as a blob, and advance the stage via the async service. */
  private suspend fun handleAsyncRequest(
    header: AdvanceComputationRequest.Header,
    content: Flow<ByteString>,
    globalId: String
  ) {
    val blobMetadata =
      asyncComputationControlClient.getOutputBlobMetadata(
        getOutputBlobMetadataRequest {
          globalComputationId = globalId
          dataOrigin = duchyIdentityProvider().id
        }
      )

    val stage = header.stageExpectingInput()
    val blob =
      computationStore.write(ComputationBlobContext(globalId, stage, blobMetadata.blobId), content)
    asyncComputationControlClient.advanceComputation(
      advanceComputationRequest {
        globalComputationId = globalId
        computationStage = stage
        blobId = blobMetadata.blobId
        blobPath = blob.blobKey
      }
    )
  }
}
