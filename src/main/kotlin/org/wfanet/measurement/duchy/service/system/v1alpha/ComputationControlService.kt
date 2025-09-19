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
import io.grpc.Status
import io.grpc.StatusException
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
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
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt
import org.wfanet.measurement.internal.duchy.advanceComputationRequest
import org.wfanet.measurement.internal.duchy.getComputationTokenRequest
import org.wfanet.measurement.internal.duchy.getOutputBlobMetadataRequest
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequest
import org.wfanet.measurement.system.v1alpha.AdvanceComputationResponse
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationKey
import org.wfanet.measurement.system.v1alpha.ComputationStage
import org.wfanet.measurement.system.v1alpha.GetComputationStageRequest
import org.wfanet.measurement.system.v1alpha.StageKey

/**
 * Service for controlling inter-Duchy operations on Computation related resources.
 *
 * @param duchyId the id of the duchy hosting this service.
 * @param asyncComputationControlClient client to the internal service.
 * @param computationStore storage to the computation blobs.
 * @param duchyIdentityProvider a provider to fetch the duchy identity of the caller.
 */
class ComputationControlService(
  private val duchyId: String,
  private val internalComputationsClient: ComputationsGrpcKt.ComputationsCoroutineStub,
  private val asyncComputationControlClient: AsyncComputationControlCoroutineStub,
  private val computationStore: ComputationStore,
  coroutineContext: CoroutineContext,
  private val duchyIdentityProvider: () -> DuchyIdentity = ::duchyIdentityFromContext,
) : ComputationControlCoroutineImplBase(coroutineContext) {

  constructor(
    duchyId: String,
    internalComputationsClient: ComputationsGrpcKt.ComputationsCoroutineStub,
    asyncComputationControlClient: AsyncComputationControlCoroutineStub,
    storageClient: StorageClient,
    coroutineContext: CoroutineContext,
    duchyIdentityProvider: () -> DuchyIdentity = ::duchyIdentityFromContext,
  ) : this(
    duchyId,
    internalComputationsClient,
    asyncComputationControlClient,
    ComputationStore(storageClient),
    coroutineContext,
    duchyIdentityProvider = duchyIdentityProvider,
  )

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
            globalComputationId,
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
    globalId: String,
  ) {
    val stage = header.stageExpectingInput()

    val blobMetadata =
      asyncComputationControlClient.getOutputBlobMetadata(
        getOutputBlobMetadataRequest {
          globalComputationId = globalId
          dataOrigin = duchyIdentityProvider().id
        }
      )
    val blob =
      computationStore.write(ComputationBlobContext(globalId, stage, blobMetadata.blobId), content)

    val request = advanceComputationRequest {
      globalComputationId = globalId
      computationStage = stage
      blobId = blobMetadata.blobId
      blobPath = blob.blobKey
    }
    try {
      asyncComputationControlClient.advanceComputation(request)
    } catch (e: StatusException) {
      throw when (e.status.code) {
          Status.Code.UNAVAILABLE -> Status.UNAVAILABLE
          Status.Code.ABORTED -> Status.ABORTED
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          else -> Status.UNKNOWN
        }
        .withCause(e)
        .asRuntimeException()
    }
  }

  override suspend fun getComputationStage(request: GetComputationStageRequest): ComputationStage {
    val stageKey = grpcRequireNotNull(StageKey.fromName(request.name)) { "Invalid Stage name." }
    grpcRequire(stageKey.duchyId == duchyId) {
      "Unmatched duchyId. request_duchy_id=${stageKey.duchyId}, service_duchy_id=${duchyId}"
    }
    val computationToken =
      try {
        internalComputationsClient
          .getComputationToken(
            getComputationTokenRequest { globalComputationId = stageKey.computationId }
          )
          .token
      } catch (e: StatusException) {
        logger.log(Level.WARNING) {
          "Fail to get computation token. global_computation_id=${stageKey.computationId}"
        }
        throw when (e.status.code) {
            Status.Code.UNAVAILABLE -> Status.UNAVAILABLE
            Status.Code.ABORTED -> Status.ABORTED
            Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
            Status.Code.NOT_FOUND -> Status.NOT_FOUND
            else -> Status.UNKNOWN
          }
          .withCause(e)
          .asRuntimeException()
      }
    return computationToken.toSystemStage(duchyId)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
