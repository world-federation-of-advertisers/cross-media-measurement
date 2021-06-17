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

package org.wfanet.measurement.duchy.service.api.v2alpha

import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionResponse
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineImplBase
import org.wfanet.measurement.common.consumeFirst
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.duchy.service.api.v2alpha.utils.RequisitionKey as RequisitionKeyV2
import org.wfanet.measurement.duchy.storage.MetricValueStore
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.ExternalRequisitionKey
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.GetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.RecordRequisitionBlobPathRequest
import org.wfanet.measurement.system.v1alpha.FulfillRequisitionRequest as SystemFulfillRequisitionRequest
import org.wfanet.measurement.system.v1alpha.RequisitionKey
import org.wfanet.measurement.system.v1alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub

private val FULFILLED_RESPONSE =
  FulfillRequisitionResponse.newBuilder().apply { state = Requisition.State.FULFILLED }.build()

/** Implementation of `wfa.measurement.api.v2alpha.RequisitionFulfillment` gRPC service. */
class RequisitionFulfillmentService(
  private val systemRequisitionsClient: RequisitionsCoroutineStub,
  private val computationsClient: ComputationsCoroutineStub,
  private val storageClient: MetricValueStore
) : RequisitionFulfillmentCoroutineImplBase() {

  override suspend fun fulfillRequisition(
    requests: Flow<FulfillRequisitionRequest>
  ): FulfillRequisitionResponse {
    grpcRequireNotNull(requests.consumeFirst()) { "Empty request stream" }.use { consumed ->
      val header = consumed.item.header
      val key = RequisitionKeyV2.fromName(header.name) ?: failGrpc { "resource_key/name invalid." }
      grpcRequire(!header.dataProviderParticipationSignature.isEmpty) {
        "resource_key/fingerprint missing or incomplete in the header."
      }

      val externalRequisitionKey =
        ExternalRequisitionKey.newBuilder()
          .apply {
            externalDataProviderId = key.dataProviderId
            externalRequisitionId = key.requisitionId
          }
          .build()

      val computationToken = externalRequisitionKey.toComputationToken()

      // The requisition is guaranteed to exist in the token, so the find() will always succeed.
      val alreadyMarkedFulfilled =
        computationToken.requisitionsList.find {
            it.externalDataProviderId == externalRequisitionKey.externalDataProviderId &&
              it.externalRequisitionId == externalRequisitionKey.externalRequisitionId
          }!!
          .path.isNotBlank()

      // Only try writing to the blob store if it is not already marked fulfilled.
      // TODO(world-federation-of-advertisers/cross-media-measurement#85): Handle the case that it
      //  is already marked fulfilled locally.
      if (!alreadyMarkedFulfilled) {
        val blob = storageClient.write(consumed.remaining.map { it.bodyChunk.data })
        recordRequisitionBlobPathLocally(computationToken, externalRequisitionKey, blob.blobKey)
      }

      fulfillRequisitionAtKingdom(
        computationToken.globalComputationId,
        externalRequisitionKey.externalRequisitionId,
        header.dataProviderParticipationSignature
      )

      return FULFILLED_RESPONSE
    }
  }

  /** Gets the token of the computation that this requisition is used in. */
  private suspend fun ExternalRequisitionKey.toComputationToken(): ComputationToken {
    val request = GetComputationTokenRequest.newBuilder().also { it.requisitionKey = this }.build()
    return getComputationToken(request).token
  }

  /** Sends a request to get computation token. */
  private suspend fun getComputationToken(
    request: GetComputationTokenRequest
  ): GetComputationTokenResponse {
    return try {
      computationsClient.getComputationToken(request)
    } catch (e: StatusException) {
      if (e.status.code == Status.Code.NOT_FOUND) {
        throw Status.NOT_FOUND
          .withDescription("No computation is expecting this requisition $this.")
          .asRuntimeException()
      } else {
        throw Status.INTERNAL.withCause(e).asRuntimeException()
      }
    }
  }

  /** Sends rpc to the duchy's internal ComputationsService to record requisition blob path. */
  private suspend fun recordRequisitionBlobPathLocally(
    token: ComputationToken,
    key: ExternalRequisitionKey,
    blobPath: String
  ) {
    computationsClient.recordRequisitionBlobPath(
      RecordRequisitionBlobPathRequest.newBuilder()
        .also {
          it.token = token
          it.key = key
          it.blobPath = blobPath
        }
        .build()
    )
  }

  /** send rpc to the kingdom's system RequisitionsService to fulfill a requisition. */
  private suspend fun fulfillRequisitionAtKingdom(
    computationId: String,
    requisitionId: String,
    signature: ByteString
  ) {
    systemRequisitionsClient.fulfillRequisition(
      SystemFulfillRequisitionRequest.newBuilder()
        .apply {
          name = RequisitionKey(computationId, requisitionId).toName()
          dataProviderParticipationSignature = signature
        }
        .build()
    )
  }

  /** Returns a blob key derived from this [ExternalRequisitionKey]. */
  private fun ExternalRequisitionKey.toBlobKey(): String {
    return "/requisitions/$externalDataProviderId/$externalRequisitionId"
  }
}
