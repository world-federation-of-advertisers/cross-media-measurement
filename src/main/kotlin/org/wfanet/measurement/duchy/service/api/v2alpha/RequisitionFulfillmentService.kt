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

import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.CanonicalRequisitionKey
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionResponse
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.consumeFirst
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.consent.client.duchy.Requisition as ConsentSignalingRequisition
import org.wfanet.measurement.consent.client.duchy.verifyRequisitionFulfillment
import org.wfanet.measurement.duchy.storage.RequisitionBlobContext
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.ExternalRequisitionKey
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.GetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.RecordRequisitionBlobPathRequest
import org.wfanet.measurement.internal.duchy.RequisitionMetadata
import org.wfanet.measurement.internal.duchy.externalRequisitionKey
import org.wfanet.measurement.internal.duchy.getComputationTokenRequest
import org.wfanet.measurement.system.v1alpha.RequisitionKey as SystemRequisitionKey
import org.wfanet.measurement.system.v1alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.system.v1alpha.fulfillRequisitionRequest as systemFulfillRequisitionRequest

private val FULFILLED_RESPONSE =
  FulfillRequisitionResponse.newBuilder().apply { state = Requisition.State.FULFILLED }.build()

/** Implementation of `wfa.measurement.api.v2alpha.RequisitionFulfillment` gRPC service. */
class RequisitionFulfillmentService(
  private val systemRequisitionsClient: RequisitionsCoroutineStub,
  private val computationsClient: ComputationsCoroutineStub,
  private val requisitionStore: RequisitionStore,
) : RequisitionFulfillmentCoroutineImplBase() {

  private enum class Permission {
    FULFILL;

    fun deniedStatus(name: String): Status {
      return Status.PERMISSION_DENIED.withDescription(
        "Permission $this denied on resource $name (or it might not exist)."
      )
    }
  }

  override suspend fun fulfillRequisition(
    requests: Flow<FulfillRequisitionRequest>
  ): FulfillRequisitionResponse {
    grpcRequireNotNull(requests.consumeFirst()) { "Empty request stream" }
      .use { consumed ->
        val header = consumed.item.header
        val key =
          grpcRequireNotNull(CanonicalRequisitionKey.fromName(header.name)) {
            "Resource name unspecified or invalid."
          }
        grpcRequire(header.nonce != 0L) { "nonce unspecified" }

        val authenticatedPrincipal = principalFromCurrentContext
        if (key.parentKey != authenticatedPrincipal.resourceKey) {
          throw Permission.FULFILL.deniedStatus(header.name).asRuntimeException()
        }

        val externalRequisitionKey = externalRequisitionKey {
          externalRequisitionId = key.requisitionId
          requisitionFingerprint = header.requisitionFingerprint
        }
        val computationToken = getComputationToken(externalRequisitionKey)
        val requisitionMetadata =
          verifyRequisitionFulfillment(computationToken, externalRequisitionKey, header.nonce)

        // Only update Computation if it is not already marked fulfilled.
        // TODO(world-federation-of-advertisers/cross-media-measurement#85): Handle the case that it
        //  is already marked fulfilled locally.
        if (!requisitionMetadata.isFulfilled()) {
          if (header.hasHonestMajorityShareShuffle()) {

          } else {
            val blob =
              requisitionStore.write(
                RequisitionBlobContext(computationToken.globalComputationId, key.requisitionId),
                consumed.remaining.map { it.bodyChunk.data }
              )
            recordRequisitionBlobPathLocally(computationToken, externalRequisitionKey, blob.blobKey)
          }
        }

        fulfillRequisitionAtKingdom(
          computationToken.globalComputationId,
          externalRequisitionKey.externalRequisitionId,
          header.nonce
        )

        return FULFILLED_RESPONSE
      }
  }

  private suspend fun getComputationToken(
    requisitionKey: ExternalRequisitionKey
  ): ComputationToken {
    return getComputationToken(getComputationTokenRequest { this.requisitionKey = requisitionKey })
      .token
  }

  /** Sends a request to get computation token. */
  private suspend fun getComputationToken(
    request: GetComputationTokenRequest
  ): GetComputationTokenResponse {
    return try {
      computationsClient.getComputationToken(request)
    } catch (e: StatusException) {
      if (e.status.code == Status.Code.NOT_FOUND) {
        throw Status.NOT_FOUND.withDescription(
            "No computation is expecting this requisition $this."
          )
          .asRuntimeException()
      } else {
        throw Status.INTERNAL.withCause(e).asRuntimeException()
      }
    }
  }

  private fun verifyRequisitionFulfillment(
    computationToken: ComputationToken,
    requisitionKey: ExternalRequisitionKey,
    nonce: Long
  ): RequisitionMetadata {
    val kingdomComputation = computationToken.computationDetails.kingdomComputation
    val requisitionMetadata =
      checkNotNull(computationToken.requisitionsList.find { it.externalKey == requisitionKey })

    val publicApiVersion =
      Version.fromStringOrNull(kingdomComputation.publicApiVersion)
        ?: throw Status.FAILED_PRECONDITION.withDescription(
            "Public API version invalid or unspecified"
          )
          .asRuntimeException()
    when (publicApiVersion) {
      Version.V2_ALPHA -> {
        val measurementSpec = MeasurementSpec.parseFrom(kingdomComputation.measurementSpec)
        if (
          !verifyRequisitionFulfillment(
            measurementSpec,
            requisitionMetadata.toConsentSignalingRequisition(),
            requisitionKey.requisitionFingerprint,
            nonce
          )
        ) {
          throw Status.FAILED_PRECONDITION.withDescription(
              "Requisition fulfillment could not be verified"
            )
            .asRuntimeException()
        }
      }
    }

    return requisitionMetadata
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
    nonce: Long
  ) {
    systemRequisitionsClient.fulfillRequisition(
      systemFulfillRequisitionRequest {
        name = SystemRequisitionKey(computationId, requisitionId).toName()
        this.nonce = nonce
      }
    )
  }
}

private fun RequisitionMetadata.toConsentSignalingRequisition() =
  ConsentSignalingRequisition(externalKey.requisitionFingerprint, details.nonceHash)

private fun RequisitionMetadata.isFulfilled(): Boolean = this.hasPath() || this.hasSeed()
