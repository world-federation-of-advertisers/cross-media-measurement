/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers

import io.grpc.StatusException
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.frequencycount.SecretShareGeneratorAdapter
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.getRequisitionRequest
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultMinimumThresholder
import org.wfanet.measurement.edpaggregator.resultsfulfiller.isTransientGrpcFailure
import org.wfanet.measurement.edpaggregator.resultsfulfiller.retryTransient
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.FrequencyVectorBuilder
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.shareshuffle.FulfillRequisitionRequestBuilder

class HMShuffleMeasurementFulfiller(
  private val requisition: Requisition,
  private val requisitionNonce: Long,
  private val sampledFrequencyVector: FrequencyVector,
  private val dataProviderSigningKeyHandle: SigningKeyHandle,
  private val dataProviderCertificateKey: DataProviderCertificateKey,
  private val requisitionFulfillmentStubMap: Map<String, RequisitionFulfillmentCoroutineStub>,
  private val requisitionsStub: RequisitionsCoroutineStub,
  private val generateSecretShares: (ByteArray) -> (ByteArray) =
    SecretShareGeneratorAdapter::generateSecretShares,
  private val retryMaxAttempts: Int = DEFAULT_RETRY_MAX_ATTEMPTS,
  private val retryBackoff: ExponentialBackoff = ExponentialBackoff(),
) : MeasurementFulfiller {
  init {
    require(retryMaxAttempts >= 1) { "retryMaxAttempts must be at least 1" }
  }

  override suspend fun fulfillRequisition() {
    logger.info("Fulfilling requisition ${requisition.name}...")
    val duchyId = getDuchyWithoutPublicKey(requisition)
    val requisitionFulfillmentStub = requisitionFulfillmentStubMap.getValue(duchyId)
    try {
      // Retry the worker RPCs on transient failures (UNAVAILABLE / DEADLINE_EXCEEDED). The whole
      // block is retried, rebuilding the request flow each attempt, so a stalled or dropped
      // connection self-heals instead of failing the entire report. `getRequisition` re-checks the
      // state on each attempt, so a requisition fulfilled by a prior attempt is skipped.
      retryTransient(retryMaxAttempts, retryBackoff, ::isTransientGrpcFailure) {
        val getRequisitionResponse =
          requisitionsStub.getRequisition(getRequisitionRequest { name = requisition.name })
        if (getRequisitionResponse.state === Requisition.State.UNFULFILLED) {
          val requests: Flow<FulfillRequisitionRequest> =
            FulfillRequisitionRequestBuilder.build(
                requisition,
                requisitionNonce,
                sampledFrequencyVector,
                dataProviderCertificateKey,
                dataProviderSigningKeyHandle,
                getRequisitionResponse.etag,
                generateSecretShares,
              )
              .asFlow()
          requisitionFulfillmentStub.fulfillRequisition(requests)
          logger.info("Successfully fulfilled HMShuffle requisition ${requisition.name}")
        } else {
          logger.info(
            "Cannot fulfill requisition ${requisition.name} with state ${getRequisitionResponse.state}"
          )
        }
      }
    } catch (e: StatusException) {
      throw Exception("Error fulfilling requisition ${requisition.name}", e)
    }
  }

  private fun getDuchyWithoutPublicKey(requisition: Requisition): String {
    return requisition.duchiesList
      .singleOrNull { !it.value.honestMajorityShareShuffle.hasPublicKey() }
      ?.key
      ?: throw IllegalArgumentException(
        "Expected exactly one Duchy entry with an HMSS encryption public key."
      )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /** Default maximum total attempts (first attempt + retries) for the fulfillment worker RPCs. */
    const val DEFAULT_RETRY_MAX_ATTEMPTS = 4

    /** Constructs a [HMShuffleMeasurementFulfiller] with a thresholded FrequencyVector. */
    fun buildThresholded(
      requisition: Requisition,
      requisitionNonce: Long,
      measurementSpec: MeasurementSpec,
      populationSpec: PopulationSpec,
      frequencyVectorBuilder: FrequencyVectorBuilder,
      dataProviderSigningKeyHandle: SigningKeyHandle,
      dataProviderCertificateKey: DataProviderCertificateKey,
      requisitionFulfillmentStubMap: Map<String, RequisitionFulfillmentCoroutineStub>,
      requisitionsStub: RequisitionsCoroutineStub,
      resultMinimumThresholds: ResultMinimumThresholds,
      maxPopulation: Int?,
      generateSecretShares: (ByteArray) -> (ByteArray) =
        SecretShareGeneratorAdapter::generateSecretShares,
    ): HMShuffleMeasurementFulfiller {
      val thresholdedFrequencyVector =
        ResultMinimumThresholder.applyThresholds(
          measurementSpec,
          populationSpec,
          frequencyVectorBuilder,
          resultMinimumThresholds,
          maxPopulation,
          protocolMinUsers = 0,
          protocolMinImpressions = 0,
        )
      return HMShuffleMeasurementFulfiller(
        requisition,
        requisitionNonce,
        thresholdedFrequencyVector,
        dataProviderSigningKeyHandle,
        dataProviderCertificateKey,
        requisitionFulfillmentStubMap,
        requisitionsStub,
        generateSecretShares,
      )
    }
  }
}
