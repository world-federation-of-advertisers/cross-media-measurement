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
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.getRequisitionRequest
import org.wfanet.measurement.computation.HistogramComputations
import org.wfanet.measurement.computation.KAnonymityParams
import org.wfanet.measurement.computation.ReachAndFrequencyComputations
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.FrequencyVectorBuilder
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.trustee.FulfillRequisitionRequestBuilder

class TrusTeeMeasurementFulfiller(
  private val requisition: Requisition,
  private val requisitionNonce: Long,
  private val sampledFrequencyVector: FrequencyVector,
  private val requisitionFulfillmentStubMap: Map<String, RequisitionFulfillmentCoroutineStub>,
  private val requisitionsStub: RequisitionsCoroutineStub,
  private val encryptionParams: FulfillRequisitionRequestBuilder.EncryptionParams?,
) : MeasurementFulfiller {
  override suspend fun fulfillRequisition() {
    logger.info("Fulfilling requisition ${requisition.name}...")
    val duchyId = getAggregatorDuchy(requisition)
    val requisitionFulfillmentStub = requisitionFulfillmentStubMap.getValue(duchyId)
    try {
      val getRequisitionResponse =
        requisitionsStub.getRequisition(getRequisitionRequest { name = requisition.name })
      if (getRequisitionResponse.state === Requisition.State.UNFULFILLED) {
        val requests: Flow<FulfillRequisitionRequest> =
          if (encryptionParams != null) {
            FulfillRequisitionRequestBuilder.buildEncrypted(
                requisition,
                requisitionNonce,
                sampledFrequencyVector,
                encryptionParams,
              )
              .asFlow()
          } else {
            FulfillRequisitionRequestBuilder.buildUnencrypted(
                requisition,
                requisitionNonce,
                sampledFrequencyVector,
              )
              .asFlow()
          }
        requisitionFulfillmentStub.fulfillRequisition(requests)
        logger.info("Successfully fulfilled TrusTee requisition ${requisition.name}")
      } else {
        logger.info(
          "Cannot fulfill requisition ${requisition.name} with state ${getRequisitionResponse.state}"
        )
      }
    } catch (e: StatusException) {
      logger.warning("Error fulfilling requisition ${requisition.name}")
      throw e
    }
  }

  private fun getAggregatorDuchy(requisition: Requisition): String {
    return requisition.duchiesList.singleOrNull { it.value.hasTrusTee() }?.key
      ?: throw IllegalArgumentException(
        "Expected exactly one Duchy entry with TrusTee protocol configuration."
      )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /** Constructs a [TrusTeeMeasurementFulfiller] with a k-anonymized FrequencyVector. */
    fun buildKAnonymized(
      requisition: Requisition,
      requisitionNonce: Long,
      measurementSpec: MeasurementSpec,
      populationSpec: PopulationSpec,
      frequencyVectorBuilder: FrequencyVectorBuilder,
      requisitionFulfillmentStubMap: Map<String, RequisitionFulfillmentCoroutineStub>,
      requisitionsStub: RequisitionsCoroutineStub,
      kAnonymityParams: KAnonymityParams,
      maxPopulation: Int?,
      encryptionParams: FulfillRequisitionRequestBuilder.EncryptionParams?,
    ): TrusTeeMeasurementFulfiller {
      val kAnonymizedFrequencyVector =
        kAnonymize(
          measurementSpec,
          populationSpec,
          frequencyVectorBuilder,
          kAnonymityParams,
          maxPopulation,
        )
      return TrusTeeMeasurementFulfiller(
        requisition,
        requisitionNonce,
        kAnonymizedFrequencyVector,
        requisitionFulfillmentStubMap,
        requisitionsStub,
        encryptionParams,
      )
    }

    /**
     * Returns an empty FrequencyVector if k-anonymity threshold is not met for reach. It does not
     * k-anonymize individual frequencies that do not meet a threshold.
     */
    private fun kAnonymize(
      measurementSpec: MeasurementSpec,
      populationSpec: PopulationSpec,
      frequencyVectorBuilder: FrequencyVectorBuilder,
      kAnonymityParams: KAnonymityParams,
      maxPopulation: Int?,
    ): FrequencyVector {
      val frequencyData = frequencyVectorBuilder.frequencyDataArray
      val histogram: LongArray =
        HistogramComputations.buildHistogram(
          frequencyVector = frequencyData,
          maxFrequency = kAnonymityParams.reachMaxFrequencyPerUser,
        )
      val reachValue =
        ReachAndFrequencyComputations.computeReach(
          rawHistogram = histogram,
          vidSamplingIntervalWidth = measurementSpec.vidSamplingInterval.width,
          vectorSize = maxPopulation,
          dpParams = null,
          kAnonymityParams = kAnonymityParams,
        )
      return if (reachValue == 0L) {
        FrequencyVectorBuilder(
            measurementSpec = measurementSpec,
            populationSpec = populationSpec,
            strict = false,
            overrideImpressionMaxFrequencyPerUser = null,
          )
          .build()
      } else {
        frequencyVectorBuilder.build()
      }
    }
  }
}
