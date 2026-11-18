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
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultMinimumThresholder
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.FrequencyVectorBuilder
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.trustee.FulfillRequisitionRequestBuilder

class TrusTeeMeasurementFulfiller
private constructor(
  private val requisition: Requisition,
  private val buildRequests: () -> Sequence<FulfillRequisitionRequest>,
  private val requisitionFulfillmentStubMap: Map<String, RequisitionFulfillmentCoroutineStub>,
  private val requisitionsStub: RequisitionsCoroutineStub,
  private val requisitionsThrottler: Throttler,
) : MeasurementFulfiller {
  constructor(
    requisition: Requisition,
    requisitionNonce: Long,
    sampledFrequencyVector: FrequencyVector,
    requisitionFulfillmentStubMap: Map<String, RequisitionFulfillmentCoroutineStub>,
    requisitionsStub: RequisitionsCoroutineStub,
    requisitionsThrottler: Throttler,
    encryptionParams: FulfillRequisitionRequestBuilder.EncryptionParams?,
    fulfillmentDetails: FulfillRequisitionRequest.Header.TrusTeeV2.FulfillmentDetails? = null,
  ) : this(
    requisition = requisition,
    buildRequests = {
      if (encryptionParams != null) {
        FulfillRequisitionRequestBuilder.buildEncrypted(
          requisition,
          requisitionNonce,
          sampledFrequencyVector,
          encryptionParams,
          fulfillmentDetails,
        )
      } else {
        FulfillRequisitionRequestBuilder.buildUnencrypted(
          requisition,
          requisitionNonce,
          sampledFrequencyVector,
          fulfillmentDetails,
        )
      }
    },
    requisitionFulfillmentStubMap = requisitionFulfillmentStubMap,
    requisitionsStub = requisitionsStub,
    requisitionsThrottler = requisitionsThrottler,
  )

  /** Borrows [sampledFrequencyVector]; keep it unchanged until fulfillment completes. */
  constructor(
    requisition: Requisition,
    requisitionNonce: Long,
    sampledFrequencyVector: ByteArray,
    requisitionFulfillmentStubMap: Map<String, RequisitionFulfillmentCoroutineStub>,
    requisitionsStub: RequisitionsCoroutineStub,
    requisitionsThrottler: Throttler,
    encryptionParams: FulfillRequisitionRequestBuilder.EncryptionParams?,
    fulfillmentDetails: FulfillRequisitionRequest.Header.TrusTeeV2.FulfillmentDetails? = null,
  ) : this(
    requisition = requisition,
    buildRequests = {
      if (encryptionParams != null) {
        FulfillRequisitionRequestBuilder.buildEncrypted(
          requisition,
          requisitionNonce,
          sampledFrequencyVector,
          encryptionParams,
          fulfillmentDetails,
        )
      } else {
        FulfillRequisitionRequestBuilder.buildUnencrypted(
          requisition,
          requisitionNonce,
          sampledFrequencyVector,
          fulfillmentDetails,
        )
      }
    },
    requisitionFulfillmentStubMap = requisitionFulfillmentStubMap,
    requisitionsStub = requisitionsStub,
    requisitionsThrottler = requisitionsThrottler,
  )

  override suspend fun fulfillRequisition() {
    logger.info("Fulfilling requisition ${requisition.name}...")
    val duchyId = getAggregatorDuchy(requisition)
    val requisitionFulfillmentStub = requisitionFulfillmentStubMap.getValue(duchyId)
    try {
      val getRequisitionResponse =
        requisitionsThrottler.onReady {
          requisitionsStub.getRequisition(getRequisitionRequest { name = requisition.name })
        }
      if (getRequisitionResponse.state === Requisition.State.UNFULFILLED) {
        val requests: Flow<FulfillRequisitionRequest> = buildRequests().asFlow()
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
    return requisition.duchiesList
      .singleOrNull { it.value.hasTrusTee() || it.value.hasTrusTeeV2() }
      ?.key
      ?: throw IllegalArgumentException(
        "Expected exactly one Duchy entry with TrusTee or TrusTeeV2 protocol configuration."
      )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /** Constructs a [TrusTeeMeasurementFulfiller] with a thresholded FrequencyVector. */
    fun buildThresholded(
      requisition: Requisition,
      requisitionNonce: Long,
      measurementSpec: MeasurementSpec,
      populationSpec: PopulationSpec,
      frequencyVectorBuilder: FrequencyVectorBuilder,
      requisitionFulfillmentStubMap: Map<String, RequisitionFulfillmentCoroutineStub>,
      requisitionsStub: RequisitionsCoroutineStub,
      requisitionsThrottler: Throttler,
      resultMinimumThresholds: ResultMinimumThresholds,
      protocolMinUsers: Int,
      protocolMinImpressions: Int,
      maxPopulation: Int?,
      encryptionParams: FulfillRequisitionRequestBuilder.EncryptionParams?,
    ): TrusTeeMeasurementFulfiller {
      val thresholdedFrequencyVector =
        ResultMinimumThresholder.applyThresholds(
          measurementSpec,
          populationSpec,
          frequencyVectorBuilder,
          resultMinimumThresholds,
          maxPopulation,
          protocolMinUsers,
          protocolMinImpressions,
        )
      return TrusTeeMeasurementFulfiller(
        requisition,
        requisitionNonce,
        thresholdedFrequencyVector,
        requisitionFulfillmentStubMap,
        requisitionsStub,
        requisitionsThrottler,
        encryptionParams,
      )
    }

    /** Constructs a [TrusTeeMeasurementFulfiller] with a thresholded byte vector. */
    fun buildThresholded(
      requisition: Requisition,
      requisitionNonce: Long,
      measurementSpec: MeasurementSpec,
      frequencyData: ByteArray,
      requisitionFulfillmentStubMap: Map<String, RequisitionFulfillmentCoroutineStub>,
      requisitionsStub: RequisitionsCoroutineStub,
      requisitionsThrottler: Throttler,
      resultMinimumThresholds: ResultMinimumThresholds,
      protocolMinUsers: Int,
      protocolMinImpressions: Int,
      maxPopulation: Int?,
      encryptionParams: FulfillRequisitionRequestBuilder.EncryptionParams?,
    ): TrusTeeMeasurementFulfiller {
      val thresholdedFrequencyVector =
        if (
          ResultMinimumThresholder.isBelowThreshold(
            frequencyData,
            measurementSpec.vidSamplingInterval.width.toDouble(),
            resultMinimumThresholds,
            maxPopulation,
            protocolMinUsers,
            protocolMinImpressions,
          )
        ) {
          ByteArray(frequencyData.size)
        } else {
          frequencyData
        }

      return TrusTeeMeasurementFulfiller(
        requisition,
        requisitionNonce,
        thresholdedFrequencyVector,
        requisitionFulfillmentStubMap,
        requisitionsStub,
        requisitionsThrottler,
        encryptionParams,
      )
    }
  }
}
