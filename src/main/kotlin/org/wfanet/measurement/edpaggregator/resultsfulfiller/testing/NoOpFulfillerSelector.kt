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

package org.wfanet.measurement.edpaggregator.resultsfulfiller.testing

import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.edpaggregator.resultsfulfiller.FulfillerSelector
import org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers.MeasurementFulfiller
import java.util.logging.Logger

/** 
 * No-op implementation of [FulfillerSelector] for testing purposes.
 * 
 * This selector creates mock fulfillers that log information about the requisition
 * but do not actually fulfill any measurement requests. Useful for testing the
 * ResultsFulfiller pipeline without making actual RPC calls.
 */
class NoOpFulfillerSelector : FulfillerSelector {

  override suspend fun selectFulfiller(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    requisitionSpec: RequisitionSpec,
    frequencyData: IntArray,
    populationSpec: PopulationSpec,
  ): MeasurementFulfiller {
    logger.info("[NOOP_SELECTOR] Creating NoOpMeasurementFulfiller for requisition: ${requisition.name}")
    logger.info("[NOOP_SELECTOR] Measurement type: ${measurementSpec.measurementTypeCase}")
    logger.info("[NOOP_SELECTOR] Frequency data size: ${frequencyData.size}")
    logger.info("[NOOP_SELECTOR] Population spec subpopulations: ${populationSpec.subpopulationsCount}")
    return NoOpMeasurementFulfiller(requisition, frequencyData)
  }

  companion object {
    private val logger = Logger.getLogger(NoOpFulfillerSelector::class.java.name)
  }

  /**
   * No-op implementation of [MeasurementFulfiller] that logs requisition details
   * but does not perform actual fulfillment.
   */
  private class NoOpMeasurementFulfiller(
    private val requisition: Requisition,
    private val frequencyData: IntArray,
  ) : MeasurementFulfiller {

    override suspend fun fulfillRequisition() {
      logger.info("[NOOP_FULFILLER] Starting fulfillRequisition() for: ${requisition.name}")
      
      val startTime = System.currentTimeMillis()
      val nonZeroFrequencies = frequencyData.count { it > 0 }
      val totalFrequency = frequencyData.sum()
      val maxFrequency = frequencyData.maxOrNull() ?: 0
      val avgFrequency = if (frequencyData.isNotEmpty()) frequencyData.average() else 0.0
      
      logger.info(
        """
        |[NOOP_FULFILLER] === MOCK FULFILLMENT ===
        |  Requisition: ${requisition.name}
        |  Protocol: ${requisition.protocolConfig.protocolsList.map { it.protocolCase }}
        |  Measurement type: ${requisition.measurementSpec.message.typeUrl}
        |  State: ${requisition.state}
        |  Data provider certificate: ${requisition.dataProviderCertificate}
        |  Frequency data analysis:
        |    - Array size: ${frequencyData.size}
        |    - Non-zero entries: $nonZeroFrequencies
        |    - Total frequency: $totalFrequency
        |    - Max frequency: $maxFrequency
        |    - Average frequency: $avgFrequency
        |  Processing time: ${System.currentTimeMillis() - startTime}ms
        |================================
        """.trimMargin()
      )
      
      // Simulate some processing time
      kotlinx.coroutines.delay(10)
      
      logger.info("[NOOP_FULFILLER] Mock fulfillment completed for: ${requisition.name}")
    }

    companion object {
      private val logger = Logger.getLogger(NoOpMeasurementFulfiller::class.java.name)
    }
  }
}