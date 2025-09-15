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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import com.google.protobuf.kotlin.unpack
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.computation.KAnonymityParams
import org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.protocols.direct.DirectMeasurementResultFactory
import org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers.DirectMeasurementFulfiller
import org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers.HMShuffleMeasurementFulfiller
import org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers.MeasurementFulfiller
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.FrequencyVectorBuilder
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.common.crypto.SigningKeyHandle

/** Default implementation of [FulfillerSelector] mirroring prior selection logic. */
class DefaultFulfillerSelector(
  private val requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub,
  private val requisitionFulfillmentStubMap:
    Map<String, RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub>,
  private val dataProviderCertificateKey: DataProviderCertificateKey,
  private val dataProviderSigningKeyHandle: SigningKeyHandle,
  private val noiserSelector: NoiserSelector,
  private val kAnonymityParams: KAnonymityParams?,
) : FulfillerSelector {

  override suspend fun selectFulfiller(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    requisitionSpec: RequisitionSpec,
    frequencyData: IntArray,
    populationSpec: PopulationSpec,
  ): MeasurementFulfiller {
    val vec = createFrequencyVectorBuilderFromArray(measurementSpec, populationSpec, frequencyData)

    return if (requisition.protocolConfig.protocolsList.any { it.hasDirect() }) {
      buildDirectMeasurementFulfiller(
        requisition = requisition,
        measurementSpec = measurementSpec,
        requisitionSpec = requisitionSpec,
        maxPopulation = null,
        frequencyData = vec.frequencyDataArray,
        kAnonymityParams = kAnonymityParams,
      )
    } else if (requisition.protocolConfig.protocolsList.any { it.hasHonestMajorityShareShuffle() }) {
      if (kAnonymityParams == null) {
        HMShuffleMeasurementFulfiller(
          requisition,
          requisitionSpec.nonce,
          vec.build(),
          dataProviderSigningKeyHandle,
          dataProviderCertificateKey,
          requisitionFulfillmentStubMap,
        )
      } else {
        HMShuffleMeasurementFulfiller.buildKAnonymized(
          requisition,
          requisitionSpec.nonce,
          measurementSpec,
          populationSpec,
          vec,
          dataProviderSigningKeyHandle,
          dataProviderCertificateKey,
          requisitionFulfillmentStubMap,
          kAnonymityParams,
          maxPopulation = null,
        )
      }
    } else {
      throw IllegalArgumentException("Protocol not supported for ${'$'}{requisition.name}")
    }
  }

  private fun createFrequencyVectorBuilderFromArray(
    measurementSpec: MeasurementSpec,
    populationSpec: PopulationSpec,
    frequencyData: IntArray,
  ): FrequencyVectorBuilder {
    val builder = FrequencyVectorBuilder(populationSpec, measurementSpec, strict = false)
    for (index in frequencyData.indices) {
      val frequency = frequencyData[index]
      if (frequency > 0) {
        builder.incrementBy(index, frequency)
      }
    }
    return builder
  }

  private suspend fun buildDirectMeasurementFulfiller(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    requisitionSpec: RequisitionSpec,
    maxPopulation: Int?,
    frequencyData: IntArray,
    kAnonymityParams: KAnonymityParams?,
  ): DirectMeasurementFulfiller {
    val measurementEncryptionPublicKey: EncryptionPublicKey =
      measurementSpec.measurementPublicKey.unpack()
    val directProtocolConfig =
      requisition.protocolConfig.protocolsList.first { it.hasDirect() }.direct
    val noiseMechanism =
      noiserSelector.selectNoiseMechanism(directProtocolConfig.noiseMechanismsList)
    val result =
      withContext(Dispatchers.Default) {
        DirectMeasurementResultFactory.buildMeasurementResult(
          directProtocolConfig,
          noiseMechanism,
          measurementSpec,
          frequencyData,
          maxPopulation,
          kAnonymityParams = kAnonymityParams,
        )
      }
    return DirectMeasurementFulfiller(
      requisition.name,
      requisition.dataProviderCertificate,
      result,
      requisitionSpec.nonce,
      measurementEncryptionPublicKey,
      directProtocolConfig,
      noiseMechanism,
      dataProviderSigningKeyHandle,
      dataProviderCertificateKey,
      requisitionsStub,
    )
  }
}
