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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Measurement.DataProviderEntry
import org.wfanet.measurement.api.v2alpha.Measurement.State
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt.DataProviderEntryKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKey
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.liquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.DifferentialPrivacyParams as InternalDifferentialPrivacyParams
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.kingdom.Measurement.DataProviderValue
import org.wfanet.measurement.internal.kingdom.MeasurementKt.dataProviderValue
import org.wfanet.measurement.internal.kingdom.MeasurementKt.details
import org.wfanet.measurement.internal.kingdom.ProtocolConfig as InternalProtocolConfig
import org.wfanet.measurement.internal.kingdom.duchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.measurement as internalMeasurement
import org.wfanet.measurement.internal.kingdom.protocolConfig as internalProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig

/** Converts an internal [InternalMeasurement.State] to a public [State]. */
fun InternalMeasurement.State.toState(): State =
  when (this) {
    InternalMeasurement.State.PENDING_REQUISITION_PARAMS,
    InternalMeasurement.State.PENDING_REQUISITION_FULFILLMENT ->
      State.AWAITING_REQUISITION_FULFILLMENT
    InternalMeasurement.State.PENDING_PARTICIPANT_CONFIRMATION,
    InternalMeasurement.State.PENDING_COMPUTATION -> State.COMPUTING
    InternalMeasurement.State.SUCCEEDED -> State.SUCCEEDED
    InternalMeasurement.State.FAILED -> State.FAILED
    InternalMeasurement.State.CANCELLED -> State.CANCELLED
    InternalMeasurement.State.STATE_UNSPECIFIED, InternalMeasurement.State.UNRECOGNIZED ->
      State.STATE_UNSPECIFIED
  }

fun InternalDifferentialPrivacyParams.toDifferentialPrivacyParams(): DifferentialPrivacyParams {
  val source = this
  return differentialPrivacyParams {
    epsilon = source.epsilon
    delta = source.delta
  }
}

fun InternalProtocolConfig.toProtocolConfig(): ProtocolConfig {
  val source = this
  return protocolConfig {
    name = ProtocolConfigKey(source.externalProtocolConfigId).toName()
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    measurementType =
      when (source.measurementType) {
        InternalProtocolConfig.MeasurementType.MEASUREMENT_TYPE_UNSPECIFIED ->
          ProtocolConfig.MeasurementType.MEASUREMENT_TYPE_UNSPECIFIED
        InternalProtocolConfig.MeasurementType.REACH_AND_FREQUENCY ->
          ProtocolConfig.MeasurementType.REACH_AND_FREQUENCY
        InternalProtocolConfig.MeasurementType.UNRECOGNIZED ->
          error("MeasurementType unrecognized.")
      }
    if (source.hasLiquidLegionsV2()) {
      liquidLegionsV2 =
        liquidLegionsV2 {
          if (source.liquidLegionsV2.hasSketchParams()) {
            val sourceSketchParams = source.liquidLegionsV2.sketchParams
            sketchParams =
              liquidLegionsSketchParams {
                decayRate = sourceSketchParams.decayRate
                maxSize = sourceSketchParams.maxSize
                samplingIndicatorSize = sourceSketchParams.samplingIndicatorSize
              }
          }
          if (source.liquidLegionsV2.hasDataProviderNoise()) {
            dataProviderNoise =
              source.liquidLegionsV2.dataProviderNoise.toDifferentialPrivacyParams()
          }
          ellipticCurveId = source.liquidLegionsV2.ellipticCurveId
          maximumFrequency = source.liquidLegionsV2.maximumFrequency
        }
    }
  }
}

/** Converts an internal [InternalMeasurement] to a public [Measurement]. */
fun InternalMeasurement.toMeasurement(): Measurement {
  val source = this
  check(Version.fromString(source.details.apiVersion) == Version.V2_ALPHA) {
    "Incompatible API version ${source.details.apiVersion}"
  }
  return measurement {
    name =
      MeasurementKey(
          externalIdToApiId(source.externalMeasurementConsumerId),
          externalIdToApiId(source.externalMeasurementId)
        )
        .toName()
    measurementConsumerCertificate =
      MeasurementConsumerCertificateKey(
          externalIdToApiId(source.externalMeasurementConsumerId),
          externalIdToApiId(source.externalMeasurementConsumerCertificateId)
        )
        .toName()
    measurementSpec =
      signedData {
        data = source.details.measurementSpec
        signature = source.details.measurementSpecSignature
      }
    serializedDataProviderList = source.details.dataProviderList
    dataProviderListSalt = source.details.dataProviderListSalt
    dataProviders +=
      source.dataProvidersMap.entries.map(Map.Entry<Long, DataProviderValue>::toDataProviderEntry)
    protocolConfig = source.details.protocolConfig.toProtocolConfig()
    state = source.state.toState()
    aggregatorCertificate = source.details.aggregatorCertificate
    encryptedResult = source.details.encryptedResult
    measurementReferenceId = source.providedMeasurementId
  }
}

/** Converts an internal [DataProviderValue] to a public [DataProviderEntry.Value]. */
fun DataProviderValue.toDataProviderEntryValue(dataProviderId: String): DataProviderEntry.Value {
  val dataProviderValue = this
  return DataProviderEntryKt.value {
    dataProviderCertificate =
      DataProviderCertificateKey(
          dataProviderId,
          externalIdToApiId(externalDataProviderCertificateId)
        )
        .toName()
    dataProviderPublicKey =
      signedData {
        data = dataProviderValue.dataProviderPublicKey
        signature = dataProviderPublicKeySignature
      }
    encryptedRequisitionSpec = dataProviderValue.encryptedRequisitionSpec
  }
}

/** Converts an internal data provider map entry to a public [DataProviderEntry]. */
fun Map.Entry<Long, DataProviderValue>.toDataProviderEntry(): DataProviderEntry {
  val mapEntry = this
  return dataProviderEntry {
    key = DataProviderKey(externalIdToApiId(mapEntry.key)).toName()
    value = mapEntry.value.toDataProviderEntryValue(externalIdToApiId(mapEntry.key))
  }
}

/** Converts a public [Measurement] to an internal [InternalMeasurement] for creation. */
fun Measurement.toInternal(
  measurementConsumerCertificateKey: MeasurementConsumerCertificateKey,
  dataProvidersMap: Map<Long, DataProviderValue>,
  measurementSpecProto: MeasurementSpec
): InternalMeasurement {
  val publicMeasurement = this
  val internalProtocolConfig: InternalProtocolConfig
  val internalDuchyProtocolConfig: DuchyProtocolConfig
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  when (measurementSpecProto.measurementTypeCase) {
    MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY -> {
      internalProtocolConfig =
        internalProtocolConfig {
          externalProtocolConfigId = Llv2ProtocolConfig.name
          measurementType = InternalProtocolConfig.MeasurementType.REACH_AND_FREQUENCY
          liquidLegionsV2 = Llv2ProtocolConfig.protocolConfig
        }
      internalDuchyProtocolConfig =
        duchyProtocolConfig { liquidLegionsV2 = Llv2ProtocolConfig.duchyProtocolConfig }
    }
    MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET -> error("MeasurementType not set.")
  }
  return internalMeasurement {
    providedMeasurementId = measurementReferenceId
    externalMeasurementConsumerId =
      apiIdToExternalId(measurementConsumerCertificateKey.measurementConsumerId)
    externalMeasurementConsumerCertificateId =
      apiIdToExternalId(measurementConsumerCertificateKey.certificateId)
    dataProviders.putAll(dataProvidersMap)
    details =
      details {
        apiVersion = Version.V2_ALPHA.string
        measurementSpec = publicMeasurement.measurementSpec.data
        measurementSpecSignature = publicMeasurement.measurementSpec.signature
        dataProviderList = publicMeasurement.serializedDataProviderList
        dataProviderListSalt = publicMeasurement.dataProviderListSalt
        protocolConfig = internalProtocolConfig
        duchyProtocolConfig = internalDuchyProtocolConfig
      }
  }
}
