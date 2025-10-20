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

import com.google.protobuf.any
import com.google.protobuf.util.Timestamps
import com.google.type.interval
import java.time.ZoneOffset
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.Account
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeKey
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepKey
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.Exchange
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKt
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Measurement.DataProviderEntry
import org.wfanet.measurement.api.v2alpha.Measurement.Failure
import org.wfanet.measurement.api.v2alpha.Measurement.State
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt.DataProviderEntryKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementKt.failure
import org.wfanet.measurement.api.v2alpha.MeasurementKt.resultOutput
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLine.Type
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelOutage
import org.wfanet.measurement.api.v2alpha.ModelOutage.State as ModelOutageState
import org.wfanet.measurement.api.v2alpha.ModelOutageKey
import org.wfanet.measurement.api.v2alpha.ModelProvider
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelRelease
import org.wfanet.measurement.api.v2alpha.ModelReleaseKey
import org.wfanet.measurement.api.v2alpha.ModelRollout
import org.wfanet.measurement.api.v2alpha.ModelRolloutKey
import org.wfanet.measurement.api.v2alpha.ModelShard
import org.wfanet.measurement.api.v2alpha.ModelShardKey
import org.wfanet.measurement.api.v2alpha.ModelShardKt.modelBlob
import org.wfanet.measurement.api.v2alpha.ModelSuite
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.Population
import org.wfanet.measurement.api.v2alpha.PopulationKey
import org.wfanet.measurement.api.v2alpha.PopulationKt.populationBlob
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.direct
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.honestMajorityShareShuffle
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.protocol
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.reachOnlyLiquidLegionsV2
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.trusTee
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.dateInterval
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.encryptedMessage
import org.wfanet.measurement.api.v2alpha.exchange
import org.wfanet.measurement.api.v2alpha.exchangeStep
import org.wfanet.measurement.api.v2alpha.exchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.liquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.modelOutage
import org.wfanet.measurement.api.v2alpha.modelProvider
import org.wfanet.measurement.api.v2alpha.modelRelease
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.modelShard
import org.wfanet.measurement.api.v2alpha.modelSuite
import org.wfanet.measurement.api.v2alpha.population
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.reachOnlyLiquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Account as InternalAccount
import org.wfanet.measurement.internal.kingdom.Certificate as InternalCertificate
import org.wfanet.measurement.internal.kingdom.DifferentialPrivacyParams as InternalDifferentialPrivacyParams
import org.wfanet.measurement.internal.kingdom.EventGroup as InternalEventGroup
import org.wfanet.measurement.internal.kingdom.Exchange as InternalExchange
import org.wfanet.measurement.internal.kingdom.ExchangeStep as InternalExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt as InternalExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetails
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetailsKt
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow as InternalExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflowKt
import org.wfanet.measurement.internal.kingdom.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.kingdom.Measurement.DataProviderValue
import org.wfanet.measurement.internal.kingdom.MeasurementFailure as InternalMeasurementFailure
import org.wfanet.measurement.internal.kingdom.ModelLine as InternalModelLine
import org.wfanet.measurement.internal.kingdom.ModelOutage as InternalModelOutage
import org.wfanet.measurement.internal.kingdom.ModelProvider as InternalModelProvider
import org.wfanet.measurement.internal.kingdom.ModelRelease as InternalModelRelease
import org.wfanet.measurement.internal.kingdom.ModelRollout as InternalModelRollout
import org.wfanet.measurement.internal.kingdom.ModelShard as InternalModelShard
import org.wfanet.measurement.internal.kingdom.ModelSuite as InternalModelSuite
import org.wfanet.measurement.internal.kingdom.Population as InternalPopulation
import org.wfanet.measurement.internal.kingdom.PopulationDetails
import org.wfanet.measurement.internal.kingdom.PopulationDetailsKt
import org.wfanet.measurement.internal.kingdom.PopulationKt as InternalPopulationKt
import org.wfanet.measurement.internal.kingdom.ProtocolConfig as InternalProtocolConfig
import org.wfanet.measurement.internal.kingdom.ProtocolConfig.NoiseMechanism as InternalNoiseMechanism
import org.wfanet.measurement.internal.kingdom.Requisition as InternalRequisition
import org.wfanet.measurement.internal.kingdom.duchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.exchangeWorkflow
import org.wfanet.measurement.internal.kingdom.measurement as internalMeasurement
import org.wfanet.measurement.internal.kingdom.measurementDetails as internalMeasurementDetails
import org.wfanet.measurement.internal.kingdom.modelLine as internalModelLine
import org.wfanet.measurement.internal.kingdom.modelOutage as internalModelOutage
import org.wfanet.measurement.internal.kingdom.modelRelease as internalModelRelease
import org.wfanet.measurement.internal.kingdom.modelRollout as internalModelRollout
import org.wfanet.measurement.internal.kingdom.modelShard as internalModelShard
import org.wfanet.measurement.internal.kingdom.modelSuite as internalModelSuite
import org.wfanet.measurement.internal.kingdom.population as internalPopulation
import org.wfanet.measurement.internal.kingdom.populationDetails
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.RoLlv2ProtocolConfig

/** Default options of direct noise mechanisms to data providers. */
val DEFAULT_DIRECT_NOISE_MECHANISMS: List<NoiseMechanism> =
  listOf(
    NoiseMechanism.NONE,
    NoiseMechanism.GEOMETRIC,
    NoiseMechanism.DISCRETE_GAUSSIAN,
    NoiseMechanism.CONTINUOUS_LAPLACE,
    NoiseMechanism.CONTINUOUS_GAUSSIAN,
  )

/**
 * Default direct reach protocol config for backward compatibility.
 *
 * Used when existing direct protocol configs of reach measurements don't have methodologies.
 */
val DEFAULT_DIRECT_REACH_PROTOCOL_CONFIG: ProtocolConfig.Direct = direct {
  noiseMechanisms += DEFAULT_DIRECT_NOISE_MECHANISMS
  customDirectMethodology = ProtocolConfig.Direct.CustomDirectMethodology.getDefaultInstance()
  deterministicCountDistinct = ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
  liquidLegionsCountDistinct = ProtocolConfig.Direct.LiquidLegionsCountDistinct.getDefaultInstance()
}

/**
 * Default direct reach-and-frequency protocol config for backward compatibility.
 *
 * Used when existing direct protocol configs of reach-and-frequency measurements don't have
 * methodologies.
 */
val DEFAULT_DIRECT_REACH_AND_FREQUENCY_PROTOCOL_CONFIG: ProtocolConfig.Direct = direct {
  noiseMechanisms += DEFAULT_DIRECT_NOISE_MECHANISMS
  customDirectMethodology = ProtocolConfig.Direct.CustomDirectMethodology.getDefaultInstance()
  deterministicCountDistinct = ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
  liquidLegionsCountDistinct = ProtocolConfig.Direct.LiquidLegionsCountDistinct.getDefaultInstance()
  deterministicDistribution = ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
  liquidLegionsDistribution = ProtocolConfig.Direct.LiquidLegionsDistribution.getDefaultInstance()
}

/**
 * Default direct impression protocol config for backward compatibility.
 *
 * Used when existing direct protocol configs of impression measurements don't have methodologies.
 */
val DEFAULT_DIRECT_IMPRESSION_PROTOCOL_CONFIG = direct {
  noiseMechanisms += DEFAULT_DIRECT_NOISE_MECHANISMS
  customDirectMethodology = ProtocolConfig.Direct.CustomDirectMethodology.getDefaultInstance()
  deterministicCount = ProtocolConfig.Direct.DeterministicCount.getDefaultInstance()
}

/**
 * Default direct watch duration protocol config for backward compatibility.
 *
 * Used when existing direct protocol configs of watch duration measurements don't have
 * methodologies.
 */
val DEFAULT_DIRECT_WATCH_DURATION_PROTOCOL_CONFIG = direct {
  noiseMechanisms += DEFAULT_DIRECT_NOISE_MECHANISMS
  customDirectMethodology = ProtocolConfig.Direct.CustomDirectMethodology.getDefaultInstance()
  deterministicSum = ProtocolConfig.Direct.DeterministicSum.getDefaultInstance()
}

/**
 * Default direct population protocol config for backward compatibility.
 *
 * Used when existing direct protocol configs of population measurements don't have methodologies.
 */
val DEFAULT_DIRECT_POPULATION_PROTOCOL_CONFIG = direct {
  noiseMechanisms += DEFAULT_DIRECT_NOISE_MECHANISMS
  deterministicCount = ProtocolConfig.Direct.DeterministicCount.getDefaultInstance()
}

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
    InternalMeasurement.State.STATE_UNSPECIFIED,
    InternalMeasurement.State.UNRECOGNIZED -> State.STATE_UNSPECIFIED
  }

/** Convert a public [State] to an internal [InternalMeasurement.State]. */
fun State.toInternalState(): List<InternalMeasurement.State> {
  return when (this) {
    State.AWAITING_REQUISITION_FULFILLMENT -> {
      listOf(
        InternalMeasurement.State.PENDING_REQUISITION_PARAMS,
        InternalMeasurement.State.PENDING_REQUISITION_FULFILLMENT,
      )
    }
    State.COMPUTING -> {
      listOf(
        InternalMeasurement.State.PENDING_PARTICIPANT_CONFIRMATION,
        InternalMeasurement.State.PENDING_COMPUTATION,
      )
    }
    State.SUCCEEDED -> listOf(InternalMeasurement.State.SUCCEEDED)
    State.FAILED -> listOf(InternalMeasurement.State.FAILED)
    State.CANCELLED -> listOf(InternalMeasurement.State.CANCELLED)
    State.STATE_UNSPECIFIED,
    State.UNRECOGNIZED -> listOf(InternalMeasurement.State.STATE_UNSPECIFIED)
  }
}

/** Converts an internal [InternalMeasurementFailure.Reason] to a public [Failure.Reason]. */
fun InternalMeasurementFailure.Reason.toReason(): Failure.Reason =
  when (this) {
    InternalMeasurementFailure.Reason.CERTIFICATE_REVOKED -> Failure.Reason.CERTIFICATE_REVOKED
    InternalMeasurementFailure.Reason.REQUISITION_REFUSED -> Failure.Reason.REQUISITION_REFUSED
    InternalMeasurementFailure.Reason.COMPUTATION_PARTICIPANT_FAILED ->
      Failure.Reason.COMPUTATION_PARTICIPANT_FAILED
    InternalMeasurementFailure.Reason.REASON_UNSPECIFIED,
    InternalMeasurementFailure.Reason.UNRECOGNIZED -> Failure.Reason.REASON_UNSPECIFIED
  }

fun InternalDifferentialPrivacyParams.toDifferentialPrivacyParams(): DifferentialPrivacyParams {
  val source = this
  return differentialPrivacyParams {
    epsilon = source.epsilon
    delta = source.delta
  }
}

/** Converts an internal [InternalNoiseMechanism] to a public [NoiseMechanism]. */
fun InternalNoiseMechanism.toNoiseMechanism(): NoiseMechanism {
  return when (this) {
    InternalNoiseMechanism.NONE -> NoiseMechanism.NONE
    InternalNoiseMechanism.GEOMETRIC -> NoiseMechanism.GEOMETRIC
    InternalNoiseMechanism.DISCRETE_GAUSSIAN -> NoiseMechanism.DISCRETE_GAUSSIAN
    InternalNoiseMechanism.CONTINUOUS_LAPLACE -> NoiseMechanism.CONTINUOUS_LAPLACE
    InternalNoiseMechanism.CONTINUOUS_GAUSSIAN -> NoiseMechanism.CONTINUOUS_GAUSSIAN
    InternalNoiseMechanism.NOISE_MECHANISM_UNSPECIFIED,
    InternalNoiseMechanism.UNRECOGNIZED -> error("invalid internal noise mechanism.")
  }
}

/** Converts a public [NoiseMechanism] to an internal [InternalNoiseMechanism]. */
fun NoiseMechanism.toInternal(): InternalNoiseMechanism {
  return when (this) {
    NoiseMechanism.GEOMETRIC -> InternalNoiseMechanism.GEOMETRIC
    NoiseMechanism.DISCRETE_GAUSSIAN -> InternalNoiseMechanism.DISCRETE_GAUSSIAN
    NoiseMechanism.NONE -> InternalNoiseMechanism.NONE
    NoiseMechanism.CONTINUOUS_LAPLACE -> InternalNoiseMechanism.CONTINUOUS_LAPLACE
    NoiseMechanism.CONTINUOUS_GAUSSIAN -> InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
    NoiseMechanism.NOISE_MECHANISM_UNSPECIFIED,
    NoiseMechanism.UNRECOGNIZED -> error("invalid internal noise mechanism.")
  }
}

/** Converts an internal [InternalProtocolConfig] to a public [ProtocolConfig]. */
fun InternalProtocolConfig.toProtocolConfig(
  measurementTypeCase: MeasurementSpec.MeasurementTypeCase,
  dataProviderCount: Int,
): ProtocolConfig {
  require(dataProviderCount >= 1) { "Expected at least one DataProvider" }

  val source = this
  return protocolConfig {
    measurementType =
      when (measurementTypeCase) {
        MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET ->
          throw IllegalArgumentException("Measurement type not specified")
        MeasurementSpec.MeasurementTypeCase.REACH -> ProtocolConfig.MeasurementType.REACH
        MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY ->
          ProtocolConfig.MeasurementType.REACH_AND_FREQUENCY
        MeasurementSpec.MeasurementTypeCase.IMPRESSION -> ProtocolConfig.MeasurementType.IMPRESSION
        MeasurementSpec.MeasurementTypeCase.DURATION -> ProtocolConfig.MeasurementType.DURATION
        MeasurementSpec.MeasurementTypeCase.POPULATION -> ProtocolConfig.MeasurementType.POPULATION
      }

    when (measurementType) {
      ProtocolConfig.MeasurementType.REACH -> {
        protocols +=
          // Direct protocol takes precedence
          if (dataProviderCount == 1) {
            protocol {
              direct =
                if (source.hasDirect()) {
                  source.direct.toDirect()
                } else {
                  // For backward compatibility
                  DEFAULT_DIRECT_REACH_PROTOCOL_CONFIG
                }
            }
          } else {
            buildMpcProtocolConfig(measurementTypeCase, source)
          }
      }
      ProtocolConfig.MeasurementType.REACH_AND_FREQUENCY -> {
        protocols +=
          // Direct protocol takes precedence
          if (dataProviderCount == 1) {
            protocol {
              direct =
                if (source.hasDirect()) {
                  source.direct.toDirect()
                } else {
                  // For backward compatibility
                  DEFAULT_DIRECT_REACH_AND_FREQUENCY_PROTOCOL_CONFIG
                }
            }
          } else {
            buildMpcProtocolConfig(measurementTypeCase, source)
          }
      }
      ProtocolConfig.MeasurementType.IMPRESSION -> {
        protocols += protocol {
          direct =
            if (source.hasDirect()) {
              source.direct.toDirect()
            } else {
              // For backward compatibility
              DEFAULT_DIRECT_IMPRESSION_PROTOCOL_CONFIG
            }
        }
      }
      ProtocolConfig.MeasurementType.DURATION -> {
        protocols += protocol {
          direct =
            if (source.hasDirect()) {
              source.direct.toDirect()
            } else {
              // For backward compatibility
              DEFAULT_DIRECT_WATCH_DURATION_PROTOCOL_CONFIG
            }
        }
      }
      ProtocolConfig.MeasurementType.POPULATION -> {
        protocols += protocol {
          direct =
            if (source.hasDirect()) {
              source.direct.toDirect()
            } else {
              // For backward compatibility
              DEFAULT_DIRECT_POPULATION_PROTOCOL_CONFIG
            }
        }
      }
      ProtocolConfig.MeasurementType.MEASUREMENT_TYPE_UNSPECIFIED,
      ProtocolConfig.MeasurementType.UNRECOGNIZED -> error("Invalid MeasurementType")
    }
  }
}

/**
 * Builds a public [ProtocolConfig.Protocol] for MPC only from an internal [InternalProtocolConfig].
 */
private fun buildMpcProtocolConfig(
  measurementType: MeasurementSpec.MeasurementTypeCase,
  protocolConfig: InternalProtocolConfig,
): ProtocolConfig.Protocol {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields are never null.
  return when (protocolConfig.protocolCase) {
    InternalProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2 -> {
      protocol {
        liquidLegionsV2 = liquidLegionsV2 {
          if (protocolConfig.liquidLegionsV2.hasSketchParams()) {
            val sourceSketchParams = protocolConfig.liquidLegionsV2.sketchParams
            sketchParams = liquidLegionsSketchParams {
              decayRate = sourceSketchParams.decayRate
              maxSize = sourceSketchParams.maxSize
              samplingIndicatorSize = sourceSketchParams.samplingIndicatorSize
            }
          }
          if (protocolConfig.liquidLegionsV2.hasDataProviderNoise()) {
            dataProviderNoise =
              protocolConfig.liquidLegionsV2.dataProviderNoise.toDifferentialPrivacyParams()
          }
          ellipticCurveId = protocolConfig.liquidLegionsV2.ellipticCurveId
          @Suppress("DEPRECATION") // For legacy Measurements.
          maximumFrequency = protocolConfig.liquidLegionsV2.maximumFrequency
          noiseMechanism =
            if (
              protocolConfig.liquidLegionsV2.noiseMechanism ==
                InternalNoiseMechanism.NOISE_MECHANISM_UNSPECIFIED
            ) {
              // Use `GEOMETRIC` for unspecified InternalNoiseMechanism for old Measurements.
              NoiseMechanism.GEOMETRIC
            } else {
              protocolConfig.liquidLegionsV2.noiseMechanism.toNoiseMechanism()
            }
        }
      }
    }
    InternalProtocolConfig.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
      protocol {
        reachOnlyLiquidLegionsV2 = reachOnlyLiquidLegionsV2 {
          if (protocolConfig.reachOnlyLiquidLegionsV2.hasSketchParams()) {
            val sourceSketchParams = protocolConfig.reachOnlyLiquidLegionsV2.sketchParams
            sketchParams = reachOnlyLiquidLegionsSketchParams {
              decayRate = sourceSketchParams.decayRate
              maxSize = sourceSketchParams.maxSize
            }
          }
          if (protocolConfig.reachOnlyLiquidLegionsV2.hasDataProviderNoise()) {
            dataProviderNoise =
              protocolConfig.reachOnlyLiquidLegionsV2.dataProviderNoise
                .toDifferentialPrivacyParams()
          }
          ellipticCurveId = protocolConfig.reachOnlyLiquidLegionsV2.ellipticCurveId
          noiseMechanism =
            if (
              protocolConfig.reachOnlyLiquidLegionsV2.noiseMechanism ==
                InternalNoiseMechanism.NOISE_MECHANISM_UNSPECIFIED
            ) {
              NoiseMechanism.GEOMETRIC
            } else {
              protocolConfig.reachOnlyLiquidLegionsV2.noiseMechanism.toNoiseMechanism()
            }
        }
      }
    }
    InternalProtocolConfig.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
      protocol {
        honestMajorityShareShuffle = honestMajorityShareShuffle {
          ringModulus =
            when (measurementType) {
              MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY ->
                protocolConfig.honestMajorityShareShuffle.reachAndFrequencyRingModulus
              MeasurementSpec.MeasurementTypeCase.REACH ->
                protocolConfig.honestMajorityShareShuffle.reachRingModulus
              else -> error("Unsupported measurement type for ring modulus.")
            }
          noiseMechanism =
            protocolConfig.honestMajorityShareShuffle.noiseMechanism.toNoiseMechanism()
        }
      }
    }
    InternalProtocolConfig.ProtocolCase.TRUS_TEE -> {
      protocol {
        trusTee = trusTee {
          noiseMechanism = protocolConfig.trusTee.noiseMechanism.toNoiseMechanism()
        }
      }
    }
    InternalProtocolConfig.ProtocolCase.DIRECT -> {
      error("Direct protocol cannot be used for MPC-based Measurements")
    }
    InternalProtocolConfig.ProtocolCase.PROTOCOL_NOT_SET -> {
      error("Protocol not specified")
    }
  }
}

/**
 * Converts an internal [InternalProtocolConfig.Direct] to a public [InternalProtocolConfig.Direct].
 */
private fun InternalProtocolConfig.Direct.toDirect(): ProtocolConfig.Direct {
  val source = this

  return direct {
    noiseMechanisms +=
      source.noiseMechanismsList.map { internalNoiseMechanism ->
        internalNoiseMechanism.toNoiseMechanism()
      }

    if (source.hasCustomDirectMethodology()) {
      customDirectMethodology = ProtocolConfig.Direct.CustomDirectMethodology.getDefaultInstance()
    }
    if (source.hasDeterministicCountDistinct()) {
      deterministicCountDistinct =
        ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
    }
    if (source.hasDeterministicDistribution()) {
      deterministicDistribution =
        ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
    }
    if (source.hasDeterministicCount()) {
      deterministicCount = ProtocolConfig.Direct.DeterministicCount.getDefaultInstance()
    }
    if (source.hasDeterministicSum()) {
      deterministicSum = ProtocolConfig.Direct.DeterministicSum.getDefaultInstance()
    }
    if (source.hasLiquidLegionsCountDistinct()) {
      liquidLegionsCountDistinct =
        ProtocolConfig.Direct.LiquidLegionsCountDistinct.getDefaultInstance()
    }
    if (source.hasLiquidLegionsDistribution()) {
      liquidLegionsDistribution =
        ProtocolConfig.Direct.LiquidLegionsDistribution.getDefaultInstance()
    }
  }
}

/** Converts an internal [InternalModelProvider] to a public [ModelProvider]. */
fun InternalModelProvider.toModelProvider(): ModelProvider {
  val source = this

  return modelProvider {
    name = ModelProviderKey(externalIdToApiId(source.externalModelProviderId)).toName()
  }
}

/** Converts an internal [InternalModelSuite] to a public [ModelSuite]. */
fun InternalModelSuite.toModelSuite(): ModelSuite {
  val source = this

  return modelSuite {
    name =
      ModelSuiteKey(
          externalIdToApiId(source.externalModelProviderId),
          externalIdToApiId(source.externalModelSuiteId),
        )
        .toName()
    displayName = source.displayName
    description = source.description
    createTime = source.createTime
  }
}

/** Converts a public [ModelSuite] to an internal [InternalModelSuite] */
fun ModelSuite.toInternal(modelProviderKey: ModelProviderKey): InternalModelSuite {
  val publicModelSuite = this

  return internalModelSuite {
    externalModelProviderId = apiIdToExternalId(modelProviderKey.modelProviderId)
    displayName = publicModelSuite.displayName
    description = publicModelSuite.description
  }
}

/** Converts a public [ModelRelease] to an internal [InternalModelRelease] */
@Suppress("UnusedReceiverParameter") //
fun ModelRelease.toInternal(
  modelSuiteKey: ModelSuiteKey,
  populationKey: PopulationKey,
): InternalModelRelease {
  return internalModelRelease {
    externalModelProviderId = apiIdToExternalId(modelSuiteKey.modelProviderId)
    externalModelSuiteId = apiIdToExternalId(modelSuiteKey.modelSuiteId)
    externalDataProviderId = apiIdToExternalId(populationKey.dataProviderId)
    externalPopulationId = apiIdToExternalId(populationKey.populationId)
  }
}

/** Converts an internal [InternalModelRelease] to a public [ModelRelease]. */
fun InternalModelRelease.toModelRelease(): ModelRelease {
  val source = this

  return modelRelease {
    name =
      ModelReleaseKey(
          externalIdToApiId(source.externalModelProviderId),
          externalIdToApiId(source.externalModelSuiteId),
          externalIdToApiId(source.externalModelReleaseId),
        )
        .toName()
    createTime = source.createTime
    population =
      PopulationKey(
          externalIdToApiId(source.externalDataProviderId),
          externalIdToApiId(source.externalPopulationId),
        )
        .toName()
  }
}

/** Converts an internal [InternalModelLine.Type] to a public [Type]. */
fun InternalModelLine.Type.toType(): Type =
  when (this) {
    InternalModelLine.Type.DEV -> Type.DEV
    InternalModelLine.Type.PROD -> Type.PROD
    InternalModelLine.Type.HOLDBACK -> Type.HOLDBACK
    InternalModelLine.Type.UNRECOGNIZED,
    InternalModelLine.Type.TYPE_UNSPECIFIED -> Type.TYPE_UNSPECIFIED
  }

/** Convert a public [Type] to an internal [InternalModelLine.Type]. */
fun Type.toInternalType(): InternalModelLine.Type {
  return when (this) {
    Type.DEV -> InternalModelLine.Type.DEV
    Type.PROD -> InternalModelLine.Type.PROD
    Type.HOLDBACK -> InternalModelLine.Type.HOLDBACK
    Type.UNRECOGNIZED,
    Type.TYPE_UNSPECIFIED -> InternalModelLine.Type.TYPE_UNSPECIFIED
  }
}

/** Converts an internal [InternalModelLine] to a public [ModelLine]. */
fun InternalModelLine.toModelLine(): ModelLine {
  val source = this

  return modelLine {
    name =
      ModelLineKey(
          externalIdToApiId(source.externalModelProviderId),
          externalIdToApiId(source.externalModelSuiteId),
          externalIdToApiId(source.externalModelLineId),
        )
        .toName()
    displayName = source.displayName
    description = source.description
    activeStartTime = source.activeStartTime
    if (source.hasActiveEndTime()) {
      activeEndTime = source.activeEndTime
    }
    type = source.type.toType()
    if (source.externalHoldbackModelLineId != 0L) {
      holdbackModelLine =
        ModelLineKey(
            externalIdToApiId(source.externalModelProviderId),
            externalIdToApiId(source.externalModelSuiteId),
            externalIdToApiId(source.externalHoldbackModelLineId),
          )
          .toName()
    }
    createTime = source.createTime
    updateTime = source.updateTime
  }
}

/** Converts a public [ModelLine] to an internal [InternalModelLine] */
fun ModelLine.toInternal(modelSuiteKey: ModelSuiteKey): InternalModelLine {
  val publicModelLine = this

  return internalModelLine {
    externalModelProviderId = apiIdToExternalId(modelSuiteKey.modelProviderId)
    externalModelSuiteId = apiIdToExternalId(modelSuiteKey.modelSuiteId)
    displayName = publicModelLine.displayName
    description = publicModelLine.description
    activeStartTime = publicModelLine.activeStartTime
    if (publicModelLine.hasActiveEndTime()) {
      activeEndTime = publicModelLine.activeEndTime
    }
    type = publicModelLine.type.toInternalType()
    if (publicModelLine.holdbackModelLine.isNotEmpty()) {
      val modelLineKey =
        requireNotNull(ModelLineKey.fromName(publicModelLine.holdbackModelLine)) {
          "holdbackModelLine is invalid"
        }
      externalHoldbackModelLineId = apiIdToExternalId(modelLineKey.modelLineId)
    }
  }
}

/** Converts an internal [InternalModelOutage.State] to a public [ModelOutageState]. */
fun InternalModelOutage.State.toModelOutageState(): ModelOutageState =
  when (this) {
    InternalModelOutage.State.ACTIVE -> ModelOutageState.ACTIVE
    InternalModelOutage.State.DELETED -> ModelOutageState.DELETED
    InternalModelOutage.State.UNRECOGNIZED,
    InternalModelOutage.State.STATE_UNSPECIFIED -> ModelOutageState.STATE_UNSPECIFIED
  }

/** Converts an internal [InternalModelOutage] to a public [ModelOutage]. */
fun InternalModelOutage.toModelOutage(): ModelOutage {
  val source = this

  return modelOutage {
    name =
      ModelOutageKey(
          externalIdToApiId(source.externalModelProviderId),
          externalIdToApiId(source.externalModelSuiteId),
          externalIdToApiId(source.externalModelLineId),
          externalIdToApiId(source.externalModelOutageId),
        )
        .toName()
    outageInterval = interval {
      startTime = source.modelOutageStartTime
      endTime = source.modelOutageEndTime
    }
    state = source.state.toModelOutageState()
    createTime = source.createTime
    deleteTime = source.deleteTime
  }
}

/** Converts a public [ModelOutage] to an internal [InternalModelOutage] */
fun ModelOutage.toInternal(modelLineKey: ModelLineKey): InternalModelOutage {
  val publicModelOutage = this

  return internalModelOutage {
    externalModelProviderId = apiIdToExternalId(modelLineKey.modelProviderId)
    externalModelSuiteId = apiIdToExternalId(modelLineKey.modelSuiteId)
    externalModelLineId = apiIdToExternalId(modelLineKey.modelLineId)
    modelOutageStartTime = publicModelOutage.outageInterval.startTime
    modelOutageEndTime = publicModelOutage.outageInterval.endTime
  }
}

/** Converts an internal [InternalModelRollout] to a public [ModelRollout]. */
fun InternalModelRollout.toModelRollout(): ModelRollout {
  val source = this

  return modelRollout {
    name =
      ModelRolloutKey(
          externalIdToApiId(source.externalModelProviderId),
          externalIdToApiId(source.externalModelSuiteId),
          externalIdToApiId(source.externalModelLineId),
          externalIdToApiId(source.externalModelRolloutId),
        )
        .toName()

    if (Timestamps.compare(source.rolloutPeriodStartTime, source.rolloutPeriodEndTime) == 0) {
      instantRolloutDate =
        source.rolloutPeriodStartTime.toInstant().atZone(ZoneOffset.UTC).toLocalDate().toProtoDate()
    } else {
      gradualRolloutPeriod = dateInterval {
        startDate =
          source.rolloutPeriodStartTime
            .toInstant()
            .atZone(ZoneOffset.UTC)
            .toLocalDate()
            .toProtoDate()
        endDate =
          source.rolloutPeriodEndTime.toInstant().atZone(ZoneOffset.UTC).toLocalDate().toProtoDate()
      }
    }
    if (source.hasRolloutFreezeTime()) {
      rolloutFreezeDate =
        source.rolloutFreezeTime.toInstant().atZone(ZoneOffset.UTC).toLocalDate().toProtoDate()
    }
    if (source.externalPreviousModelRolloutId != 0L) {
      previousModelRollout =
        ModelRolloutKey(
            externalIdToApiId(source.externalModelProviderId),
            externalIdToApiId(source.externalModelSuiteId),
            externalIdToApiId(source.externalModelLineId),
            externalIdToApiId(source.externalPreviousModelRolloutId),
          )
          .toName()
    }
    modelRelease =
      ModelReleaseKey(
          externalIdToApiId(source.externalModelProviderId),
          externalIdToApiId(source.externalModelSuiteId),
          externalIdToApiId(source.externalModelReleaseId),
        )
        .toName()
    createTime = source.createTime
    updateTime = source.updateTime
  }
}

/** Converts a public [ModelRollout] to an internal [InternalModelRollout] */
fun ModelRollout.toInternal(
  modelLineKey: ModelLineKey,
  modelReleaseKey: ModelReleaseKey,
): InternalModelRollout {
  val publicModelRollout = this

  return internalModelRollout {
    externalModelProviderId = apiIdToExternalId(modelLineKey.modelProviderId)
    externalModelSuiteId = apiIdToExternalId(modelLineKey.modelSuiteId)
    externalModelLineId = apiIdToExternalId(modelLineKey.modelLineId)

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (publicModelRollout.rolloutDeployPeriodCase) {
      ModelRollout.RolloutDeployPeriodCase.GRADUAL_ROLLOUT_PERIOD -> {
        rolloutPeriodStartTime =
          publicModelRollout.gradualRolloutPeriod.startDate
            .toLocalDate()
            .atStartOfDay()
            .toInstant(ZoneOffset.UTC)
            .toProtoTime()
        rolloutPeriodEndTime =
          publicModelRollout.gradualRolloutPeriod.endDate
            .toLocalDate()
            .atStartOfDay()
            .toInstant(ZoneOffset.UTC)
            .toProtoTime()
      }
      ModelRollout.RolloutDeployPeriodCase.INSTANT_ROLLOUT_DATE -> {
        rolloutPeriodStartTime =
          publicModelRollout.instantRolloutDate
            .toLocalDate()
            .atStartOfDay()
            .toInstant(ZoneOffset.UTC)
            .toProtoTime()
        rolloutPeriodEndTime =
          publicModelRollout.instantRolloutDate
            .toLocalDate()
            .atStartOfDay()
            .toInstant(ZoneOffset.UTC)
            .toProtoTime()
      }
      ModelRollout.RolloutDeployPeriodCase.ROLLOUTDEPLOYPERIOD_NOT_SET -> {
        error("RolloutDeployPeriod not set.")
      }
    }

    if (publicModelRollout.hasRolloutFreezeDate()) {
      rolloutFreezeTime =
        publicModelRollout.rolloutFreezeDate
          .toLocalDate()
          .atStartOfDay()
          .toInstant(ZoneOffset.UTC)
          .toProtoTime()
    }

    externalModelReleaseId = apiIdToExternalId(modelReleaseKey.modelReleaseId)
  }
}

/** Converts an internal [InternalModelShard] to a public [ModelShard]. */
fun InternalModelShard.toModelShard(): ModelShard {
  val source = this

  return modelShard {
    name =
      ModelShardKey(
          externalIdToApiId(source.externalDataProviderId),
          externalIdToApiId(source.externalModelShardId),
        )
        .toName()
    modelRelease =
      ModelReleaseKey(
          externalIdToApiId(source.externalModelProviderId),
          externalIdToApiId(source.externalModelSuiteId),
          externalIdToApiId(source.externalModelReleaseId),
        )
        .toName()
    modelBlob = modelBlob { modelBlobPath = source.modelBlobPath }
    createTime = source.createTime
  }
}

/** Converts a public [ModelShard] to an internal [InternalModelShard] */
fun ModelShard.toInternal(
  dataProviderKey: DataProviderKey,
  modelReleaseKey: ModelReleaseKey,
): InternalModelShard {
  val publicModelShard = this

  return internalModelShard {
    externalDataProviderId = apiIdToExternalId(dataProviderKey.dataProviderId)
    externalModelProviderId = apiIdToExternalId(modelReleaseKey.modelProviderId)
    externalModelSuiteId = apiIdToExternalId(modelReleaseKey.modelSuiteId)
    externalModelReleaseId = apiIdToExternalId(modelReleaseKey.modelReleaseId)
    modelBlobPath = publicModelShard.modelBlob.modelBlobPath
  }
}

/** Converts an internal [InternalPopulation] to a public [Population]. */
fun InternalPopulation.toPopulation(): Population {
  val source = this

  return population {
    name =
      PopulationKey(
          externalIdToApiId(source.externalDataProviderId),
          externalIdToApiId(source.externalPopulationId),
        )
        .toName()
    description = source.description
    createTime = source.createTime
    if (source.details.hasPopulationSpec()) {
      populationSpec = source.details.populationSpec.toPopulationSpec()
    } else if (source.hasPopulationBlob()) {
      populationBlob = populationBlob { blobUri = source.populationBlob.blobUri }
    }
  }
}

/** Converts an internal PopulationSpec to a public [PopulationSpec]. */
fun PopulationDetails.PopulationSpec.toPopulationSpec(): PopulationSpec {
  val source = this

  return populationSpec {
    for (internalSubPopulation in source.subpopulationsList) {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          for (internalVidRange in internalSubPopulation.vidRangesList) {
            vidRanges +=
              PopulationSpecKt.vidRange {
                startVid = internalVidRange.startVid
                endVidInclusive = internalVidRange.endVidInclusive
              }
          }
          attributes += internalSubPopulation.attributesList
        }
    }
  }
}

/** Converts a public [Population] to an internal [InternalPopulation] */
fun Population.toInternal(dataProviderKey: DataProviderKey): InternalPopulation {
  val source = this

  return internalPopulation {
    externalDataProviderId = apiIdToExternalId(dataProviderKey.dataProviderId)
    description = source.description
    if (source.hasPopulationSpec()) {
      details = populationDetails { populationSpec = source.populationSpec.toInternal() }
    } else if (source.hasPopulationBlob()) {
      populationBlob =
        InternalPopulationKt.populationBlob { blobUri = source.populationBlob.blobUri }
    }
  }
}

/** Converts a [PopulationSpec] to an internal PopulationSpec. */
fun PopulationSpec.toInternal(): PopulationDetails.PopulationSpec {
  val source = this

  return PopulationDetailsKt.populationSpec {
    for (subPopulation in source.subpopulationsList) {
      subpopulations +=
        PopulationDetailsKt.PopulationSpecKt.subPopulation {
          for (vidRange in subPopulation.vidRangesList) {
            vidRanges +=
              PopulationDetailsKt.PopulationSpecKt.vidRange {
                startVid = vidRange.startVid
                endVidInclusive = vidRange.endVidInclusive
              }
          }
          attributes += subPopulation.attributesList
        }
    }
  }
}

/** Converts an internal [InternalMeasurement] to a public [Measurement]. */
fun InternalMeasurement.toMeasurement(): Measurement {
  val apiVersion = Version.fromString(details.apiVersion)

  val source = this
  return measurement {
    name =
      MeasurementKey(
          externalIdToApiId(source.externalMeasurementConsumerId),
          externalIdToApiId(source.externalMeasurementId),
        )
        .toName()
    measurementConsumerCertificate =
      MeasurementConsumerCertificateKey(
          externalIdToApiId(source.externalMeasurementConsumerId),
          externalIdToApiId(source.externalMeasurementConsumerCertificateId),
        )
        .toName()
    measurementSpec = signedMessage {
      setMessage(
        any {
          value = source.details.measurementSpec
          typeUrl =
            when (apiVersion) {
              Version.V2_ALPHA -> ProtoReflection.getTypeUrl(MeasurementSpec.getDescriptor())
            }
        }
      )
      signature = source.details.measurementSpecSignature
      signatureAlgorithmOid = source.details.measurementSpecSignatureAlgorithmOid
    }
    dataProviders += source.dataProvidersMap.entries.map { it.toDataProviderEntry(apiVersion) }

    val measurementTypeCase = measurementSpec.unpack<MeasurementSpec>().measurementTypeCase

    protocolConfig =
      source.details.protocolConfig.toProtocolConfig(measurementTypeCase, dataProvidersCount)

    state = source.state.toState()
    results +=
      source.resultsList.map {
        val resultApiVersion = Version.fromString(it.apiVersion)
        val certificateApiId = externalIdToApiId(it.externalCertificateId)
        resultOutput {
          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf oneof case fields cannot be null.
          certificate =
            when (it.certificateParentCase) {
              InternalMeasurement.ResultInfo.CertificateParentCase.EXTERNAL_AGGREGATOR_DUCHY_ID ->
                DuchyCertificateKey(it.externalAggregatorDuchyId, certificateApiId).toName()
              InternalMeasurement.ResultInfo.CertificateParentCase.EXTERNAL_DATA_PROVIDER_ID ->
                DataProviderCertificateKey(
                    externalIdToApiId(it.externalDataProviderId),
                    certificateApiId,
                  )
                  .toName()
              InternalMeasurement.ResultInfo.CertificateParentCase.CERTIFICATEPARENT_NOT_SET ->
                error("certificate_parent not set")
            }
          encryptedResult = encryptedMessage {
            ciphertext = it.encryptedResult
            typeUrl =
              when (resultApiVersion) {
                Version.V2_ALPHA -> ProtoReflection.getTypeUrl(SignedMessage.getDescriptor())
              }
          }
        }
      }
    measurementReferenceId = source.providedMeasurementId
    if (source.details.hasFailure()) {
      failure = failure {
        reason = source.details.failure.reason.toReason()
        message = source.details.failure.message
      }
    }
    createTime = source.createTime
    updateTime = source.updateTime
  }
}

/** Converts an internal [DataProviderValue] to a public [DataProviderEntry.Value]. */
fun DataProviderValue.toDataProviderEntryValue(
  dataProviderId: String,
  apiVersion: Version,
): DataProviderEntry.Value {
  val dataProviderValue = this
  return DataProviderEntryKt.value {
    dataProviderCertificate =
      DataProviderCertificateKey(
          dataProviderId,
          externalIdToApiId(externalDataProviderCertificateId),
        )
        .toName()
    dataProviderPublicKey = any {
      value = dataProviderValue.dataProviderPublicKey
      typeUrl =
        when (apiVersion) {
          Version.V2_ALPHA -> ProtoReflection.getTypeUrl(EncryptionPublicKey.getDescriptor())
        }
    }
    encryptedRequisitionSpec = encryptedMessage {
      ciphertext = dataProviderValue.encryptedRequisitionSpec
      typeUrl =
        when (apiVersion) {
          Version.V2_ALPHA -> ProtoReflection.getTypeUrl(SignedMessage.getDescriptor())
        }
    }
    nonceHash = dataProviderValue.nonceHash
  }
}

/** Converts an internal data provider map entry to a public [DataProviderEntry]. */
fun Map.Entry<Long, DataProviderValue>.toDataProviderEntry(apiVersion: Version): DataProviderEntry {
  val mapEntry = this
  return dataProviderEntry {
    key = DataProviderKey(externalIdToApiId(mapEntry.key)).toName()
    value = mapEntry.value.toDataProviderEntryValue(externalIdToApiId(mapEntry.key), apiVersion)
  }
}

/**
 * Converts a public [Measurement] to an internal [InternalMeasurement] for creation.
 *
 * @throws [IllegalStateException] if MeasurementType not specified
 */
fun Measurement.toInternal(
  measurementConsumerCertificateKey: MeasurementConsumerCertificateKey,
  dataProviderValues: Map<ExternalId, DataProviderValue>,
  internalProtocolConfig: InternalProtocolConfig,
): InternalMeasurement {
  val source = this
  return internalMeasurement {
    providedMeasurementId = measurementReferenceId
    externalMeasurementConsumerId =
      apiIdToExternalId(measurementConsumerCertificateKey.measurementConsumerId)
    externalMeasurementConsumerCertificateId =
      apiIdToExternalId(measurementConsumerCertificateKey.certificateId)
    dataProviders.putAll(dataProviderValues.mapKeys { it.key.value })
    details = internalMeasurementDetails {
      apiVersion = Version.V2_ALPHA.string
      measurementSpec = source.measurementSpec.message.value
      measurementSpecSignature = source.measurementSpec.signature
      measurementSpecSignatureAlgorithmOid = source.measurementSpec.signatureAlgorithmOid

      protocolConfig = internalProtocolConfig
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (protocolConfig.protocolCase) {
        InternalProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2 -> {
          duchyProtocolConfig = duchyProtocolConfig {
            liquidLegionsV2 = Llv2ProtocolConfig.duchyProtocolConfig
          }
        }
        InternalProtocolConfig.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
          duchyProtocolConfig = duchyProtocolConfig {
            reachOnlyLiquidLegionsV2 = RoLlv2ProtocolConfig.duchyProtocolConfig
          }
        }
        InternalProtocolConfig.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE,
        InternalProtocolConfig.ProtocolCase.TRUS_TEE,
        InternalProtocolConfig.ProtocolCase.DIRECT -> {}
        InternalProtocolConfig.ProtocolCase.PROTOCOL_NOT_SET -> error("protocol not set")
      }
    }
  }
}

/** @throws [IllegalStateException] if InternalExchange.State not specified */
fun InternalExchange.toExchange(): Exchange {
  val source = this
  val exchangeKey =
    CanonicalExchangeKey(
      recurringExchangeId = externalIdToApiId(externalRecurringExchangeId),
      exchangeId = date.toLocalDate().toString(),
    )
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // ProtoBuf enum fields cannot be null.
  val state =
    when (source.state) {
      InternalExchange.State.ACTIVE -> Exchange.State.ACTIVE
      InternalExchange.State.SUCCEEDED -> Exchange.State.SUCCEEDED
      InternalExchange.State.FAILED -> Exchange.State.FAILED
      InternalExchange.State.STATE_UNSPECIFIED,
      InternalExchange.State.UNRECOGNIZED -> error("Invalid InternalExchange state.")
    }

  return exchange {
    name = exchangeKey.toName()
    date = source.date
    this.state = state
    auditTrailHash = source.details.auditTrailHash
    // TODO(@yunyeng): Add graphvizRepresentation to Exchange proto.
    graphvizRepresentation = ""
  }
}

/** Converts this [InternalExchangeStep] to an [ExchangeStep]. */
fun InternalExchangeStep.toExchangeStep(): ExchangeStep {
  val exchangeStepKey =
    CanonicalExchangeStepKey(
      recurringExchangeId = externalIdToApiId(externalRecurringExchangeId),
      exchangeId = date.toLocalDate().toString(),
      exchangeStepId = stepIndex.toString(),
    )
  val apiVersion = Version.fromString(apiVersion)

  val source = this
  return exchangeStep {
    name = exchangeStepKey.toName()
    exchangeDate = source.date
    stepIndex = source.stepIndex

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
    when (source.partyCase) {
      InternalExchangeStep.PartyCase.EXTERNAL_DATA_PROVIDER_ID ->
        dataProvider =
          DataProviderKey(ExternalId(source.externalDataProviderId).apiId.value).toName()
      InternalExchangeStep.PartyCase.EXTERNAL_MODEL_PROVIDER_ID ->
        modelProvider =
          ModelProviderKey(ExternalId(source.externalModelProviderId).apiId.value).toName()
      InternalExchangeStep.PartyCase.PARTY_NOT_SET -> error("party not set")
    }

    state = source.state.toState()
    exchangeWorkflow = any {
      value = source.serializedExchangeWorkflow
      typeUrl =
        when (apiVersion) {
          Version.V2_ALPHA -> ProtoReflection.getTypeUrl(ExchangeWorkflow.getDescriptor())
        }
    }
  }
}

/** @throws [IllegalStateException] if State not specified */
fun ExchangeStepAttempt.State.toInternal(): InternalExchangeStepAttempt.State {
  return when (this) {
    ExchangeStepAttempt.State.STATE_UNSPECIFIED,
    ExchangeStepAttempt.State.UNRECOGNIZED -> error("Invalid ExchangeStepAttempt state: $this")
    ExchangeStepAttempt.State.ACTIVE -> InternalExchangeStepAttempt.State.ACTIVE
    ExchangeStepAttempt.State.SUCCEEDED -> InternalExchangeStepAttempt.State.SUCCEEDED
    ExchangeStepAttempt.State.FAILED -> InternalExchangeStepAttempt.State.FAILED
    ExchangeStepAttempt.State.FAILED_STEP -> InternalExchangeStepAttempt.State.FAILED_STEP
  }
}

fun Iterable<ExchangeStepAttempt.DebugLogEntry>.toInternal():
  Iterable<ExchangeStepAttemptDetails.DebugLog> {
  return map { apiProto ->
    ExchangeStepAttemptDetailsKt.debugLog {
      time = apiProto.entryTime
      message = apiProto.message
    }
  }
}

fun InternalExchangeStepAttempt.toExchangeStepAttempt(): ExchangeStepAttempt {
  val key =
    CanonicalExchangeStepAttemptKey(
      recurringExchangeId = externalIdToApiId(externalRecurringExchangeId),
      exchangeId = date.toLocalDate().toString(),
      exchangeStepId = stepIndex.toString(),
      exchangeStepAttemptId = this@toExchangeStepAttempt.attemptNumber.toString(),
    )
  val source = this
  return exchangeStepAttempt {
    name = key.toName()
    attemptNumber = source.attemptNumber
    state = source.state.toState()
    debugLogEntries += source.details.debugLogEntriesList.map { it.toDebugLogEntry() }
    startTime = source.details.startTime
    updateTime = source.details.updateTime
  }
}

fun ExchangeStep.State.toInternal(): InternalExchangeStep.State {
  return when (this) {
    ExchangeStep.State.BLOCKED -> InternalExchangeStep.State.BLOCKED
    ExchangeStep.State.READY -> InternalExchangeStep.State.READY
    ExchangeStep.State.READY_FOR_RETRY -> InternalExchangeStep.State.READY_FOR_RETRY
    ExchangeStep.State.IN_PROGRESS -> InternalExchangeStep.State.IN_PROGRESS
    ExchangeStep.State.SUCCEEDED -> InternalExchangeStep.State.SUCCEEDED
    ExchangeStep.State.FAILED -> InternalExchangeStep.State.FAILED
    ExchangeStep.State.STATE_UNSPECIFIED,
    ExchangeStep.State.UNRECOGNIZED -> error("Invalid ExchangeStep state: $this")
  }
}

private fun ExchangeStepAttemptDetails.DebugLog.toDebugLogEntry():
  ExchangeStepAttempt.DebugLogEntry {
  val source = this
  return ExchangeStepAttemptKt.debugLogEntry {
    entryTime = source.time
    message = source.message
  }
}

private fun InternalExchangeStepAttempt.State.toState(): ExchangeStepAttempt.State {
  return when (this) {
    InternalExchangeStepAttempt.State.STATE_UNSPECIFIED,
    InternalExchangeStepAttempt.State.UNRECOGNIZED ->
      error("Invalid InternalExchangeStepAttempt state: $this")
    InternalExchangeStepAttempt.State.ACTIVE -> ExchangeStepAttempt.State.ACTIVE
    InternalExchangeStepAttempt.State.SUCCEEDED -> ExchangeStepAttempt.State.SUCCEEDED
    InternalExchangeStepAttempt.State.FAILED -> ExchangeStepAttempt.State.FAILED
    InternalExchangeStepAttempt.State.FAILED_STEP -> ExchangeStepAttempt.State.FAILED_STEP
  }
}

private fun InternalExchangeStep.State.toState(): ExchangeStep.State {
  return when (this) {
    InternalExchangeStep.State.BLOCKED -> ExchangeStep.State.BLOCKED
    InternalExchangeStep.State.READY -> ExchangeStep.State.READY
    InternalExchangeStep.State.READY_FOR_RETRY -> ExchangeStep.State.READY_FOR_RETRY
    InternalExchangeStep.State.IN_PROGRESS -> ExchangeStep.State.IN_PROGRESS
    InternalExchangeStep.State.SUCCEEDED -> ExchangeStep.State.SUCCEEDED
    InternalExchangeStep.State.FAILED -> ExchangeStep.State.FAILED
    InternalExchangeStep.State.STATE_UNSPECIFIED,
    InternalExchangeStep.State.UNRECOGNIZED -> error("Invalid InternalExchangeStep state: $this")
  }
}

/** @throws [IllegalArgumentException] if step dependencies invalid */
fun ExchangeWorkflow.toInternal(): InternalExchangeWorkflow {
  val labelsMap = mutableMapOf<String, MutableSet<Pair<String, Int>>>()
  for ((index, step) in stepsList.withIndex()) {
    for (outputLabel in step.outputLabelsMap.values) {
      labelsMap.getOrPut(outputLabel) { mutableSetOf() }.add(step.stepId to index)
    }
  }
  val internalSteps =
    stepsList.mapIndexed { index, step ->
      ExchangeWorkflowKt.step {
        stepIndex = index
        party = step.party.toInternal()
        prerequisiteStepIndices +=
          step.inputLabelsMap.values
            .flatMap { value ->
              val prerequisites = labelsMap.getOrDefault(value, emptyList())
              prerequisites.forEach { (stepId, stepIndex) ->
                require(step.hasCopyFromPreviousExchangeStep() || stepIndex < index) {
                  "Step ${step.stepId} with index $index cannot depend on step $stepId with" +
                    " index $stepIndex. To depend on another step, the index must be greater than" +
                    " the prerequisite step"
                }
              }
              prerequisites.map { it.second }
            }
            .filter { it < index }
            .toSet()
      }
    }

  return exchangeWorkflow { steps += internalSteps }
}

/** @throws [IllegalArgumentException] if Provider not specified */
fun ExchangeWorkflow.Party.toInternal(): InternalExchangeWorkflow.Party {
  return when (this) {
    ExchangeWorkflow.Party.DATA_PROVIDER -> InternalExchangeWorkflow.Party.DATA_PROVIDER
    ExchangeWorkflow.Party.MODEL_PROVIDER -> InternalExchangeWorkflow.Party.MODEL_PROVIDER
    ExchangeWorkflow.Party.PARTY_UNSPECIFIED,
    ExchangeWorkflow.Party.UNRECOGNIZED ->
      throw IllegalArgumentException("Provider is not set for the Exchange Step.")
  }
}

fun InternalEventGroup.State.toV2Alpha(): EventGroup.State {
  return when (this) {
    InternalEventGroup.State.STATE_UNSPECIFIED -> EventGroup.State.STATE_UNSPECIFIED
    InternalEventGroup.State.ACTIVE -> EventGroup.State.ACTIVE
    InternalEventGroup.State.DELETED -> EventGroup.State.DELETED
    InternalEventGroup.State.UNRECOGNIZED -> error("Invalid InternalEventGroup state: $this")
  }
}

/** Converts an internal [InternalRequisition.State] to a public [Requisition.State]. */
fun InternalRequisition.State.toRequisitionState(): Requisition.State =
  when (this) {
    InternalRequisition.State.PENDING_PARAMS,
    InternalRequisition.State.UNFULFILLED -> Requisition.State.UNFULFILLED
    InternalRequisition.State.FULFILLED -> Requisition.State.FULFILLED
    InternalRequisition.State.REFUSED -> Requisition.State.REFUSED
    InternalRequisition.State.WITHDRAWN -> Requisition.State.WITHDRAWN
    InternalRequisition.State.STATE_UNSPECIFIED,
    InternalRequisition.State.UNRECOGNIZED -> Requisition.State.STATE_UNSPECIFIED
  }

/** Converts an internal [InternalAccount.ActivationState] to a public [Account.ActivationState]. */
fun InternalAccount.ActivationState.toActivationState(): Account.ActivationState =
  when (this) {
    InternalAccount.ActivationState.ACTIVATED -> Account.ActivationState.ACTIVATED
    InternalAccount.ActivationState.UNACTIVATED -> Account.ActivationState.UNACTIVATED
    InternalAccount.ActivationState.UNRECOGNIZED,
    InternalAccount.ActivationState.ACTIVATION_STATE_UNSPECIFIED ->
      Account.ActivationState.ACTIVATION_STATE_UNSPECIFIED
  }

/**
 * Converts an internal [InternalCertificate.RevocationState] to a public
 * [Certificate.RevocationState].
 */
fun InternalCertificate.RevocationState.toRevocationState(): Certificate.RevocationState =
  when (this) {
    InternalCertificate.RevocationState.REVOKED -> Certificate.RevocationState.REVOKED
    InternalCertificate.RevocationState.HOLD -> Certificate.RevocationState.HOLD
    InternalCertificate.RevocationState.UNRECOGNIZED,
    InternalCertificate.RevocationState.REVOCATION_STATE_UNSPECIFIED ->
      Certificate.RevocationState.REVOCATION_STATE_UNSPECIFIED
  }
