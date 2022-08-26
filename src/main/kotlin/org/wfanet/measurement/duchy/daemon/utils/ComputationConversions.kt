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

package org.wfanet.measurement.duchy.daemon.utils

import com.google.protobuf.ByteString
import java.lang.IllegalArgumentException
import org.wfanet.anysketch.crypto.ElGamalKeyPair as AnySketchElGamalKeyPair
import org.wfanet.anysketch.crypto.ElGamalPublicKey as AnySketchElGamalPublicKey
import org.wfanet.anysketch.crypto.elGamalKeyPair as anySketchElGamalKeyPair
import org.wfanet.anysketch.crypto.elGamalPublicKey as anySketchElGamalPublicKey
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams as V2AlphaDifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey as V2AlphaElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey as V2AlphaEncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.frequency
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.MeasurementTypeCase
import org.wfanet.measurement.api.v2alpha.elGamalPublicKey as v2AlphaElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey as v2alphaEncryptionPublicKey
import org.wfanet.measurement.consent.client.duchy.computeRequisitionFingerprint
import org.wfanet.measurement.internal.duchy.ComputationDetails.KingdomComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationDetailsKt.kingdomComputationDetails
import org.wfanet.measurement.internal.duchy.DifferentialPrivacyParams
import org.wfanet.measurement.internal.duchy.ElGamalKeyPair
import org.wfanet.measurement.internal.duchy.ElGamalPublicKey
import org.wfanet.measurement.internal.duchy.EncryptionPublicKey
import org.wfanet.measurement.internal.duchy.RequisitionDetails
import org.wfanet.measurement.internal.duchy.RequisitionEntry
import org.wfanet.measurement.internal.duchy.differentialPrivacyParams
import org.wfanet.measurement.internal.duchy.elGamalPublicKey
import org.wfanet.measurement.internal.duchy.encryptionPublicKey
import org.wfanet.measurement.internal.duchy.externalRequisitionKey
import org.wfanet.measurement.internal.duchy.requisitionDetails
import org.wfanet.measurement.internal.duchy.requisitionEntry
import org.wfanet.measurement.system.v1alpha.Computation as SystemComputation
import org.wfanet.measurement.system.v1alpha.ComputationKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipant
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.DifferentialPrivacyParams as SystemDifferentialPrivacyParams
import org.wfanet.measurement.system.v1alpha.Requisition as SystemRequisition
import org.wfanet.measurement.system.v1alpha.RequisitionKey

/** Supported measurement types in the duchy. */
enum class MeasurementType {
  REACH_AND_FREQUENCY
}

/** Gets the measurement type from the system computation. */
fun SystemComputation.toMeasurementType(): MeasurementType {
  return when (Version.fromString(publicApiVersion)) {
    Version.V2_ALPHA -> {
      val v2AlphaMeasurementSpec = MeasurementSpec.parseFrom(measurementSpec)
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (v2AlphaMeasurementSpec.measurementTypeCase) {
        MeasurementTypeCase.REACH_AND_FREQUENCY -> MeasurementType.REACH_AND_FREQUENCY
        MeasurementTypeCase.DURATION,
        MeasurementTypeCase.IMPRESSION ->
          error("Unexpected MeasurementType for system API Computation.")
        MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET -> error("Measurement type not set.")
      }
    }
    Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
  }
}

/** Creates a KingdomComputationDetails from the kingdom system API Computation. */
fun SystemComputation.toKingdomComputationDetails(): KingdomComputationDetails {
  val source = this
  return kingdomComputationDetails {
    publicApiVersion = source.publicApiVersion
    measurementSpec = source.measurementSpec
    when (Version.fromString(source.publicApiVersion)) {
      Version.V2_ALPHA -> {
        val measurementSpec = MeasurementSpec.parseFrom(source.measurementSpec)
        measurementPublicKey =
          measurementSpec.measurementPublicKey.toDuchyEncryptionPublicKey(
            Version.fromString(source.publicApiVersion)
          )
      }
      Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
    }
  }
}

/** Resource key. */
val SystemComputation.key: ComputationKey
  get() {
    return if (name.isEmpty()) {
      ComputationKey.defaultValue
    } else {
      checkNotNull(ComputationKey.fromName(name)) { "Invalid resource name $name" }
    }
  }

/**
 * Parses a serialized Public API EncryptionPublicKey and converts to duchy internal
 * EncryptionPublicKey.
 */
fun ByteString.toDuchyEncryptionPublicKey(publicApiVersion: Version): EncryptionPublicKey {
  return when (publicApiVersion) {
    Version.V2_ALPHA -> V2AlphaEncryptionPublicKey.parseFrom(this).toDuchyEncryptionPublicKey()
    Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
  }
}

/**
 * Parses a serialized Public API ElGamalPublicKey and converts to duchy internal ElGamalPublicKey.
 */
fun ByteString.toDuchyElGamalPublicKey(publicApiVersion: Version): ElGamalPublicKey {
  return when (publicApiVersion) {
    Version.V2_ALPHA -> V2AlphaElGamalPublicKey.parseFrom(this).toDuchyElGamalPublicKey()
    Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
  }
}

/** Converts a v2alpha Public API EncryptionPublicKey to duchy internal [EncryptionPublicKey]. */
fun V2AlphaEncryptionPublicKey.toDuchyEncryptionPublicKey(): EncryptionPublicKey {
  val source = this
  return encryptionPublicKey {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    format =
      when (source.format) {
        V2AlphaEncryptionPublicKey.Format.TINK_KEYSET -> EncryptionPublicKey.Format.TINK_KEYSET
        V2AlphaEncryptionPublicKey.Format.FORMAT_UNSPECIFIED ->
          EncryptionPublicKey.Format.FORMAT_UNSPECIFIED
        V2AlphaEncryptionPublicKey.Format.UNRECOGNIZED ->
          throw IllegalArgumentException("Invalid Format: ${source.format}")
      }

    data = source.data
  }
}

/** Converts a duchy internal [EncryptionPublicKey] to v2alpha Public API EncryptionPublicKey. */
fun EncryptionPublicKey.toV2AlphaEncryptionPublicKey(): V2AlphaEncryptionPublicKey {
  val source = this
  return v2alphaEncryptionPublicKey {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    format =
      when (source.format) {
        EncryptionPublicKey.Format.TINK_KEYSET -> V2AlphaEncryptionPublicKey.Format.TINK_KEYSET
        EncryptionPublicKey.Format.FORMAT_UNSPECIFIED ->
          V2AlphaEncryptionPublicKey.Format.FORMAT_UNSPECIFIED
        EncryptionPublicKey.Format.UNRECOGNIZED ->
          throw IllegalArgumentException("Invalid Format: ${source.format}")
      }

    data = source.data
  }
}

/** Converts a system API DifferentialPrivacyParams to duchy internal DifferentialPrivacyParams. */
fun SystemDifferentialPrivacyParams.toDuchyDifferentialPrivacyParams(): DifferentialPrivacyParams {
  val source = this
  return differentialPrivacyParams {
    epsilon = source.epsilon
    delta = source.delta
  }
}

/** Resource key. */
val ComputationParticipant.key: ComputationParticipantKey
  get() {
    return if (name.isEmpty()) {
      ComputationParticipantKey.defaultValue
    } else {
      checkNotNull(ComputationParticipantKey.fromName(name)) { "Invalid resource name $name" }
    }
  }

/** Converts this collection of system API Requisitions to Duchy internal [RequisitionDetails]. */
fun Iterable<SystemRequisition>.toRequisitionEntries(
  serializedMeasurementSpec: ByteString
): Iterable<RequisitionEntry> {
  return map {
    requisitionEntry {
      key = externalRequisitionKey {
        externalRequisitionId = it.key.requisitionId
        requisitionFingerprint =
          computeRequisitionFingerprint(serializedMeasurementSpec, it.requisitionSpecHash)
      }
      value = it.toDuchyRequisitionDetails()
    }
  }
}

/** Converts a system API Requisition to duchy internal [RequisitionDetails]. */
fun SystemRequisition.toDuchyRequisitionDetails(): RequisitionDetails {
  val source = this
  return requisitionDetails {
    nonceHash = source.nonceHash
    if (source.fulfillingComputationParticipant.isNotEmpty()) {
      externalFulfillingDuchyId =
        checkNotNull(ComputationParticipantKey.fromName(source.fulfillingComputationParticipant))
          .duchyId
    }
    nonce = source.nonce
  }
}

/** Resource key. */
val SystemRequisition.key: RequisitionKey
  get() {
    return if (name.isEmpty()) {
      RequisitionKey.defaultValue
    } else {
      checkNotNull(RequisitionKey.fromName(name)) { "Invalid resource name $name" }
    }
  }

/**
 * Converts a v2alpha Public API DifferentialPrivacyParams to duchy internal
 * DifferentialPrivacyParams.
 */
fun V2AlphaDifferentialPrivacyParams.toDuchyDifferentialPrivacyParams(): DifferentialPrivacyParams {
  val source = this
  return differentialPrivacyParams {
    epsilon = source.epsilon
    delta = source.delta
  }
}

/** Converts a duchy internal ElGamalPublicKey to the v2alpha public API ElGamalPublicKey. */
fun ElGamalPublicKey.toV2AlphaElGamalPublicKey(): V2AlphaElGamalPublicKey {
  val source = this
  return v2AlphaElGamalPublicKey {
    generator = source.generator
    element = source.element
  }
}

/** Converts a v2alpha Public API ElGamalPublicKey to duchy internal ElGamalPublicKey. */
fun V2AlphaElGamalPublicKey.toDuchyElGamalPublicKey(): ElGamalPublicKey {
  val source = this
  return elGamalPublicKey {
    generator = source.generator
    element = source.element
  }
}

/** The result and frequency estimation of a computation. */
data class ReachAndFrequency(val reach: Long, val frequency: Map<Long, Double>)

/** Converts a ReachAndFrequency object to the v2Alpha measurement result. */
fun ReachAndFrequency.toV2AlphaMeasurementResult(): Measurement.Result {
  val source = this
  return MeasurementKt.result {
    reach = reach { value = source.reach }
    frequency = frequency { relativeFrequencyDistribution.putAll(source.frequency) }
  }
}

fun AnySketchElGamalPublicKey.toCmmsElGamalPublicKey(): ElGamalPublicKey {
  val source = this
  return elGamalPublicKey {
    generator = source.generator
    element = source.element
  }
}

fun ElGamalPublicKey.toAnySketchElGamalPublicKey(): AnySketchElGamalPublicKey {
  val source = this
  return anySketchElGamalPublicKey {
    generator = source.generator
    element = source.element
  }
}

fun ElGamalKeyPair.toAnySketchElGamalKeyPair(): AnySketchElGamalKeyPair {
  val source = this
  return anySketchElGamalKeyPair {
    publicKey = source.publicKey.toAnySketchElGamalPublicKey()
    secretKey = source.secretKey
  }
}
