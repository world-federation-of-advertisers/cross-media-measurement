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
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams as V2AlphaDifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey as V2AlphaElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey as V2AlphaEncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.HybridCipherSuite as V2AlphaHybridCipherSuite
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.MeasurementTypeCase
import org.wfanet.measurement.internal.duchy.ComputationDetails.KingdomComputationDetails
import org.wfanet.measurement.internal.duchy.DifferentialPrivacyParams
import org.wfanet.measurement.internal.duchy.ElGamalKeyPair
import org.wfanet.measurement.internal.duchy.ElGamalPublicKey
import org.wfanet.measurement.internal.duchy.EncryptionPublicKey
import org.wfanet.measurement.internal.duchy.HybridCipherSuite
import org.wfanet.measurement.internal.duchy.RequisitionDetails
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
        MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET -> error("Measurement type not set.")
      }
    }
    Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
  }
}

/** Creates a KingdomComputationDetails from the kingdom system API Computation. */
fun SystemComputation.toKingdomComputationDetails(): KingdomComputationDetails {
  return KingdomComputationDetails.newBuilder()
    .also {
      it.publicApiVersion = publicApiVersion
      it.measurementSpec = measurementSpec
      it.dataProviderList = dataProviderList
      it.dataProviderListSalt = dataProviderListSalt
      when (Version.fromString(publicApiVersion)) {
        Version.V2_ALPHA -> {
          val measurementSpec = MeasurementSpec.parseFrom(measurementSpec)
          it.measurementPublicKey =
            measurementSpec.measurementPublicKey.toDuchyEncryptionPublicKey(
              Version.fromString(publicApiVersion)
            )
          it.cipherSuite = measurementSpec.cipherSuite.toDuchyHybridCipherSuite()
        }
        Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
      }
    }
    .build()
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

/** Converts a v2alpha Public API EncryptionPublicKey to duchy internal EncryptionPublicKey. */
fun V2AlphaEncryptionPublicKey.toDuchyEncryptionPublicKey(): EncryptionPublicKey {
  return EncryptionPublicKey.newBuilder()
    .also {
      it.type =
        when (this.type) {
          V2AlphaEncryptionPublicKey.Type.TYPE_UNSPECIFIED ->
            EncryptionPublicKey.Type.TYPE_UNSPECIFIED
          V2AlphaEncryptionPublicKey.Type.EC_P256 -> EncryptionPublicKey.Type.EC_P256
          V2AlphaEncryptionPublicKey.Type.UNRECOGNIZED, null -> error("unrecognized type.")
        }
      it.publicKeyInfo = publicKeyInfo
    }
    .build()
}

/** Converts a v2alpha Public API HybridCipherSuite to duchy internal HybridCipherSuite. */
fun V2AlphaHybridCipherSuite.toDuchyHybridCipherSuite(): HybridCipherSuite {
  return HybridCipherSuite.newBuilder()
    .also {
      it.kem =
        when (this.kem) {
          V2AlphaHybridCipherSuite.KeyEncapsulationMechanism
            .KEY_ENCAPSULATION_MECHANISM_UNSPECIFIED ->
            HybridCipherSuite.KeyEncapsulationMechanism.KEY_ENCAPSULATION_MECHANISM_UNSPECIFIED
          V2AlphaHybridCipherSuite.KeyEncapsulationMechanism.ECDH_P256_HKDF_HMAC_SHA256 ->
            HybridCipherSuite.KeyEncapsulationMechanism.ECDH_P256_HKDF_HMAC_SHA256
          V2AlphaHybridCipherSuite.KeyEncapsulationMechanism.UNRECOGNIZED, null ->
            error("unrecognized KeyEncapsulationMechanism.")
        }
      it.dem =
        when (this.dem) {
          V2AlphaHybridCipherSuite.DataEncapsulationMechanism
            .DATA_ENCAPSULATION_MECHANISM_UNSPECIFIED ->
            HybridCipherSuite.DataEncapsulationMechanism.DATA_ENCAPSULATION_MECHANISM_UNSPECIFIED
          V2AlphaHybridCipherSuite.DataEncapsulationMechanism.AES_128_GCM ->
            HybridCipherSuite.DataEncapsulationMechanism.AES_128_GCM
          V2AlphaHybridCipherSuite.DataEncapsulationMechanism.UNRECOGNIZED, null ->
            error("unrecognized DataEncapsulationMechanism.")
        }
    }
    .build()
}

/** Converts a system API DifferentialPrivacyParams to duchy internal DifferentialPrivacyParams. */
fun SystemDifferentialPrivacyParams.toDuchyDifferentialPrivacyParams(): DifferentialPrivacyParams {
  return DifferentialPrivacyParams.newBuilder()
    .also {
      it.epsilon = epsilon
      it.delta = delta
    }
    .build()
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

/** Converts a system API DifferentialPrivacyParams to duchy internal DifferentialPrivacyParams. */
fun SystemRequisition.toDuchyRequisitionDetails(): RequisitionDetails {
  return RequisitionDetails.newBuilder()
    .also {
      it.dataProviderCertificate = dataProviderCertificate
      it.requisitionSpecHash = requisitionSpecHash
      it.dataProviderParticipationSignature = dataProviderParticipationSignature
      if (fulfillingComputationParticipant.isNotEmpty()) {
        it.externalFulfillingDuchyId =
          checkNotNull(ComputationParticipantKey.fromName(fulfillingComputationParticipant)).duchyId
      }
    }
    .build()
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
  return DifferentialPrivacyParams.newBuilder()
    .also {
      it.epsilon = epsilon
      it.delta = delta
    }
    .build()
}

/** Converts a duchy internal ElGamalPublicKey to the corresponding public API ElGamalPublicKey. */
fun ElGamalPublicKey.toPublicApiElGamalPublicKeyBytes(publicApiVersion: Version): ByteString {
  return when (publicApiVersion) {
    Version.V2_ALPHA ->
      V2AlphaElGamalPublicKey.newBuilder()
        .also {
          it.generator = generator
          it.element = element
        }
        .build()
        .toByteString()
    Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
  }
}

/** Converts a v2alpha Public API ElGamalPublicKey to duchy internal ElGamalPublicKey. */
fun V2AlphaElGamalPublicKey.toDuchyElGamalPublicKey(): ElGamalPublicKey {
  return ElGamalPublicKey.newBuilder()
    .also {
      it.generator = generator
      it.element = element
    }
    .build()
}

/** The result and frequency estimation of a computation. */
data class ReachAndFrequency(val reach: Long, val frequency: Map<Long, Double>)

/** Converts a ReachAndFrequency object to the corresponding public API measurement result. */
fun ReachAndFrequency.toPublicApiMeasurementResult(publicApiVersion: Version): ByteString {
  return when (publicApiVersion) {
    Version.V2_ALPHA ->
      Measurement.Result.newBuilder()
        .also {
          it.reachBuilder.value = reach
          it.frequencyBuilder.putAllRelativeFrequencyDistribution(frequency)
        }
        .build()
        .toByteString()
    Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
  }
}

fun org.wfanet.anysketch.crypto.ElGamalPublicKey.toCmmsElGamalPublicKey(): ElGamalPublicKey {
  return ElGamalPublicKey.newBuilder()
    .also {
      it.generator = generator
      it.element = element
    }
    .build()
}

fun ElGamalPublicKey.toAnySketchElGamalPublicKey(): org.wfanet.anysketch.crypto.ElGamalPublicKey {
  return org.wfanet.anysketch.crypto.ElGamalPublicKey.newBuilder()
    .also {
      it.generator = generator
      it.element = element
    }
    .build()
}

fun ElGamalKeyPair.toAnySketchElGamalKeyPair(): org.wfanet.anysketch.crypto.ElGamalKeyPair {
  return org.wfanet.anysketch.crypto.ElGamalKeyPair.newBuilder()
    .also {
      it.publicKey = publicKey.toAnySketchElGamalPublicKey()
      it.secretKey = secretKey
    }
    .build()
}

/** Extracts the DataProviderId from the DataProvider public Api resource name. */
fun extractDataProviderId(publicApiVersion: Version, resourceName: String): String {
  return when (publicApiVersion) {
    Version.V2_ALPHA -> DataProviderKey.fromName(resourceName)?.dataProviderId
        ?: error("Resource name unspecified or invalid.")
    Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
  }
}
