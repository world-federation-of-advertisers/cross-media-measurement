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
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams as V2AlphaDifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey as V2AlphaElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey as V2AlphaEncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.HybridCipherSuite as V2AlphaHybridCipherSuite
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.MeasurementTypeCase
import org.wfanet.measurement.internal.duchy.ComputationDetails.KingdomComputationDetails
import org.wfanet.measurement.internal.duchy.DifferentialPrivacyParams
import org.wfanet.measurement.internal.duchy.ElGamalPublicKey
import org.wfanet.measurement.internal.duchy.EncryptionPublicKey
import org.wfanet.measurement.internal.duchy.HybridCipherSuite
import org.wfanet.measurement.internal.duchy.RequisitionDetails
import org.wfanet.measurement.system.v1alpha.Computation as SystemComputation
import org.wfanet.measurement.system.v1alpha.DifferentialPrivacyParams as SystemDifferentialPrivacyParams
import org.wfanet.measurement.system.v1alpha.Requisition as SystemRequisition

/** Supported measurement types in the duchy. */
enum class MeasurementType {
  REACH_AND_FREQUENCY
}

/** Gets the measurement type from the system computation. */
fun SystemComputation.toMeasurementType(): MeasurementType {
  return when (publicApiVersion.toPublicApiVersion()) {
    PublicApiVersion.V2_ALPHA -> {
      val v2AlphaMeasurementSpec = MeasurementSpec.parseFrom(measurementSpec)
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (v2AlphaMeasurementSpec.measurementTypeCase) {
        MeasurementTypeCase.REACH_AND_FREQUENCY -> MeasurementType.REACH_AND_FREQUENCY
        MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET -> error("Measurement type not set.")
      }
    }
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
      when (publicApiVersion.toPublicApiVersion()) {
        PublicApiVersion.V2_ALPHA -> {
          val measurementSpec = MeasurementSpec.parseFrom(measurementSpec)
          it.measurementPublicKey =
            measurementSpec.measurementPublicKey.toDuchyEncryptionPublicKey(
              publicApiVersion.toPublicApiVersion()
            )
          it.cipherSuite = measurementSpec.cipherSuite.toDuchyHybridCipherSuite()
        }
      }
    }
    .build()
}

/**
 * Parses a serialized Public API EncryptionPublicKey and converts to duchy internal
 * EncryptionPublicKey.
 */
fun ByteString.toDuchyEncryptionPublicKey(publicApiVersion: PublicApiVersion): EncryptionPublicKey {
  return when (publicApiVersion) {
    PublicApiVersion.V2_ALPHA ->
      V2AlphaEncryptionPublicKey.parseFrom(this).toDuchyEncryptionPublicKey()
  }
}

/**
 * Parses a serialized Public API ElGamalPublicKey and converts to duchy internal ElGamalPublicKey.
 */
fun ByteString.toDuchyElGamalPublicKey(publicApiVersion: PublicApiVersion): ElGamalPublicKey {
  return when (publicApiVersion) {
    PublicApiVersion.V2_ALPHA -> V2AlphaElGamalPublicKey.parseFrom(this).toDuchyElGamalPublicKey()
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

/** Converts a system API DifferentialPrivacyParams to duchy internal DifferentialPrivacyParams. */
fun SystemRequisition.toDuchyRequisitionDetails(): RequisitionDetails {
  return RequisitionDetails.newBuilder()
    .also {
      it.dataProviderCertificate = dataProviderCertificate
      it.requisitionSpecHash = requisitionSpecHash
      it.dataProviderParticipationSignature = dataProviderParticipationSignature
      it.externalFulfillingDuchyId = fulfillingComputationParticipant.duchyId
    }
    .build()
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
fun ElGamalPublicKey.toPublicApiElGamalPublicKeyBytes(
  publicApiVersion: PublicApiVersion
): ByteString {
  return when (publicApiVersion) {
    PublicApiVersion.V2_ALPHA ->
      V2AlphaElGamalPublicKey.newBuilder()
        .also {
          it.generator = generator
          it.element = element
        }
        .build()
        .toByteString()
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
