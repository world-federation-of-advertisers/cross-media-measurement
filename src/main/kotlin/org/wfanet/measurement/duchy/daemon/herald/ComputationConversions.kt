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

package org.wfanet.measurement.duchy.daemon.herald

import com.google.protobuf.ByteString
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams as V2AlphaDifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey as V2AlphaEncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.HybridCipherSuite as V2AlphaHybridCipherSuite
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.duchy.daemon.utils.PublicApiVersion
import org.wfanet.measurement.duchy.daemon.utils.toPublicApiVersion
import org.wfanet.measurement.internal.duchy.ComputationDetails.KingdomComputationDetails
import org.wfanet.measurement.internal.duchy.DifferentialPrivacyParams
import org.wfanet.measurement.internal.duchy.EncryptionPublicKey
import org.wfanet.measurement.internal.duchy.HybridCipherSuite
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.DifferentialPrivacyParams as SystemDifferentialPrivacyParams

/** Creates a KingdomComputationDetails from the kingdom system API Computation. */
fun Computation.toKingdomComputationDetails(): KingdomComputationDetails {
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
            measurementSpec.measurementPublicKey.toDuchyEncryptionPublicKey(publicApiVersion)
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
fun ByteString.toDuchyEncryptionPublicKey(publicApiVersion: String): EncryptionPublicKey {
  return when (publicApiVersion.toPublicApiVersion()) {
    PublicApiVersion.V2_ALPHA ->
      V2AlphaEncryptionPublicKey.parseFrom(this).toDuchyEncryptionPublicKey()
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
