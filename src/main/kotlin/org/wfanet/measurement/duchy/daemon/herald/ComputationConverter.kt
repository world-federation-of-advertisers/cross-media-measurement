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
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey as v2AlphaEncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.HybridCipherSuite as v2AlphaHybridCipherSuite
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.duchy.daemon.utils.PublicApiVersion
import org.wfanet.measurement.duchy.daemon.utils.toPublicApiVersion
import org.wfanet.measurement.internal.duchy.ComputationDetails.KingdomComputationDetails
import org.wfanet.measurement.internal.duchy.EncryptionPublicKey
import org.wfanet.measurement.internal.duchy.HybridCipherSuite
import org.wfanet.measurement.system.v1alpha.Computation

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
      v2AlphaEncryptionPublicKey.parseFrom(this).toDuchyEncryptionPublicKey()
  }
}

/** Converts a v2alpha Public API EncryptionPublicKey to duchy internal EncryptionPublicKey. */
fun v2AlphaEncryptionPublicKey.toDuchyEncryptionPublicKey(): EncryptionPublicKey {
  return EncryptionPublicKey.newBuilder()
    .also {
      it.type =
        when (this.type) {
          v2AlphaEncryptionPublicKey.Type.TYPE_UNSPECIFIED ->
            EncryptionPublicKey.Type.TYPE_UNSPECIFIED
          v2AlphaEncryptionPublicKey.Type.EC_P256 -> EncryptionPublicKey.Type.EC_P256
          v2AlphaEncryptionPublicKey.Type.UNRECOGNIZED, null -> error("unrecognized type.")
        }
      it.publicKeyInfo = publicKeyInfo
    }
    .build()
}

/** Converts a v2alpha Public API HybridCipherSuite to duchy internal HybridCipherSuite. */
fun v2AlphaHybridCipherSuite.toDuchyHybridCipherSuite(): HybridCipherSuite {
  return HybridCipherSuite.newBuilder()
    .also {
      it.kem =
        when (this.kem) {
          v2AlphaHybridCipherSuite
            .KeyEncapsulationMechanism
            .KEY_ENCAPSULATION_MECHANISM_UNSPECIFIED ->
            HybridCipherSuite.KeyEncapsulationMechanism.KEY_ENCAPSULATION_MECHANISM_UNSPECIFIED
          v2AlphaHybridCipherSuite.KeyEncapsulationMechanism.ECDH_P256_HKDF_HMAC_SHA256 ->
            HybridCipherSuite.KeyEncapsulationMechanism.ECDH_P256_HKDF_HMAC_SHA256
          v2AlphaHybridCipherSuite.KeyEncapsulationMechanism.UNRECOGNIZED, null ->
            error("unrecognized KeyEncapsulationMechanism.")
        }
      it.dem =
        when (this.dem) {
          v2AlphaHybridCipherSuite
            .DataEncapsulationMechanism
            .DATA_ENCAPSULATION_MECHANISM_UNSPECIFIED ->
            HybridCipherSuite.DataEncapsulationMechanism.DATA_ENCAPSULATION_MECHANISM_UNSPECIFIED
          v2AlphaHybridCipherSuite.DataEncapsulationMechanism.AES_128_GCM ->
            HybridCipherSuite.DataEncapsulationMechanism.AES_128_GCM
          v2AlphaHybridCipherSuite.DataEncapsulationMechanism.UNRECOGNIZED, null ->
            error("unrecognized DataEncapsulationMechanism.")
        }
    }
    .build()
}
