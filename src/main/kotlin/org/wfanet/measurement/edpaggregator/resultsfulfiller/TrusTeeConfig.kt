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

import com.google.crypto.tink.KmsClient
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.trustee.FulfillRequisitionRequestBuilder as TrusteeFulfillRequisitionRequestBuilder

/**
 * Configuration for TrusTee protocol envelope encryption.
 *
 * This class holds the configuration needed to build TrusTee encryption parameters
 * at requisition fulfillment time.
 *
 * @property kmsClient The Tink KMS client for key management operations.
 * @property kekUriToKeyNameMap Map of input KEK URIs to output key names for re-encryption.
 *   The first key in this map is used as the default input KEK URI.
 * @property workloadIdentityProvider The workload identity provider URL for GCP WIF.
 * @property impersonatedServiceAccount The service account to impersonate for KMS operations.
 */
data class TrusTeeConfig(
  val kmsClient: KmsClient,
  val kekUriToKeyNameMap: Map<String, String>,
  val workloadIdentityProvider: String,
  val impersonatedServiceAccount: String,
) {
  /**
   * Builds EncryptionParams for the TrusTee protocol using the default KEK URI.
   *
   * The default KEK URI is the first key in [kekUriToKeyNameMap]. If the map is empty,
   * returns null.
   *
   * TODO(world-federation-of-advertisers/cross-media-measurement#XXX): Implement dynamic
   * KEK URI selection based on BlobDetails.encryptedDek.kekUri. This requires architectural
   * changes to pass BlobDetails to the FulfillerSelector.
   *
   * @return EncryptionParams for the TrusTee fulfillment request builder, or null if
   *   no KEK URI mapping is configured.
   */
  fun buildEncryptionParams(): TrusteeFulfillRequisitionRequestBuilder.EncryptionParams? {
    val inputKekUri = kekUriToKeyNameMap.keys.firstOrNull() ?: return null
    return buildEncryptionParams(inputKekUri)
  }

  /**
   * Builds EncryptionParams for the TrusTee protocol.
   *
   * @param inputKekUri The KEK URI from BlobDetails.encryptedDek that was used to encrypt the
   *   input data.
   * @return EncryptionParams for the TrusTee fulfillment request builder.
   */
  fun buildEncryptionParams(
    inputKekUri: String
  ): TrusteeFulfillRequisitionRequestBuilder.EncryptionParams {
    val outputKekUri = mapKekUri(inputKekUri)
    return TrusteeFulfillRequisitionRequestBuilder.EncryptionParams(
      kmsClient = kmsClient,
      kmsKekUri = outputKekUri,
      workloadIdentityProvider = workloadIdentityProvider,
      impersonatedServiceAccount = impersonatedServiceAccount,
    )
  }

  /**
   * Maps an input KEK URI to an output KEK URI by replacing the key name.
   *
   * If the input KEK URI is found in [kekUriToKeyNameMap], the key name in the URI is replaced
   * with the mapped value. Otherwise, the original URI is returned unchanged.
   *
   * @param inputKekUri The KEK URI to map (e.g.,
   *   "gcp-kms://projects/my-project/locations/global/keyRings/my-ring/cryptoKeys/input-key").
   * @return The mapped KEK URI with the key name replaced, or the original URI if no mapping exists.
   */
  private fun mapKekUri(inputKekUri: String): String {
    val mappedKeyName = kekUriToKeyNameMap[inputKekUri] ?: return inputKekUri

    // Replace the key name in the URI
    // URI format: gcp-kms://projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys/{keyName}
    val cryptoKeysPrefix = "/cryptoKeys/"
    val prefixIndex = inputKekUri.lastIndexOf(cryptoKeysPrefix)
    if (prefixIndex == -1) {
      return inputKekUri
    }

    val baseUri = inputKekUri.substring(0, prefixIndex + cryptoKeysPrefix.length)
    return baseUri + mappedKeyName
  }
}
