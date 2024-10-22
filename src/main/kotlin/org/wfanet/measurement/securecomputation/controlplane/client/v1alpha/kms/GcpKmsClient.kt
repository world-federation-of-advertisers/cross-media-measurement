// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.controlplane.client.v1alpha.kms

import com.google.cloud.kms.v1.CryptoKeyName
import com.google.cloud.kms.v1.KeyManagementServiceClient
import com.google.cloud.kms.v1.DecryptRequest
import com.google.protobuf.ByteString
import java.util.Base64

data class GcpKmsDecryptionParams(
  val projectId: String,
  val locationId: String,
  val keyRingId: String,
  val keyId: String,
  val encryptedKeyBase64: String
) : KmsDecryptionParams()

/**
 * A Google Cloud KMS client that implements the [KmsClient] interface.
 * This class provides functionality to decrypt an encrypted Data Encryption Key (DEK) using an
 * asymmetric Key Encryption Key (KEK) stored in Google Cloud KMS.
 *
 * The client utilizes Google Cloud's KMS API to handle the decryption process.
 */
class GcpKmsClient(private val kmsClient: KeyManagementServiceClient) : KmsClient {

  /**
   * Decrypts an encrypted symmetric key (DEK) using Google Cloud KMS.
   * The method requires the [GcpKmsDecryptionParams] to specify the GCP project, location,
   * key ring, key ID, and the encrypted symmetric key in Base64 format.
   *
   * @param params The parameters required for the decryption operation, specifically
   *               [GcpKmsDecryptionParams] for Google Cloud KMS.
   *
   * @return The decrypted symmetric key as a [ByteArray].
   *
   * @throws IllegalArgumentException If the provided parameters are not of type [GcpKmsDecryptionParams].
   * @throws Exception If an error occurs during the decryption process.
   */
  override fun decryptKey(params: KmsDecryptionParams): ByteArray {
    val gcpParams = params as GcpKmsDecryptionParams
    val cryptoKeyName = CryptoKeyName.of(
      gcpParams.projectId,
      gcpParams.locationId,
      gcpParams.keyRingId,
      gcpParams.keyId
    )
    val encryptedDekBytes = Base64.getDecoder().decode(gcpParams.encryptedKeyBase64)
    val decryptRequest = DecryptRequest.newBuilder()
      .setName(cryptoKeyName.toString())
      .setCiphertext(ByteString.copyFrom(encryptedDekBytes))
      .build()
    val decryptResponse = kmsClient.decrypt(decryptRequest)
    return decryptResponse.plaintext.toByteArray()
  }
}
