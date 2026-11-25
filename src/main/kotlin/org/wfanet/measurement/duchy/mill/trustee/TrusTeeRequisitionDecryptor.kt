// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill.trustee

import com.google.crypto.tink.BinaryKeysetReader
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.StreamingAead
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import java.nio.file.Path
import java.security.GeneralSecurityException
import org.wfanet.measurement.common.crypto.tink.ConfidentialSpaceToAwsWifCredentials
import org.wfanet.measurement.common.crypto.tink.GCloudToAwsWifCredentials
import org.wfanet.measurement.common.crypto.tink.GCloudWifCredentials
import org.wfanet.measurement.common.crypto.tink.KmsClientFactory
import org.wfanet.measurement.common.crypto.tink.WifCredentials
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients.PermanentErrorException
import org.wfanet.measurement.internal.duchy.RequisitionDetails.RequisitionProtocol.TrusTee
import org.wfanet.measurement.internal.duchy.RequisitionDetails.RequisitionProtocol.TrusTee.DataFormat

/**
 * Reads the frequency vector of a fulfilled TrusTEE requisition, decrypting it with its envelope
 * encryption parameters where it is encrypted.
 *
 * This restates the decryption of [TrusTeeMill] so that the TrusTEE v2 Mill shares it.
 *
 * TODO(world-federation-of-advertisers/cross-media-measurement#4532): Have [TrusTeeMill] use this
 *   class too, rather than its own copy.
 *
 * @param attestationTokenPath the Confidential Space attestation token presented to the KMS
 */
class TrusTeeRequisitionDecryptor(
  private val gcloudKmsClientFactory: KmsClientFactory<GCloudWifCredentials>,
  private val gcloudToAwsKmsClientFactory: KmsClientFactory<GCloudToAwsWifCredentials>,
  private val confidentialSpaceToAwsKmsClientFactory:
    KmsClientFactory<ConfidentialSpaceToAwsWifCredentials>,
  private val attestationTokenPath: Path,
) {
  /**
   * Cache of [KmsClient] instances by [WifCredentials].
   *
   * Reusing refreshable credentials avoids exhausting Confidential Space attestation quotas.
   */
  private val kmsClientCache =
    object : LinkedHashMap<WifCredentials, KmsClient>(16, 0.75f, true) {
      override fun removeEldestEntry(eldest: MutableMap.MutableEntry<WifCredentials, KmsClient>) =
        size > MAX_CACHED_KMS_CLIENTS
    }

  /**
   * Returns the frequency vector held in [data], the requisition blob, as one byte per position.
   *
   * @param protocol the TrusTEE parameters the `DataProvider` fulfilled the requisition with
   * @throws PermanentErrorException if the vector cannot be decrypted
   * @throws IllegalArgumentException if [protocol] has no supported data format
   */
  fun readFrequencyVector(protocol: TrusTee, data: ByteString): ByteArray {
    return when (protocol.dataFormat) {
      DataFormat.FREQUENCY_VECTOR -> data.toByteArray()
      DataFormat.ENCRYPTED_FREQUENCY_VECTOR -> {
        val dek: KeysetHandle = getDekKeysetHandle(getKmsClient(protocol), protocol)
        decrypt(dek, data)
      }
      DataFormat.DATA_FORMAT_UNSPECIFIED,
      DataFormat.UNRECOGNIZED ->
        throw IllegalArgumentException(
          "Unsupported or unspecified TrusTEE data format ${protocol.dataFormat}"
        )
    }
  }

  private fun getKmsClient(protocol: TrusTee): KmsClient {
    try {
      return if (protocol.hasAwsKmsParams()) {
        val awsConfig = protocol.awsKmsParams
        if (
          awsConfig.credentialSource == TrusTee.AwsKmsParams.CredentialSource.CONFIDENTIAL_SPACE
        ) {
          // Direct Confidential Space -> AWS: present the attestation token to AWS STS.
          val credentials =
            ConfidentialSpaceToAwsWifCredentials(
              roleArn = awsConfig.roleArn,
              roleSessionName = awsConfig.roleSession,
              region = awsConfig.region,
              audience = awsConfig.audience,
            )
          getCachedKmsClient(confidentialSpaceToAwsKmsClientFactory, credentials)
        } else {
          // Two-hop: GCP WIF + service-account impersonation mints the OIDC token for AWS STS.
          val credentials =
            GCloudToAwsWifCredentials(
              gcloudAudience = protocol.workloadIdentityProvider,
              subjectTokenType = OAUTH_TOKEN_TYPE_ID_TOKEN,
              tokenUrl = GOOGLE_STS_TOKEN_URL,
              credentialSourceFilePath = attestationTokenPath.toString(),
              serviceAccountImpersonationUrl =
                IAM_IMPERSONATION_URL_FORMAT.format(protocol.impersonatedServiceAccount),
              roleArn = awsConfig.roleArn,
              roleSessionName = awsConfig.roleSession,
              region = awsConfig.region,
              awsAudience = awsConfig.audience,
            )
          getCachedKmsClient(gcloudToAwsKmsClientFactory, credentials)
        }
      } else {
        val credentials =
          GCloudWifCredentials(
            audience = protocol.workloadIdentityProvider,
            subjectTokenType = OAUTH_TOKEN_TYPE_ID_TOKEN,
            tokenUrl = GOOGLE_STS_TOKEN_URL,
            credentialSourceFilePath = attestationTokenPath.toString(),
            serviceAccountImpersonationUrl =
              IAM_IMPERSONATION_URL_FORMAT.format(protocol.impersonatedServiceAccount),
          )
        getCachedKmsClient(gcloudKmsClientFactory, credentials)
      }
    } catch (e: GeneralSecurityException) {
      throw PermanentErrorException("Failed to create KMS client", e)
    }
  }

  private fun <T : WifCredentials> getCachedKmsClient(
    factory: KmsClientFactory<T>,
    credentials: T,
  ): KmsClient =
    synchronized(kmsClientCache) {
      kmsClientCache.getOrPut(credentials) { factory.getKmsClient(credentials) }
    }

  private fun getDekKeysetHandle(kmsClient: KmsClient, protocol: TrusTee): KeysetHandle {
    try {
      val kekAead = kmsClient.getAead(protocol.kmsKekUri)
      return KeysetHandle.read(
        BinaryKeysetReader.withBytes(protocol.encryptedDekCiphertext.toByteArray()),
        kekAead,
      )
    } catch (e: GeneralSecurityException) {
      throw PermanentErrorException("Failed to get DEK keyset due to a cryptographic error", e)
    }
  }

  private fun decrypt(dek: KeysetHandle, data: ByteString): ByteArray {
    try {
      // The frequency vector is encrypted with no associated data.
      val streamingAead = dek.getPrimitive(StreamingAead::class.java)
      return streamingAead.newDecryptingStream(data.newInput(), byteArrayOf()).use {
        it.readAllBytes()
      }
    } catch (e: GeneralSecurityException) {
      throw PermanentErrorException(
        "Failed to decrypt requisition data due to a cryptographic error",
        e,
      )
    }
  }

  companion object {
    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }

    private const val MAX_CACHED_KMS_CLIENTS = 128

    private const val OAUTH_TOKEN_TYPE_ID_TOKEN = "urn:ietf:params:oauth:token-type:id_token"
    private const val GOOGLE_STS_TOKEN_URL = "https://sts.googleapis.com/v1/token"
    private const val IAM_IMPERSONATION_URL_FORMAT =
      "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken"
  }
}
