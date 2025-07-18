// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.kms

import com.google.auth.oauth2.GoogleCredentials
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import java.io.IOException
import java.security.GeneralSecurityException
import org.wfanet.measurement.duchy.mill.trustee.KmsClientFactory
import org.wfanet.measurement.duchy.mill.trustee.WifCredentialsConfig

/** A [KmsClientFactory] for creating Tink [KmsClient] instances for Google Cloud KMS. */
class GcpKmsClientFactory : KmsClientFactory {

  /**
   * Returns a [GcpKmsClient] that uses Application Default Credentials.
   *
   * This is suitable for environments where credentials are automatically available, such as on
   * Google Cloud Engine or when the `gcloud auth application-default login` command has been run.
   *
   * @throws GeneralSecurityException if the client cannot be initialized.
   */
  override fun getKmsClient(): KmsClient {
    // GcpKmsClient.withDefaultCredentials() uses GoogleCredentials.getApplicationDefault()
    // under the hood and wraps exceptions in GeneralSecurityException.
    return GcpKmsClient().withDefaultCredentials()
  }

  /**
   * Returns a [GcpKmsClient] configured for Workload Identity Federation (WIF) with service
   * account impersonation.
   *
   * This method programmatically builds an `ExternalAccountCredentials` object, which is ideal for
   * environments like Confidential Space where a short-lived token from a file needs to be
   * exchanged for a GCP access token.
   *
   * @param config The WIF and impersonation configuration.
   * @return An initialized [GcpKmsClient].
   * @throws GeneralSecurityException if the client cannot be initialized with the provided
   *   credentials.
   */
  override fun getKmsClient(config: WifCredentialsConfig): KmsClient {
    val wifConfigJson =
      JsonObject().run {
        addProperty("type", "external_account")
        addProperty("audience", config.audience)
        addProperty("subject_token_type", config.subjectTokenType)
        addProperty("token_url", config.tokenUrl)
        add(
          "credential_source",
          JsonObject().apply { addProperty("file", config.credentialSourceFilePath) }
        )
        addProperty("service_account_impersonation_url", config.serviceAccountImpersonationUrl)
        add("scopes", JsonArray().apply { add("https://www.googleapis.com/auth/cloud-platform") })
        toString()
      }

    val credentials =
      try {
        GoogleCredentials.fromStream(wifConfigJson.byteInputStream(Charsets.UTF_8))
      } catch (e: IOException) {
        throw GeneralSecurityException("Failed to create GoogleCredentials from WIF config", e)
      }

    return GcpKmsClient().withCredentials(credentials)
  }
}
