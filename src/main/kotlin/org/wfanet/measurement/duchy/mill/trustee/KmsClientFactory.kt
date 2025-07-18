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

package org.wfanet.measurement.duchy.mill.trustee

import com.google.crypto.tink.KmsClient

/**
 * Configuration for creating credentials using Workload Identity Federation (WIF) and service
 * account impersonation.
 *
 * audience The audience for the WIF token exchange.
 * subjectTokenType The type of the token being presented (e.g., an OIDC token type).
 * tokenUrl The Security Token Service (STS) token endpoint URL.
 * credentialSourceFilePath The file path to the subject token (e.g., an attestation
 *   token).
 * serviceAccountImpersonationUrl The URL to impersonate a service account to get a final
 *   access token.
 */
data class WifCredentialsConfig(
  val audience: String,
  val subjectTokenType: String,
  val tokenUrl: String,
  val credentialSourceFilePath: String,
  val serviceAccountImpersonationUrl: String,
)

/** Factory for creating [KmsClient] instances. */
interface KmsClientFactory {
  /** Returns a [KmsClient] instance using default credentials (e.g., ADC). */
  fun getKmsClient(): KmsClient

  /**
   * Returns a [KmsClient] instance using Workload Identity Federation credentials.
   *
   * @param config The configuration for WIF and service account impersonation.
   */
  fun getKmsClient(config: WifCredentialsConfig): KmsClient
}
