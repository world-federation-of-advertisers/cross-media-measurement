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

package org.wfanet.measurement.integration.crypto.testing

import com.google.crypto.tink.Aead
import com.google.crypto.tink.KmsClient
import java.security.GeneralSecurityException

/**
 * A [KmsClient] that throws on all operations. Used as a placeholder in integration tests that
 * require a [KmsClient] parameter but do not exercise TrusTee protocol paths.
 */
object ThrowingKmsClient : KmsClient {
  override fun doesSupport(keyUri: String?): Boolean = false

  override fun withCredentials(credentialPath: String?): KmsClient {
    throw GeneralSecurityException("KMS access is disabled")
  }

  override fun withDefaultCredentials(): KmsClient {
    throw GeneralSecurityException("KMS access is disabled")
  }

  override fun getAead(keyUri: String?): Aead {
    throw GeneralSecurityException("KMS access is disabled")
  }
}
