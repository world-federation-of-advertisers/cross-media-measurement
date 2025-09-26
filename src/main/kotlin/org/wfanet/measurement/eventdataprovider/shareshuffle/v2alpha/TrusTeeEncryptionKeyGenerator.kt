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

package org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha

import com.google.crypto.tink.KeyTemplate
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadKeyTemplates
import org.wfanet.measurement.api.v2alpha.EncryptionKey

class TrusTeeEncryptionKeyGenerator(
  private val kmsClient: KmsClient,
  private val kmsKekUri: String,
  private val keyTemplate: KeyTemplate = AeadKeyTemplates.AES256_GCM
) {
  fun generate(): EncryptionKey {
    // TODO
  }

  companion object {
    /** A convenience function */
    fun build(
      kmsClient: KmsClient,
      kmsKekUri: String,
      keyTemplate: KeyTemplate = AeadKeyTemplates.AES256_GCM
    ): EncryptionKey {
      // TODO
    }
  }
}
