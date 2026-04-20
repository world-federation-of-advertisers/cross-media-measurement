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

import com.google.crypto.tink.KmsClient
import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.EncryptionKey

class TrusTeeFulfillRequisitionRequestBuilder(
  private val requisition: Requisition,
  private val requisitionNonce: Long,
  private val frequencyVector: FrequencyVector,
  private val dataProviderCertificateKey: DataProviderCertificateKey,
  private val encryptedDek: EncryptionKey,
  private val kmsKekUri: String,
  private val workloadIdentityProvider: String,
) {
  fun build(): Sequence<FulfillRequisitionRequest> = sequence {
    // TODO
  }

  companion object {
    /** A convenience function */
    fun build(
      requisition: Requisition,
      requisitionNonce: Long,
      frequencyVector: FrequencyVector,
      dataProviderCertificateKey: DataProviderCertificateKey,
      encryptedDek: EncryptionKey,
      kmsKekUri: String,
      workloadIdentityProvider: String,
    ): Sequence<FulfillRequisitionRequest> {
      // TODO
    }
  }
}
