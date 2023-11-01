// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.api.v2alpha

import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey

sealed interface CertificateKey : ChildResourceKey {
  override val parentKey: CertificateParentKey

  val certificateId: String

  companion object FACTORY : ResourceKey.Factory<CertificateKey> {
    override fun fromName(resourceName: String): CertificateKey? {
      return DataProviderCertificateKey.fromName(resourceName)
        ?: MeasurementConsumerCertificateKey.fromName(resourceName)
        ?: DuchyCertificateKey.fromName(resourceName)
        ?: ModelProviderCertificateKey.fromName(resourceName)
    }
  }
}

sealed interface CertificateParentKey : ResourceKey
