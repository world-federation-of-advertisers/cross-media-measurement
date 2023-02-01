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

import org.wfanet.measurement.common.ResourceNameParser

private val parser =
  ResourceNameParser("modelProviders/{model_provider}/certificates/{certificate}")

/** [ResourceKey] of a ModelProvider Certificate. */
data class ModelProviderCertificateKey(
  val modelProviderId: String,
  override val certificateId: String
) : CertificateParentKey {
  override fun toName(): String {
    return parser.assembleName(
      mapOf(IdVariable.MODEL_PROVIDER to modelProviderId, IdVariable.CERTIFICATE to certificateId)
    )
  }

  companion object {
    val defaultValue = ModelProviderCertificateKey("", "")

    fun fromName(resourceName: String): ModelProviderCertificateKey? {
      return parser.parseIdVars(resourceName)?.let {
        ModelProviderCertificateKey(
          it.getValue(IdVariable.MODEL_PROVIDER),
          it.getValue(IdVariable.CERTIFICATE)
        )
      }
    }
  }
}
