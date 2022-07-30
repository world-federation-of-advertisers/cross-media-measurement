// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v1alpha

import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.api.Principal
import org.wfanet.measurement.common.api.ResourcePrincipal
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig

/** Identifies the sender of an inbound gRPC request. */
sealed interface ReportingPrincipal : Principal {
  val config: MeasurementConsumerConfig

  companion object {
    fun fromConfigs(name: String, config: MeasurementConsumerConfig): ReportingPrincipal? {
      return when (name.substringBefore('/')) {
        MeasurementConsumerKey.COLLECTION_NAME -> {
          require(
            config.apiKey.isNotBlank() &&
              MeasurementConsumerCertificateKey.fromName(config.signingCertificateName) != null &&
              config.signingPrivateKeyPath.isNotBlank()
          )
          MeasurementConsumerKey.fromName(name)?.let { MeasurementConsumerPrincipal(it, config) }
        }
        else -> null
      }
    }
  }
}

data class MeasurementConsumerPrincipal(
  override val resourceKey: MeasurementConsumerKey,
  override val config: MeasurementConsumerConfig
) : ReportingPrincipal, ResourcePrincipal
