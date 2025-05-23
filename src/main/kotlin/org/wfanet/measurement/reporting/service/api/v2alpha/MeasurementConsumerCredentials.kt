/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.service.api.v2alpha

import org.wfanet.measurement.api.ApiKeyCredentials
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig

data class MeasurementConsumerCredentials(
  val resourceKey: MeasurementConsumerKey,
  val callCredentials: ApiKeyCredentials,
  val signingCertificateKey: MeasurementConsumerCertificateKey,
  val signingPrivateKeyPath: String,
) {
  companion object {
    fun fromConfig(resourceKey: MeasurementConsumerKey, config: MeasurementConsumerConfig) =
      MeasurementConsumerCredentials(
        resourceKey,
        ApiKeyCredentials(config.apiKey),
        requireNotNull(MeasurementConsumerCertificateKey.fromName(config.signingCertificateName)),
        config.signingPrivateKeyPath,
      )
  }
}
