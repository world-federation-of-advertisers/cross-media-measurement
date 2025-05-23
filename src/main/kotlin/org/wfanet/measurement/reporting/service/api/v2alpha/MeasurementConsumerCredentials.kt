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
