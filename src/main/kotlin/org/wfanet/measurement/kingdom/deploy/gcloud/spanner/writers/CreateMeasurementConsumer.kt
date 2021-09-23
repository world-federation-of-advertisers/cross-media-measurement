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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.copy

class CreateMeasurementConsumer(private val measurementConsumer: MeasurementConsumer) :
  SpannerWriter<MeasurementConsumer, MeasurementConsumer>() {
  override suspend fun TransactionScope.runTransaction(): MeasurementConsumer {
    val internalCertificateId = idGenerator.generateInternalId()

    measurementConsumer
      .certificate
      .toInsertMutation(internalCertificateId)
      .bufferTo(transactionContext)

    val internalMeasurementConsumerId = idGenerator.generateInternalId()
    val externalMeasurementConsumerId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("MeasurementConsumers") {
      set("MeasurementConsumerId" to internalMeasurementConsumerId)
      set("PublicKeyCertificateId" to internalCertificateId)
      set("ExternalMeasurementConsumerId" to externalMeasurementConsumerId)
      set("MeasurementConsumerDetails" to measurementConsumer.details)
      setJson("MeasurementConsumerDetailsJson" to measurementConsumer.details)
    }

    val externalMeasurementConsumerCertificateId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("MeasurementConsumerCertificates") {
      set("MeasurementConsumerId" to internalMeasurementConsumerId)
      set("CertificateId" to internalCertificateId)
      set(
        "ExternalMeasurementConsumerCertificateId" to externalMeasurementConsumerCertificateId.value
      )
    }

    return measurementConsumer.copy {
      this.externalMeasurementConsumerId = externalMeasurementConsumerId.value
      certificate =
        certificate.copy {
          this.externalMeasurementConsumerId = externalMeasurementConsumerId.value
          externalCertificateId = externalMeasurementConsumerCertificateId.value
        }
    }
  }

  override fun ResultScope<MeasurementConsumer>.buildResult(): MeasurementConsumer {
    return checkNotNull(transactionResult)
  }
}
