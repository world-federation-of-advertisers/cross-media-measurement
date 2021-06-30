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

import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.insertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer

class CreateMeasurementConsumer(private val measurementConsumer: MeasurementConsumer) :
  SpannerWriter<MeasurementConsumer, MeasurementConsumer>() {
  override suspend fun TransactionScope.runTransaction(): MeasurementConsumer {
    val internalCertificateId = idGenerator.generateInternalId()

    measurementConsumer
      .preferredCertificate
      .toInsertMutation(internalCertificateId)
      .bufferTo(transactionContext)

    val internalMeasurementConsumerId = idGenerator.generateInternalId()
    val externalMeasurementConsumerId = idGenerator.generateExternalId()

    insertMutation("MeasurementConsumers") {
        set("MeasurementConsumerId" to internalMeasurementConsumerId.value)
        set("PublicKeyCertificateId" to internalCertificateId.value)
        set("ExternalMeasurementConsumerId" to externalMeasurementConsumerId.value)
        set("MeasurementConsumerDetails" to measurementConsumer.details)
        setJson("MeasurementConsumerDetailsJson" to measurementConsumer.details)
      }
      .bufferTo(transactionContext)

    val externalMeasurementConsumerCertificateId = idGenerator.generateExternalId()

    insertMutation("MeasurementConsumerCertificates") {
        set("MeasurementConsumerId" to internalMeasurementConsumerId.value)
        set("CertificateId" to internalCertificateId.value)
        set(
          "ExternalMeasurementConsumerCertificateId" to
            externalMeasurementConsumerCertificateId.value
        )
      }
      .bufferTo(transactionContext)

    return measurementConsumer
      .toBuilder()
      .also {
        it.externalMeasurementConsumerId = externalMeasurementConsumerId.value
        it.externalPublicKeyCertificateId = externalMeasurementConsumerCertificateId.value
        it.preferredCertificateBuilder.also {
          it.externalMeasurementConsumerId = externalMeasurementConsumerId.value
          it.externalCertificateId = externalMeasurementConsumerCertificateId.value
        }
      }
      .build()
  }

  override fun ResultScope<MeasurementConsumer>.buildResult(): MeasurementConsumer {
    return checkNotNull(transactionResult)
  }
}
