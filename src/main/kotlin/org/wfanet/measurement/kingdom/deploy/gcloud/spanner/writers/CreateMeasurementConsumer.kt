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

import com.google.cloud.spanner.Mutation
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer

class CreateMeasurementConsumer(private val measurementConsumer: MeasurementConsumer) :
  SpannerWriter<ExternalId, MeasurementConsumer>() {
  override suspend fun TransactionScope.runTransaction(): ExternalId {
    val internalCertificateId = idGenerator.generateInternalId()
    val externalCertificateId = idGenerator.generateExternalId()

    measurementConsumer
      .preferredCertificate
      .toInsertMutation(internalCertificateId, externalCertificateId)
      .bufferTo(transactionContext)

    val internalMeasurementConsumerId = idGenerator.generateInternalId()
    val externalMeasurementConsumerId = idGenerator.generateExternalId()

    Mutation.newInsertBuilder("MeasurementConsumers")
      .set("MeasurementConsumerId")
      .to(internalMeasurementConsumerId.value)
      .set("ExternalMeasurementConsumerId")
      .to(externalMeasurementConsumerId.value)
      .set("MeasurementConsumerDetails")
      .toProtoBytes(measurementConsumer.details)
      .set("MeasurementConsumerDetailsJson")
      .toProtoJson(measurementConsumer.details)
      .build()
      .bufferTo(transactionContext)

    val externalMeasurementConsumerCertificateId = idGenerator.generateExternalId()

    Mutation.newInsertBuilder("MeasurementConsumerCertificates")
      .set("MeasurementConsumerId")
      .to(internalMeasurementConsumerId.value)
      .set("CertificateId")
      .to(internalCertificateId.value)
      .set("ExternalMeasurementConsumerCertificateId")
      .to(externalMeasurementConsumerCertificateId.value)
      .build()
      .bufferTo(transactionContext)

    return externalMeasurementConsumerId
  }

  override fun ResultScope<ExternalId>.buildResult(): MeasurementConsumer {
    val externalMeasurementConsumerId = checkNotNull(transactionResult).value
    return measurementConsumer
      .toBuilder()
      .setExternalMeasurementConsumerId(externalMeasurementConsumerId)
      .build()
  }
}
