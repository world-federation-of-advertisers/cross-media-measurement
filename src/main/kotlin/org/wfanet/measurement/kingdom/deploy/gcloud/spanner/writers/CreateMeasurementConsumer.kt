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
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer

class CreateMeasurementConsumer(private val measurementConsumer: MeasurementConsumer) :
  SpannerWriter<ExternalId, MeasurementConsumer>() {
  override suspend fun TransactionScope.runTransaction(): ExternalId {
    val internalId = idGenerator.generateInternalId()
    val externalId = idGenerator.generateExternalId()
    Mutation.newInsertBuilder("MeasurementConsumers")
      .set("MeasurementConsumerId")
      .to(internalId.value)
      .set("ExternalMeasurementConsumerId")
      .to(externalId.value)
      .set("MeasurementConsumerDetails")
      .to("")
      .set("MeasurementConsumerDetailsJson")
      .to("")
      .build()
      .bufferTo(transactionContext)
    return externalId
  }

  override fun ResultScope<ExternalId>.buildResult(): MeasurementConsumer {
    val externalMeasurementConsumerId = checkNotNull(transactionResult).value
    return MeasurementConsumer.newBuilder()
      .setExternalMeasurementConsumerId(externalMeasurementConsumerId)
      .build()
  }
}
