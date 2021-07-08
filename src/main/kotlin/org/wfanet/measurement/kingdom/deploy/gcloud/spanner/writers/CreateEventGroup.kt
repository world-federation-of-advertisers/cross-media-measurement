// Copyright 2020 The Cross-Media Measurement Authors
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

import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.insertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader

class CreateEventGroup(private val eventGroup: EventGroup) :
  SpannerWriter<EventGroup, EventGroup>() {
  override suspend fun TransactionScope.runTransaction(): EventGroup {
    val measurementConsumerId =
      MeasurementConsumerReader()
        .readExternalId(transactionContext, ExternalId(eventGroup.externalMeasurementConsumerId))
        .measurementConsumerId

    val dataProviderId =
      DataProviderReader()
        .readExternalId(transactionContext, ExternalId(eventGroup.externalDataProviderId))
        .dataProviderId
    val internalEventGroupId = idGenerator.generateInternalId()
    val externalEventGroupId = idGenerator.generateExternalId()

    insertMutation("EventGroups") {
      set("EventGroupId" to internalEventGroupId.value)
      set("ExternalEventGroupId" to externalEventGroupId.value)
      set("MeasurementConsumerId" to measurementConsumerId)
      set("DataProviderId" to dataProviderId)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
    }
      .bufferTo(transactionContext)

    return eventGroup.toBuilder().setExternalEventGroupId(externalEventGroupId.value).build()
  }

  override fun ResultScope<EventGroup>.buildResult(): EventGroup {
    return checkNotNull(transactionResult)
  }
}
