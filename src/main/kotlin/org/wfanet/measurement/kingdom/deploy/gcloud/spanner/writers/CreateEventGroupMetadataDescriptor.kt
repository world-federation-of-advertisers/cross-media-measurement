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

import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptor
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupMetadataDescriptorReader

class CreateEventGroupMetadataDescriptor(
  private val eventGroupMetadataDescriptor: EventGroupMetadataDescriptor
) : SpannerWriter<EventGroupMetadataDescriptor, EventGroupMetadataDescriptor>() {
  override suspend fun TransactionScope.runTransaction(): EventGroupMetadataDescriptor {
    val dataProviderId =
      DataProviderReader()
        .readByExternalDataProviderId(
          transactionContext,
          ExternalId(eventGroupMetadataDescriptor.externalDataProviderId)
        )
        ?.dataProviderId
        ?: throw DataProviderNotFoundException(
          ExternalId(eventGroupMetadataDescriptor.externalDataProviderId)
        )
    return if (eventGroupMetadataDescriptor.externalEventGroupMetadataDescriptorId == 0L) {
      createNewEventGroupMetadataDescriptor(dataProviderId)
    } else {
      findExistingEventGroupMetadataDescriptor(
        dataProviderId,
        eventGroupMetadataDescriptor.externalEventGroupMetadataDescriptorId
      )
        ?: createNewEventGroupMetadataDescriptor(dataProviderId)
    }
  }

  private suspend fun TransactionScope.createNewEventGroupMetadataDescriptor(
    dataProviderId: Long
  ): EventGroupMetadataDescriptor {
    val internalDescriptorId: InternalId = idGenerator.generateInternalId()
    val externalDescriptorId: ExternalId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("EventGroupMetadataDescriptors") {
      set("DataProviderId" to dataProviderId)
      set("EventGroupMetadataDescriptorId" to internalDescriptorId)
      set("ExternalEventGroupMetadataDescriptorId" to externalDescriptorId)

      set("DescriptorDetails" to eventGroupMetadataDescriptor.details)
      setJson("DescriptorDetailsJson" to eventGroupMetadataDescriptor.details)
    }

    return eventGroupMetadataDescriptor
      .toBuilder()
      .setExternalEventGroupMetadataDescriptorId(externalDescriptorId.value)
      .build()
  }

  private suspend fun TransactionScope.findExistingEventGroupMetadataDescriptor(
    dataProviderId: Long,
    externalEventGroupMetadataDescriptorId: Long
  ): EventGroupMetadataDescriptor? {
    return EventGroupMetadataDescriptorReader()
      .bindWhereClause(dataProviderId, externalEventGroupMetadataDescriptorId)
      .execute(transactionContext)
      .singleOrNull()
      ?.eventGroupMetadataDescriptor
  }

  override fun ResultScope<EventGroupMetadataDescriptor>.buildResult():
    EventGroupMetadataDescriptor {
    return checkNotNull(this.transactionResult)
  }
}
