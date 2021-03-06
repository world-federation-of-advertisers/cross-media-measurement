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
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader

/**
 * @throws KingdomInternalException if the MeasurementConsumer or DataProvider for this EventGroup
 * is not found.
 */
class CreateEventGroup(private val eventGroup: EventGroup) :
  SpannerWriter<EventGroup, EventGroup>() {
  override suspend fun TransactionScope.runTransaction(): EventGroup {
    val measurementConsumerId =
      MeasurementConsumerReader()
        .readExternalIdOrNull(
          transactionContext,
          ExternalId(eventGroup.externalMeasurementConsumerId)
        )
        ?.measurementConsumerId
        ?: throw KingdomInternalException(
          KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND
        )

    val dataProviderId =
      DataProviderReader()
        .readExternalIdOrNull(transactionContext, ExternalId(eventGroup.externalDataProviderId))
        ?.dataProviderId
        ?: throw KingdomInternalException(KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND)

    return findExistingEventGroup(dataProviderId)
      ?: createNewEventGroup(dataProviderId, measurementConsumerId)
  }

  private suspend fun TransactionScope.createNewEventGroup(
    dataProviderId: Long,
    measurementConsumerId: Long
  ): EventGroup {
    val internalEventGroupId = idGenerator.generateInternalId()
    val externalEventGroupId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("EventGroups") {
      set("EventGroupId" to internalEventGroupId.value)
      set("ExternalEventGroupId" to externalEventGroupId.value)
      set("MeasurementConsumerId" to measurementConsumerId)
      set("DataProviderId" to dataProviderId)
      set("ProvidedEventGroupId" to eventGroup.providedEventGroupId)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
    }

    return eventGroup.toBuilder().setExternalEventGroupId(externalEventGroupId.value).build()
  }

  private suspend fun TransactionScope.findExistingEventGroup(dataProviderId: Long): EventGroup? {
    val whereClause =
      """
      WHERE EventGroups.DataProviderId = @data_provider_id
        AND EventGroups.ProvidedEventGroupId = @provided_event_group_id
      """.trimIndent()

    return EventGroupReader()
      .withBuilder {
        appendClause(whereClause)
        bind("data_provider_id").to(dataProviderId)
        bind("provided_event_group_id").to(eventGroup.providedEventGroupId)
      }
      .execute(transactionContext)
      .map { it.eventGroup }
      .singleOrNull()
  }

  override fun ResultScope<EventGroup>.buildResult(): EventGroup {
    val eventGroup = checkNotNull(transactionResult)
    return if (eventGroup.hasCreateTime()) {
      eventGroup
    } else {
      eventGroup.toBuilder().apply { createTime = commitTimestamp.toProto() }.build()
    }
  }
}
