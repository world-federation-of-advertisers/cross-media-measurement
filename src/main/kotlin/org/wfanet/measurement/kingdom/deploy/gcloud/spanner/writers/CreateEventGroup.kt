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
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.checkValidCertificate as checkValidCertificate

/**
 * Creates a EventGroup in the database
 *
 * Throws one of the following [KingdomInternalException] types on [execute].
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 * * [DataProviderNotFoundException] DataProvider not found
 */
class CreateEventGroup(private val eventGroup: EventGroup) :
  SpannerWriter<EventGroup, EventGroup>() {
  override suspend fun TransactionScope.runTransaction(): EventGroup {
    val measurementConsumerId =
      MeasurementConsumerReader()
        .readByExternalMeasurementConsumerId(
          transactionContext,
          ExternalId(eventGroup.externalMeasurementConsumerId)
        )
        ?.measurementConsumerId
        ?: throw MeasurementConsumerNotFoundException(
          ExternalId(eventGroup.externalMeasurementConsumerId)
        )

    val dataProviderId =
      DataProviderReader()
        .readByExternalDataProviderId(
          transactionContext,
          ExternalId(eventGroup.externalDataProviderId)
        )
        ?.dataProviderId
        ?: throw DataProviderNotFoundException(ExternalId(eventGroup.externalDataProviderId))

    return if (eventGroup.providedEventGroupId.isBlank()) {
      createNewEventGroup(dataProviderId, measurementConsumerId)
    } else {
      findExistingEventGroup(dataProviderId)
        ?: createNewEventGroup(dataProviderId, measurementConsumerId)
    }
  }

  private suspend fun TransactionScope.createNewEventGroup(
    dataProviderId: Long,
    measurementConsumerId: Long
  ): EventGroup {
    val internalEventGroupId: InternalId = idGenerator.generateInternalId()
    val externalEventGroupId: ExternalId = idGenerator.generateExternalId()
    val measurementConsumerCertificateId =
      if (eventGroup.externalMeasurementConsumerCertificateId > 0L)
        checkValidCertificate(
          eventGroup.externalMeasurementConsumerCertificateId,
          eventGroup.externalMeasurementConsumerId,
          transactionContext
        )
      else null
    transactionContext.bufferInsertMutation("EventGroups") {
      set("EventGroupId" to internalEventGroupId)
      set("ExternalEventGroupId" to externalEventGroupId)
      set("MeasurementConsumerId" to measurementConsumerId)
      if (measurementConsumerCertificateId != null) {
        set("MeasurementConsumerCertificateId" to measurementConsumerCertificateId)
      }
      set("DataProviderId" to dataProviderId)
      if (eventGroup.providedEventGroupId.isNotBlank()) {
        set("ProvidedEventGroupId" to eventGroup.providedEventGroupId)
      }
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      if (eventGroup.hasDetails()) {
        set("EventGroupDetails" to eventGroup.details)
        setJson("EventGroupDetailsJson" to eventGroup.details)
      }
      set("State" to EventGroup.State.ACTIVE)
    }

    return eventGroup.copy {
      this.externalEventGroupId = externalEventGroupId.value
      this.state = EventGroup.State.ACTIVE
    }
  }

  private suspend fun TransactionScope.findExistingEventGroup(dataProviderId: Long): EventGroup? {
    return EventGroupReader()
      .bindWhereClause(dataProviderId, eventGroup.providedEventGroupId)
      .execute(transactionContext)
      .singleOrNull()
      ?.eventGroup
  }

  override fun ResultScope<EventGroup>.buildResult(): EventGroup {
    val eventGroup = checkNotNull(transactionResult)
    return if (eventGroup.hasCreateTime() && eventGroup.hasUpdateTime()) {
      eventGroup
    } else {
      eventGroup.copy {
        createTime = commitTimestamp.toProto()
        updateTime = commitTimestamp.toProto()
      }
    }
  }
}
