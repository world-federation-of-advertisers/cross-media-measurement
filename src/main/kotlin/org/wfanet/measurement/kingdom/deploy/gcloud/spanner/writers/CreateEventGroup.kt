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
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.CreateEventGroupRequest
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader

/**
 * Creates a EventGroup in the database
 *
 * Throws one of the following [KingdomInternalException] types on [execute].
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 * * [DataProviderNotFoundException] DataProvider not found
 */
class CreateEventGroup(private val request: CreateEventGroupRequest) :
  SpannerWriter<EventGroup, EventGroup>() {

  override suspend fun TransactionScope.runTransaction(): EventGroup {
    val externalMeasurementConsumerId = ExternalId(request.eventGroup.externalMeasurementConsumerId)
    val measurementConsumerId: InternalId =
      MeasurementConsumerReader.readMeasurementConsumerId(
        transactionContext,
        externalMeasurementConsumerId
      ) ?: throw MeasurementConsumerNotFoundException(externalMeasurementConsumerId)

    val externalDataProviderId = ExternalId(request.eventGroup.externalDataProviderId)
    val dataProviderId: InternalId =
      DataProviderReader.readDataProviderId(transactionContext, externalDataProviderId)
        ?: throw DataProviderNotFoundException(externalDataProviderId)

    if (request.requestId.isNotEmpty()) {
      val existingEventGroup: EventGroup? = findExistingEventGroup(dataProviderId)
      if (existingEventGroup != null) {
        return existingEventGroup
      }
    }
    return createNewEventGroup(dataProviderId, measurementConsumerId)
  }

  private fun TransactionScope.createNewEventGroup(
    dataProviderId: InternalId,
    measurementConsumerId: InternalId
  ): EventGroup {
    val internalEventGroupId: InternalId = idGenerator.generateInternalId()
    val externalEventGroupId: ExternalId = idGenerator.generateExternalId()
    transactionContext.bufferInsertMutation("EventGroups") {
      set("EventGroupId" to internalEventGroupId)
      set("ExternalEventGroupId" to externalEventGroupId)
      set("MeasurementConsumerId" to measurementConsumerId)
      set("DataProviderId" to dataProviderId)
      if (request.requestId.isNotEmpty()) {
        set("CreateRequestId" to request.requestId)
      }
      if (request.eventGroup.providedEventGroupId.isNotEmpty()) {
        set("ProvidedEventGroupId" to request.eventGroup.providedEventGroupId)
      }
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      if (request.eventGroup.hasDetails()) {
        set("EventGroupDetails" to request.eventGroup.details)
        setJson("EventGroupDetailsJson" to request.eventGroup.details)
      }
      set("State" to EventGroup.State.ACTIVE)
    }

    return request.eventGroup.copy {
      this.externalEventGroupId = externalEventGroupId.value
      this.state = EventGroup.State.ACTIVE
    }
  }

  private suspend fun TransactionScope.findExistingEventGroup(
    dataProviderId: InternalId
  ): EventGroup? {
    return EventGroupReader()
      .readByCreateRequestId(transactionContext, dataProviderId, request.requestId)
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
