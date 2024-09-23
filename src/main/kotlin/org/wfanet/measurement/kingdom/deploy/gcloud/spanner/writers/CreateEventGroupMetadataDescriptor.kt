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

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.SpannerException
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptor
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupMetadataDescriptorAlreadyExistsWithTypeException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupMetadataDescriptorReader

/**
 * Creates a EventGroupMetadataDescriptor in the database.
 *
 * Throws one of the following subclasses of [KingdomInternalException] on [execute]:
 * * [DataProviderNotFoundException]
 * * [EventGroupMetadataDescriptorAlreadyExistsWithTypeException]
 */
class CreateEventGroupMetadataDescriptor(
  private val eventGroupMetadataDescriptor: EventGroupMetadataDescriptor,
  private val protobufTypeNames: List<String>,
) : SpannerWriter<EventGroupMetadataDescriptor, EventGroupMetadataDescriptor>() {
  init {
    require(eventGroupMetadataDescriptor.externalEventGroupMetadataDescriptorId == 0L)
  }

  override suspend fun TransactionScope.runTransaction(): EventGroupMetadataDescriptor {
    val externalDataProviderId = ExternalId(eventGroupMetadataDescriptor.externalDataProviderId)
    val dataProviderId =
      DataProviderReader.readDataProviderId(transactionContext, externalDataProviderId)
        ?: throw DataProviderNotFoundException(externalDataProviderId)

    val idempotencyKey: String = eventGroupMetadataDescriptor.idempotencyKey
    if (idempotencyKey.isNotEmpty()) {
      val existingResult: EventGroupMetadataDescriptorReader.Result? =
        EventGroupMetadataDescriptorReader()
          .readByIdempotencyKey(transactionContext, dataProviderId, idempotencyKey)
      if (existingResult != null) {
        return existingResult.eventGroupMetadataDescriptor
      }
    }

    return createNewEventGroupMetadataDescriptor(dataProviderId)
  }

  override suspend fun handleSpannerException(e: SpannerException): EventGroupMetadataDescriptor? {
    if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
      throw EventGroupMetadataDescriptorAlreadyExistsWithTypeException(cause = e)
    } else {
      throw e
    }
  }

  private fun TransactionScope.createNewEventGroupMetadataDescriptor(
    dataProviderId: InternalId
  ): EventGroupMetadataDescriptor {
    val internalDescriptorId: InternalId = idGenerator.generateInternalId()
    val externalDescriptorId: ExternalId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("EventGroupMetadataDescriptors") {
      set("DataProviderId" to dataProviderId)
      set("EventGroupMetadataDescriptorId" to internalDescriptorId)
      set("ExternalEventGroupMetadataDescriptorId" to externalDescriptorId)
      if (eventGroupMetadataDescriptor.idempotencyKey.isNotEmpty()) {
        set("IdempotencyKey" to eventGroupMetadataDescriptor.idempotencyKey)
      }

      set("DescriptorDetails").to(eventGroupMetadataDescriptor.details)
    }

    for (protobufTypeName in protobufTypeNames) {
      transactionContext.bufferInsertMutation("EventGroupMetadataDescriptorTypes") {
        set("DataProviderId" to dataProviderId)
        set("EventGroupMetadataDescriptorId" to internalDescriptorId)
        set("ProtobufTypeName" to protobufTypeName)
      }
    }

    return eventGroupMetadataDescriptor
      .toBuilder()
      .setExternalEventGroupMetadataDescriptorId(externalDescriptorId.value)
      .build()
  }

  override fun ResultScope<EventGroupMetadataDescriptor>.buildResult():
    EventGroupMetadataDescriptor {
    return checkNotNull(this.transactionResult)
  }
}
