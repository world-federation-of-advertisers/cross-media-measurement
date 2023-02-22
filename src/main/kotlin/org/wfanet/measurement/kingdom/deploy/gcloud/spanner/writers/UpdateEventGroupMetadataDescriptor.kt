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

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptor
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupMetadataDescriptorNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupMetadataDescriptorReader

/**
 * Update [EventGroupMetadataDescriptor] in the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [EventGroupMetadataDescriptorNotFoundException] EventGroupMetadataDescriptor not found
 */
class UpdateEventGroupMetadataDescriptor(
  private val eventGroupMetadataDescriptor: EventGroupMetadataDescriptor
) : SpannerWriter<EventGroupMetadataDescriptor, EventGroupMetadataDescriptor>() {
  override suspend fun TransactionScope.runTransaction(): EventGroupMetadataDescriptor {
    val internalMetadataDescriptorResult =
      EventGroupMetadataDescriptorReader()
        .readByExternalIds(
          transactionContext,
          eventGroupMetadataDescriptor.externalDataProviderId,
          eventGroupMetadataDescriptor.externalEventGroupMetadataDescriptorId
        )
        ?: throw EventGroupMetadataDescriptorNotFoundException(
          ExternalId(eventGroupMetadataDescriptor.externalDataProviderId),
          ExternalId(eventGroupMetadataDescriptor.externalEventGroupMetadataDescriptorId)
        )

    transactionContext.bufferUpdateMutation("EventGroupMetadataDescriptors") {
      set("DataProviderId" to internalMetadataDescriptorResult.internalDataProviderId.value)
      set(
        "EventGroupMetadataDescriptorId" to
          internalMetadataDescriptorResult.internalDescriptorId.value
      )
      set(
        "ExternalEventGroupMetadataDescriptorId" to
          eventGroupMetadataDescriptor.externalEventGroupMetadataDescriptorId
      )

      set("DescriptorDetails" to eventGroupMetadataDescriptor.details)
      setJson("DescriptorDetailsJson" to eventGroupMetadataDescriptor.details)
    }

    return eventGroupMetadataDescriptor
  }

  override fun ResultScope<EventGroupMetadataDescriptor>.buildResult():
    EventGroupMetadataDescriptor {
    return eventGroupMetadataDescriptor
  }
}
