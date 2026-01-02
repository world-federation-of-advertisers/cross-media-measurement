/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.protobuf.Empty
import com.google.type.Date
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.kingdom.BatchDeleteEventGroupActivitiesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupActivityNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupActivityReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader

class BatchDeleteEventGroupActivities(
  private val requests: BatchDeleteEventGroupActivitiesRequest
) : SimpleSpannerWriter<Empty>() {

  override suspend fun TransactionScope.runTransaction(): Empty {
    val externalDataProviderId = ExternalId(requests.externalDataProviderId)
    val dataProviderId: InternalId =
      DataProviderReader.readDataProviderId(transactionContext, externalDataProviderId)
        ?: throw DataProviderNotFoundException(externalDataProviderId)

    val externalEventGroupId = ExternalId(requests.externalEventGroupId)
    val eventGroupId: InternalId =
      EventGroupReader.readEventGroupId(transactionContext, dataProviderId, externalEventGroupId)
        ?: throw EventGroupNotFoundException(externalDataProviderId, externalEventGroupId)

    val uniqueDates = requests.externalEventGroupActivityIdsList.distinct()

    val dateToKey: Map<Date, Key> =
      EventGroupActivityReader.readKeysByIndex(
        transactionContext,
        dataProviderId,
        eventGroupId,
        uniqueDates,
      )

    if (dateToKey.size != uniqueDates.size) {
      val missingDate = uniqueDates.first { !dateToKey.containsKey(it) }
      throw EventGroupActivityNotFoundException(
        externalDataProviderId,
        externalEventGroupId,
        missingDate,
      )
    }

    val keySet = KeySet.newBuilder()
    dateToKey.values.forEach { keySet.addKey(it) }

    transactionContext.buffer(Mutation.delete("EventGroupActivities", keySet.build()))

    return Empty.getDefaultInstance()
  }
}
