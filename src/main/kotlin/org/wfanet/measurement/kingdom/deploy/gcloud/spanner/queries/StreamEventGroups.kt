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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import com.google.cloud.spanner.Statement
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader

class StreamEventGroups(requestFilter: StreamEventGroupsRequest.Filter, limit: Int = 0) :
  SimpleSpannerQuery<EventGroupReader.Result>() {
  override val reader =
    EventGroupReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause("ORDER BY ExternalDataProviderId ASC, ExternalEventGroupId ASC")
      if (limit > 0) {
        appendClause("LIMIT @$LIMIT")
        bind(LIMIT to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamEventGroupsRequest.Filter) {
    val conjuncts = mutableListOf<String>()
    if (filter.externalMeasurementConsumerIdsList.isNotEmpty()) {
      conjuncts.add("ExternalMeasurementConsumerId IN UNNEST(@$EXTERNAL_MEASUREMENT_CONSUMER_IDS)")
      bind(EXTERNAL_MEASUREMENT_CONSUMER_IDS)
        .toInt64Array(filter.externalMeasurementConsumerIdsList.map { it.toLong() })
    }
    if (filter.externalDataProviderId != 0L) {
      conjuncts.add("ExternalDataProviderId = @$EXTERNAL_DATA_PROVIDER_ID")
      bind(EXTERNAL_DATA_PROVIDER_ID to filter.externalDataProviderId)
    }

    if (filter.externalEventGroupIdAfter != 0L && filter.externalDataProviderIdAfter != 0L) {
      conjuncts.add(
        """
          ((ExternalDataProviderId > @$EXTERNAL_DATA_PROVIDER_ID_AFTER)
          OR (ExternalDataProviderId = @$EXTERNAL_DATA_PROVIDER_ID_AFTER
          AND ExternalEventGroupId > @$EXTERNAL_EVENT_GROUP_ID_AFTER))
        """
          .trimIndent()
      )
      bind(EXTERNAL_DATA_PROVIDER_ID_AFTER).to(filter.externalDataProviderIdAfter)
      bind(EXTERNAL_EVENT_GROUP_ID_AFTER).to(filter.externalEventGroupIdAfter)
    }

    if (!filter.showDeleted) {
      conjuncts.add("State != @$DELETED_STATE")
      bind(DELETED_STATE).toProtoEnum(EventGroup.State.DELETED)
    }

    if (conjuncts.isEmpty()) {
      return
    }

    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  companion object {
    const val LIMIT = "limit"
    const val EXTERNAL_MEASUREMENT_CONSUMER_IDS = "externalMeasurementConsumerIds"
    const val EXTERNAL_DATA_PROVIDER_ID = "externalDataProviderId"
    const val EXTERNAL_EVENT_GROUP_ID_AFTER = "externalEventGroupIdAfter"
    const val EXTERNAL_DATA_PROVIDER_ID_AFTER = "externalDataProviderIdAfter"
    const val DELETED_STATE = "deletedState"
  }
}
