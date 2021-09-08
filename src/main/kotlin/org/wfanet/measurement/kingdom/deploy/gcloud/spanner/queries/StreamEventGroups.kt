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
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader

class StreamEventGroups(requestFilter: StreamEventGroupsRequest.Filter, limit: Int = 0) :
  SimpleSpannerQuery<EventGroup>() {
  override val reader =
    EventGroupReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause("ORDER BY EventGroups.CreateTime ASC")
      if (limit > 0) {
        appendClause("LIMIT @$LIMIT")
        bind(LIMIT to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamEventGroupsRequest.Filter) {
    val conjuncts = mutableListOf<String>()
    if (filter.externalMeasurementConsumerIdsList.isNotEmpty()) {
      conjuncts.add(
        "ExternalMeasurementConsumerId IN UNNEST(@$EXTERNAL_MEASUREMENT_CONSUMER_IDS)"
      )
      bind(EXTERNAL_MEASUREMENT_CONSUMER_IDS)
        .toInt64Array(filter.externalMeasurementConsumerIdsList.map { it.toLong() })
    }
    if (filter.externalDataProviderId != 0L) {
      conjuncts.add("ExternalDataProviderId = @$EXTERNAL_DATA_PROVIDER_ID")
      bind(EXTERNAL_DATA_PROVIDER_ID to filter.externalDataProviderId)
    }
    // TODO(world-federation-of-advertisers/cross-media-measurement#255) : stop using CREATED_AFTER
    // as
    // a filter.
    if (filter.hasCreatedAfter()) {
      conjuncts.add("EventGroups.CreateTime > @$CREATED_AFTER")
      bind(CREATED_AFTER to filter.createdAfter.toGcloudTimestamp())
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
    const val CREATED_AFTER = "createdAfter"
  }
}
