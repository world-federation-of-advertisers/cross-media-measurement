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
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.StreamEventGroupRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader

private object Params {
  const val LIMIT = "limit"
  const val EXTERNAL_MEASUREMENT_CONSUMER_ID = "externalMeasurementConsumerId"
  const val EXTERNAL_MEASUREMENT_ID = "externalMeasurementId"
  const val EXTERNAL_DATA_PROVIDER_ID = "externalDataProviderId"
  const val STATES = "states"
  const val MEASUREMENT_STATES = "measurementStates"
  const val UPDATED_AFTER = "updatedAfter"
}

class StreamEventGroups(requestFilter: StreamEventGroups.Filter, limit: Int = 0) :
  SimpleSpannerQuery<EventGroup>() {

  override val reader =
    Reader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause("ORDER BY EventGroup.UpdateTime ASC")
      if (limit > 0) {
        appendClause("LIMIT @${Params.LIMIT}")
        bind(Params.LIMIT to limit.toLong())
      }
    }
}

private fun Statement.Builder.appendWhereClause(filter: StreamEventGroupRequest.Filter) {
  val conjuncts = mutableListOf<String>()
  if (filter.externalMeasurementConsumerId != 0L) {
    conjuncts.add("ExternalMeasurementConsumerId = @${Params.EXTERNAL_MEASUREMENT_CONSUMER_ID}")
    bind(Params.EXTERNAL_MEASUREMENT_CONSUMER_ID to filter.externalMeasurementConsumerId)
  }
  if (filter.externalMeasurementId != 0L) {
    conjuncts.add("ExternalMeasurementId = @${Params.EXTERNAL_MEASUREMENT_ID}")
    bind(Params.EXTERNAL_MEASUREMENT_ID to filter.externalMeasurementId)
  }
  if (filter.externalDataProviderId != 0L) {
    conjuncts.add("ExternalDataProviderId = @${Params.EXTERNAL_DATA_PROVIDER_ID}")
    bind(Params.EXTERNAL_DATA_PROVIDER_ID to filter.externalDataProviderId)
  }
  if (filter.statesValueList.isNotEmpty()) {
    conjuncts.add("EventGroup.State IN UNNEST(@${Params.STATES})")
    bind(Params.STATES).toInt64Array(filter.statesValueList.map { it.toLong() })
  }
  if (filter.measurementStatesValueList.isNotEmpty()) {
    conjuncts.add("Measurements.State IN UNNEST(@${Params.MEASUREMENT_STATES})")
    bind(Params.MEASUREMENT_STATES)
      .toInt64Array(filter.measurementStatesValueList.map { it.toLong() })
  }
  if (filter.hasUpdatedAfter()) {
    conjuncts.add("EventGroup.UpdateTime > @${Params.UPDATED_AFTER}")
    bind(Params.UPDATED_AFTER to filter.updatedAfter.toGcloudTimestamp())
  }

  if (conjuncts.isEmpty()) {
    return
  }

  appendClause("WHERE ")
  append(conjuncts.joinToString(" AND "))
}
