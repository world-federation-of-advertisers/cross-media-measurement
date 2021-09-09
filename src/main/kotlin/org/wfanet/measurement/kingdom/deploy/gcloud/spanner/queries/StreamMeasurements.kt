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
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader

class StreamMeasurements(
  private val view: Measurement.View,
  requestFilter: StreamMeasurementsRequest.Filter,
  limit: Int = 0
) : SimpleSpannerQuery<MeasurementReader.Result>() {
  override val reader =
    MeasurementReader(view).withBuilder {
      appendWhereClause(requestFilter)
      appendClause("ORDER BY Measurements.CreateTime ASC")
      if (limit > 0) {
        appendClause("LIMIT @$LIMIT_PARAMgi")
        bind(LIMIT_PARAM to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamMeasurementsRequest.Filter) {
    val conjuncts = mutableListOf<String>()
    if (filter.externalMeasurementConsumerId != 0L) {
      conjuncts.add("ExternalMeasurementConsumerId = @$EXTERNAL_MEASUREMENT_CONSUMER_ID_PARAM")
      bind(EXTERNAL_MEASUREMENT_CONSUMER_ID_PARAM to filter.externalMeasurementConsumerId)
    }

    if (filter.statesValueList.isNotEmpty()) {
      conjuncts.add("Measurements.State IN UNNEST(@$STATES_PARAM)")
      bind(STATES_PARAM).toInt64Array(filter.statesValueList.map { it.toLong() })
    }
    if (filter.hasUpdatedAfter()) {
      conjuncts.add("Measurements.UpdateTime > @$UPDATED_AFTER_PARAM")
      bind(UPDATED_AFTER_PARAM to filter.updatedAfter.toGcloudTimestamp())
    }

    if (conjuncts.isEmpty()) {
      return
    }

    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  companion object {
    const val LIMIT_PARAM = "limit"
    const val EXTERNAL_MEASUREMENT_CONSUMER_ID_PARAM = "externalMeasurementConsumerId"
    const val UPDATED_AFTER_PARAM = "updatedAfter"
    const val STATES_PARAM = "states"
  }
}
