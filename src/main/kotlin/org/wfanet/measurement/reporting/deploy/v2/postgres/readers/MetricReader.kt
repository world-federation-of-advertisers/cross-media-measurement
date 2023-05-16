/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.postgres.readers

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.reporting.v2.Metric

class MetricReader(private val readContext: ReadContext) {
  data class Result(
    val measurementConsumerId: InternalId,
    val metricId: InternalId,
    val createMetricRequestId: String,
    val metric: Metric
  )

  suspend fun readMetricsByRequestId(
    measurementConsumerId: InternalId,
    createMetricRequestIds: Collection<String>
  ): Flow<Result> {
    if (createMetricRequestIds.isEmpty()) {
      return emptyFlow()
    }

    // TODO(tristanvuong2021): implement read metric
    return emptyFlow()
  }
}
