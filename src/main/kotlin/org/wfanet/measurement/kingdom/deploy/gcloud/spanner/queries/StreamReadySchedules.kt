// Copyright 2020 The Measurement System Authors
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

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.BaseSpannerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ScheduleReader

/**
 * SpannerQuery for finding [ReportConfigSchedule]s with nextReportStartTimes in the past.
 */
class StreamReadySchedules(
  limit: Long
) : SpannerQuery<ScheduleReader.Result, ReportConfigSchedule>() {

  override val reader: BaseSpannerReader<ScheduleReader.Result> by lazy {
    ScheduleReader()
      .withBuilder {
        appendClause("WHERE ReportConfigSchedules.NextReportStartTime < CURRENT_TIMESTAMP()")

        if (limit > 0) {
          appendClause("LIMIT @limit")
          bind("limit").to(limit)
        }
      }
  }

  override fun Flow<ScheduleReader.Result>.transform(): Flow<ReportConfigSchedule> {
    return map { it.schedule }
  }
}
