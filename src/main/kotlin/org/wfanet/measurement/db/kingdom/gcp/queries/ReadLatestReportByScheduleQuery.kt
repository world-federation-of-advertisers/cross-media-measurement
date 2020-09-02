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

package org.wfanet.measurement.db.kingdom.gcp.queries

import com.google.cloud.spanner.ReadContext
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.db.kingdom.gcp.readers.ReportReader
import org.wfanet.measurement.internal.kingdom.Report

class ReadLatestReportByScheduleQuery {
  fun execute(readContext: ReadContext, externalScheduleId: ExternalId): Report = runBlocking {
    val whereClause =
      """
      WHERE ReportConfigSchedules.ExternalScheduleId = @external_schedule_id
      ORDER BY CreateTime DESC
      LIMIT 1
      """.trimIndent()

    ReportReader()
      .withBuilder {
        appendClause(whereClause)
        bind("external_schedule_id").to(externalScheduleId.value)
      }
      .execute(readContext)
      .single()
      .report
  }
}
