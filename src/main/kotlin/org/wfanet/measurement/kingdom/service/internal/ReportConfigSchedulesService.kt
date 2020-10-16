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

package org.wfanet.measurement.kingdom.service.internal

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedulesGrpcKt.ReportConfigSchedulesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamReadyReportConfigSchedulesRequest
import org.wfanet.measurement.kingdom.db.KingdomRelationalDatabase

class ReportConfigSchedulesService(
  private val kingdomRelationalDatabase: KingdomRelationalDatabase
) : ReportConfigSchedulesCoroutineImplBase() {

  override fun streamReadyReportConfigSchedules(
    request: StreamReadyReportConfigSchedulesRequest
  ): Flow<ReportConfigSchedule> =
    kingdomRelationalDatabase.streamReadySchedules(request.limit)
}
