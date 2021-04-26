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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import java.time.Clock
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.RequisitionTemplate
import org.wfanet.measurement.kingdom.db.LegacySchedulingDatabase
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.ReadRequisitionTemplates
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamReadySchedules

class SpannerLegacySchedulingDatabase(
  clock: Clock,
  idGenerator: IdGenerator,
  client: AsyncDatabaseClient
) : LegacySchedulingDatabase, BaseSpannerDatabase(clock, idGenerator, client) {
  override fun listRequisitionTemplates(reportConfigId: ExternalId): Flow<RequisitionTemplate> {
    return ReadRequisitionTemplates(reportConfigId).execute()
  }

  override fun streamReadySchedules(limit: Long): Flow<ReportConfigSchedule> {
    return StreamReadySchedules(limit).execute()
  }
}
