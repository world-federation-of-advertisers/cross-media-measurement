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

package org.wfanet.measurement.kingdom.db

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.RequisitionTemplate

/**
 * Database abstraction supporting "legacy" scheduling operations: those present in the v1alpha
 * public API but deliberately removed from v2alpha (the Kingdom will no longer handle scheduling).
 */
interface LegacySchedulingDatabase {
  /**
   * Lists the idealized [RequisitionTemplate]s for a [ReportConfig].
   *
   * TODO: this will be removed with the transition to the v2alpha public API.
   */
  fun listRequisitionTemplates(reportConfigId: ExternalId): Flow<RequisitionTemplate>

  /**
   * Streams [ReportConfigSchedule]s with a nextReportStartTime in the past.
   *
   * TODO: this will be removed with the transition to the v2alpha public API.
   */
  fun streamReadySchedules(limit: Long): Flow<ReportConfigSchedule>
}
