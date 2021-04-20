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

package org.wfanet.measurement.kingdom.db.testing

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.Advertiser
import org.wfanet.measurement.internal.kingdom.Campaign
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.ReportConfig
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule

/** Abstracts database operations required for testing. */
interface DatabaseTestHelper {
  /** Registers a Data Provider. */
  suspend fun createDataProvider(): DataProvider

  /** Registers an Advertiser. */
  suspend fun createAdvertiser(): Advertiser

  /**
   * Registers a Campaign.
   *
   * @param externalDataProviderId the Data Provider providing data for the campaign
   * @param externalAdvertiserId the Advertiser owning of the campaign
   * @param providedCampaignId user-provided, unvalidated name of the campaign (for display in UIs)
   * @return the created [Campaign]
   */
  suspend fun createCampaign(
    externalDataProviderId: ExternalId,
    externalAdvertiserId: ExternalId,
    providedCampaignId: String
  ): Campaign

  /**
   * Creates a [ReportConfig] for an Advertiser.
   *
   * The `externalReportConfigId` in [reportConfig] is ignored and the return value will have a new
   * `externalReportConfigId` populated.
   */
  suspend fun createReportConfig(
    reportConfig: ReportConfig,
    campaigns: List<ExternalId>
  ): ReportConfig

  /**
   * Creates a [ReportConfigSchedule] for a [ReportConfig].
   *
   * The `externalScheduleId` in [schedule] is ignored and the return value will have a new
   * `externalScheduleId` populated.
   */
  suspend fun createSchedule(schedule: ReportConfigSchedule): ReportConfigSchedule
}
