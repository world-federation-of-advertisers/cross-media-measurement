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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing

import java.time.Clock
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.Advertiser
import org.wfanet.measurement.internal.kingdom.Campaign
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.ReportConfig
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.kingdom.db.testing.DatabaseTestHelper
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.BaseSpannerDatabase
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateAdvertiser
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateCampaign
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateDataProviderLegacy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateReportConfig
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateSchedule

class SpannerDatabaseTestHelper(
  clock: Clock,
  idGenerator: IdGenerator,
  client: AsyncDatabaseClient
) : DatabaseTestHelper, BaseSpannerDatabase(clock, idGenerator, client) {
  override suspend fun createDataProvider(): DataProvider {
    return CreateDataProviderLegacy().execute()
  }

  override suspend fun createAdvertiser(): Advertiser {
    return CreateAdvertiser().execute()
  }

  override suspend fun createCampaign(
    externalDataProviderId: ExternalId,
    externalAdvertiserId: ExternalId,
    providedCampaignId: String
  ): Campaign {
    return CreateCampaign(externalDataProviderId, externalAdvertiserId, providedCampaignId)
      .execute()
  }

  override suspend fun createReportConfig(
    reportConfig: ReportConfig,
    campaigns: List<ExternalId>
  ): ReportConfig {
    return CreateReportConfig(reportConfig, campaigns).execute()
  }

  override suspend fun createSchedule(schedule: ReportConfigSchedule): ReportConfigSchedule {
    return CreateSchedule(schedule).execute()
  }
}
