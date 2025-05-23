/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.deploy.gcloud

import org.junit.ClassRule
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.integration.common.IMPRESSION_QUALIFICATION_FILTER_MAPPING
import org.wfanet.measurement.integration.common.reporting.v2.InProcessEventGroupsTest
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata.REPORTING_CHANGELOG_PATH

class GCloudInProcessReportingEventGroupsTest :
  InProcessEventGroupsTest(
    KingdomDataServicesProviderRule(spannerEmulator),
    InternalReportingServicesProviderRule(
      spannerEmulator,
      reportingPostgresDatabaseProvider,
      IMPRESSION_QUALIFICATION_FILTER_MAPPING,
    ),
    SpannerAccessServicesFactory(spannerEmulator),
  ) {

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    @get:ClassRule
    @JvmStatic
    val reportingPostgresDatabaseProvider = PostgresDatabaseProviderRule(REPORTING_CHANGELOG_PATH)
  }
}
