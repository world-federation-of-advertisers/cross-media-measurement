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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner

import org.junit.ClassRule
import org.junit.Rule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.reporting.service.internal.testing.v2.MeasurementConsumersServiceTest

@RunWith(JUnit4::class)
class SpannerMeasurementConsumersServiceTest : MeasurementConsumersServiceTest<SpannerMeasurementConsumersService>() {

  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.REPORTING_CHANGELOG_PATH)

  override fun newService(idGenerator: IdGenerator): SpannerMeasurementConsumersService {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    return SpannerMeasurementConsumersService(spannerDatabaseClient)
  }

  companion object {
    @get:ClassRule @JvmStatic
    val spannerEmulator = SpannerEmulatorRule()
  }
}
