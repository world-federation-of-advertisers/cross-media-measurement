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

package org.wfanet.measurement.db.duchy.metricvalue.gcp

import org.junit.Before
import org.junit.Rule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.db.duchy.metricvalue.testing.AbstractMetricValueDatabaseTest
import org.wfanet.measurement.db.gcp.AsyncDatabaseClient
import org.wfanet.measurement.db.gcp.testing.SpannerEmulatorDatabaseRule

private const val SCHEMA_RESOURCE_PATH = "/src/main/db/gcp/metric_values.sdl"

/** [SpannerMetricValueDatabase] test. */
@RunWith(JUnit4::class)
class SpannerMetricValueDatabaseTest :
  AbstractMetricValueDatabaseTest<SpannerMetricValueDatabase>() {

  @Rule
  @JvmField
  val spannerDatabase = SpannerEmulatorDatabaseRule(SCHEMA_RESOURCE_PATH)

  val databaseClient: AsyncDatabaseClient
    get() = spannerDatabase.databaseClient

  @Before fun initMetricValueDb() {
    fixedIdGenerator = FixedIdGenerator()
    metricValueDb = SpannerMetricValueDatabase(databaseClient, fixedIdGenerator)
  }
}
