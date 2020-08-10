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

package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.spanner.DatabaseClient
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.testing.FixedIdGenerator
import org.wfanet.measurement.db.duchy.testing.AbstractMetricValueDatabaseTest
import org.wfanet.measurement.db.gcp.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.db.gcp.testing.SpannerEmulatorRule

private const val SCHEMA_RESOURCE_PATH = "/src/main/db/gcp/metric_values.sdl"

/** [SpannerMetricValueDatabase] test. */
@RunWith(JUnit4::class)
class SpannerMetricValueDatabaseTest :
  AbstractMetricValueDatabaseTest<SpannerMetricValueDatabase>() {

  @Rule
  @JvmField
  val spannerDatabase = SpannerEmulatorDatabaseRule(spannerEmulator.instance, SCHEMA_RESOURCE_PATH)
  val databaseClient: DatabaseClient
    get() = spannerEmulator.getDatabaseClient(spannerDatabase.databaseId)

  @Before fun initMetricValueDb() {
    fixedIdGenerator = FixedIdGenerator()
    metricValueDb = SpannerMetricValueDatabase(databaseClient, fixedIdGenerator)
  }

  companion object {
    @ClassRule
    @JvmField
    val spannerEmulator = SpannerEmulatorRule()
  }
}
