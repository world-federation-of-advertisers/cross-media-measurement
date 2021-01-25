// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner

import org.junit.Before
import org.junit.Rule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.duchy.db.metricvalue.testing.AbstractMetricValueDatabaseTest
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.testing.METRIC_VALUES_SCHEMA
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule

/** [SpannerMetricValueDatabase] test. */
@RunWith(JUnit4::class)
class SpannerMetricValueDatabaseTest :
  AbstractMetricValueDatabaseTest<SpannerMetricValueDatabase>() {

  @Rule
  @JvmField
  val spannerDatabase = SpannerEmulatorDatabaseRule(METRIC_VALUES_SCHEMA)

  val databaseClient: AsyncDatabaseClient
    get() = spannerDatabase.databaseClient

  @Before fun initMetricValueDb() {
    fixedIdGenerator = FixedIdGenerator()
    metricValueDb = SpannerMetricValueDatabase(databaseClient, fixedIdGenerator)
  }
}
