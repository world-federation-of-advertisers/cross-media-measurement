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

package org.wfanet.measurement.gcloud.spanner.testing

import com.google.cloud.spanner.Statement
import java.time.Instant
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.wfanet.measurement.gcloud.common.toInstant
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient

/**
 * Base class for JUnit4 tests using Cloud Spanner databases running in a test
 * [Instance][com.google.cloud.spanner.Instance] in Cloud Spanner Emulator.
 *
 * One emulator and test instance is created per test class, and one database is
 * created per test case method. The [AsyncDatabaseClient] is accessible via the
 * [databaseClient] property.
 *
 * Example use:
 * ```
 * class MySpannerTest : UsingSpannerEmulator("/path/to/spanner/schema/resource") {
 *   @Test
 *   fun mutationsGonnaMutate() {
 *     databaseClient.write(buildMutations())
 *
 *     val result = databaseClient.singleUse().executeQuery(buildQuery)
 *     // Make assertions about the results
 *     // ...
 *   }
 * }
 * ```
 */
abstract class UsingSpannerEmulator(schema: SpannerSchema) {
  @get:Rule
  val spannerDatabase = SpannerEmulatorDatabaseRule(schema)

  val databaseClient: AsyncDatabaseClient
    get() = spannerDatabase.databaseClient

  val currentSpannerTimestamp: Instant
    get() = runBlocking { getCurrentSpannerTimestamp() }

  suspend fun getCurrentSpannerTimestamp(): Instant {
    return databaseClient.singleUse()
      .executeQuery(Statement.of("SELECT CURRENT_TIMESTAMP()"))
      .single()
      .getTimestamp(0)
      .toInstant()
  }
}
