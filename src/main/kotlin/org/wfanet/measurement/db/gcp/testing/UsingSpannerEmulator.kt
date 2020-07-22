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

package org.wfanet.measurement.db.gcp.testing

import com.google.cloud.spanner.DatabaseClient
import org.junit.ClassRule
import org.junit.Rule

/**
 * Base class for JUnit4 tests using Cloud Spanner databases running in a test
 * [Instance][com.google.cloud.spanner.Instance] in Cloud Spanner Emulator.
 *
 * One emulator and test instance is created per test class, and one database is created per test
 * case method. The [DatabaseClient] is accessible via the [databaseClient] property.
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
abstract class UsingSpannerEmulator(schemaResourcePath: String) {
  @Rule
  @JvmField
  val spannerDatabase = SpannerEmulatorDatabaseRule(spannerEmulator.instance, schemaResourcePath)

  val databaseClient: DatabaseClient
    get() = spannerEmulator.getDatabaseClient(spannerDatabase.databaseId)

  companion object {
    @ClassRule
    @JvmField
    val spannerEmulator = SpannerEmulatorRule()
  }
}
