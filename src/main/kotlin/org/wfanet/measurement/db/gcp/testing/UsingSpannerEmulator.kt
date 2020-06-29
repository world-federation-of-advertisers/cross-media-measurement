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
