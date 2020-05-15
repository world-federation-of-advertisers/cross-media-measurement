package org.wfanet.measurement.db.gcp.testing

import com.google.cloud.spanner.DatabaseAdminClient
import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.InstanceAdminClient
import com.google.cloud.spanner.InstanceConfigId
import com.google.cloud.spanner.InstanceId
import com.google.cloud.spanner.InstanceInfo
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.SpannerOptions
import java.util.concurrent.atomic.AtomicInteger
import org.junit.ClassRule
import org.junit.Rule
import org.junit.rules.ExternalResource

/**
 * Test case base class for running against an emulated Spanner backend.
 *
 * This class assumes the emulator running a gRPC service at $SPANNER_EMULATOR_HOST
 *
 * Example use:
 *
 * class MySpannerTest : UsingSpannerEmulator("/path/to/spanner/schema/resource") {
 *   [@Test]
 *   fun `some database test`() {
 *     val dbClient = spanner.client
 *     // Do stuff with dbClient..
 *   }
 * }
 */
abstract class UsingSpannerEmulator(schemaResourcePath: String) {
  @get:Rule
  val spanner = SpannerDatabaseRule(
    schemaResourcePath,
    project = project,
    instance = instance
  )

  companion object {
    const val instance = "test-instance"
    private const val project = "test-project"
    internal val options = SpannerOptions.newBuilder()
      .setProjectId(project)
      .setEmulatorHost(
        System.getenv("SPANNER_EMULATOR_HOST")
          ?: error("SPANNER_EMULATOR_HOST not set. (e.g. localhost:9010)")
      )
      .build()

    /** Executes a block of code with a [InstanceAdminClient]. */
    private fun withSpannerInstanceAdminClient(block: (InstanceAdminClient) -> Unit) {
      val spanner = options.service
      try {
        block(spanner.instanceAdminClient)
      } finally {
        spanner.close()
      }
    }

    @ClassRule
    @JvmField
      /**
       * Creates a test instance in the spanner emulator before running tests and deletes it
       * after all tests.
       */
    val spannerInstanceClassRule = object : ExternalResource() {

      override fun before() {
        withSpannerInstanceAdminClient {

          it.createInstance(
            InstanceInfo.newBuilder(InstanceId.of(project, instance))
              .setDisplayName("Test Instance")
              .setInstanceConfigId(InstanceConfigId.of(project, "emulator-config"))
              .setNodeCount(1)
              .build()
          ).get()
        }
      }

      override fun after() {
        withSpannerInstanceAdminClient {
          it.deleteInstance(instance)
        }
      }
    }
  }
}

/**
 * JUnit test rule which creates a spanner database from a sdl schema file in a emulated
 * test instance.
 *
 * The database is created before each test and removed after each test. This prevents the state
 * of one test from affecting the results of another.
 */
class SpannerDatabaseRule(
  private val sdlResourcePath: String,
  val project: String = "test-project",
  val instance: String = "test-instance"
) : ExternalResource() {

  private val dbName: String = "test-db-${testCounter.incrementAndGet()}"

  val spanner: Spanner = UsingSpannerEmulator.options.service
  private val dbAdminClient: DatabaseAdminClient = spanner.databaseAdminClient
  val databaseId: DatabaseId = DatabaseId.of(project, instance, dbName)
  val client: DatabaseClient = spanner.getDatabaseClient(databaseId)

  /** Creates a database using the schema before a test is run. */
  override fun before() {
    dbAdminClient.createDatabase(instance, dbName, readEmulatorSchema(sdlResourcePath))?.get()
  }

  /** Drop the test database after test. */
  override fun after() {
    dbAdminClient.dropDatabase(instance, dbName)
    spanner.close()
  }

  companion object {
    /** Atomic counter to ensure each test is given its own database to run against. */
    private val testCounter: AtomicInteger = AtomicInteger(0)
  }
}

/** Reads a spanner schema file and transforms it into a list of operations to creat a database. */
private fun readEmulatorSchema(resourcePath: String): List<String> {
  return resourcePath.javaClass::class.java.getResource(resourcePath).readText()
    .split('\n')
    // Replace comments and refrences to foreign keys from schema file.
    .map { it.replace("""(--|CONSTRAINT|FOREIGN|REFERENCES).*$""".toRegex(), "") }
    // Delete blank lines
    .filter { it.isNotBlank() }
    // Rejoin to single sting
    .joinToString("\n")
    // and split into operations
    .split(';')
    // Removing any blank operations
    .filter { it.isNotBlank() }
}
