package org.wfanet.measurement.db.gcp.testing

import com.google.cloud.spanner.Database
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Instance
import java.util.concurrent.atomic.AtomicInteger
import org.junit.rules.TestRule
import org.wfanet.measurement.common.testing.CloseableResource

/**
 * JUnit rule for [SpannerEmulator] instance.
 */
class SpannerEmulatorDatabaseRule(spannerInstance: Instance, schemaResourcePath: String) :
  DatabaseRule by DatabaseRuleImpl(spannerInstance, schemaResourcePath)

private interface DatabaseRule : TestRule {
  val databaseId: DatabaseId
}

private class DatabaseRuleImpl(spannerInstance: Instance, schemaResourcePath: String) :
  DatabaseRule,
  CloseableResource<TemporaryDatabase>({ TemporaryDatabase(spannerInstance, schemaResourcePath) }) {

  override val databaseId: DatabaseId
    get() = resource.databaseId

  override fun before() {
    resource.init()
  }
}

private class TemporaryDatabase(
  private val spannerInstance: Instance,
  private val schemaResourcePath: String
) : AutoCloseable {
  companion object {
    /** Atomic counter to ensure each test is given its own database to run against. */
    private val testCounter: AtomicInteger = AtomicInteger(0)

    /** Reads a spanner schema file and transforms it into a list of operations to creat a database. */
    private fun readEmulatorSchema(resourcePath: String): List<String> {
      return TemporaryDatabase::class.java.getResource(resourcePath).readText().split('\n')
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
  }

  private lateinit var database: Database
  val databaseId: DatabaseId
    get() = database.id

  fun init() {
    check(!this::database.isInitialized)

    val databaseName: String = "test-db-${testCounter.incrementAndGet()}"
    database =
      spannerInstance.createDatabase(databaseName, readEmulatorSchema(schemaResourcePath)).get()
  }

  override fun close() {
    if (this::database.isInitialized) {
      database.drop()
    }
  }
}
