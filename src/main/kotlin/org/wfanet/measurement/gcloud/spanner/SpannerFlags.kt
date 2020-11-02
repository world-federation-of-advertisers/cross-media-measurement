package org.wfanet.measurement.gcloud.spanner

import java.time.Duration
import picocli.CommandLine

/**
 * Common command-line flags for connecting to a single Spanner database.
 */
class SpannerFlags {
  @CommandLine.Option(
    names = ["--spanner-project"],
    description = ["Name of the Spanner project."],
    required = true
  )
  lateinit var projectName: String
    private set

  @CommandLine.Option(
    names = ["--spanner-instance"],
    description = ["Name of the Spanner instance."],
    required = true
  )
  lateinit var instanceName: String
    private set

  @CommandLine.Option(
    names = ["--spanner-database"],
    description = ["Name of the Spanner database."],
    required = true
  )
  lateinit var databaseName: String
    private set

  @CommandLine.Option(
    names = ["--spanner-ready-timeout"],
    description = ["How long to wait for Spanner to be ready."],
    defaultValue = "10s"
  )
  lateinit var readyTimeout: Duration
    private set

  @CommandLine.Option(
    names = ["--spanner-emulator-host"],
    description = ["Host name and port of the spanner emulator."],
    required = false
  )
  var spannerEmulatorHost: String? = null
    private set

  /**
   * Builds a [SpannerDatabaseConnector] from these flags.
   */
  private fun toSpannerDatabaseConnector(): SpannerDatabaseConnector {
    return SpannerDatabaseConnector(
      instanceName,
      projectName,
      readyTimeout,
      databaseName,
      spannerEmulatorHost
    )
  }

  /**
   * Executes [block] with a [SpannerDatabaseConnector] resource once it's ready,
   * ensuring that the resource is closed.
   */
  suspend fun <R> usingSpanner(block: suspend (spanner: SpannerDatabaseConnector) -> R): R {
    return toSpannerDatabaseConnector().usingSpanner(block)
  }
}
