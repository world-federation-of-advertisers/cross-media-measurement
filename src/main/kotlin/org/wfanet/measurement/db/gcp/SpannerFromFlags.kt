package org.wfanet.measurement.db.gcp

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.SpannerOptions
import picocli.CommandLine

class SpannerFromFlags(
  private val flags: Flags
) {
  val spannerOptions: SpannerOptions by lazy {
    SpannerOptions.newBuilder().setProjectId(flags.projectName).build()
  }

  val spanner: Spanner by lazy { spannerOptions.service }

  val databaseId: DatabaseId by lazy {
    DatabaseId.of(flags.projectName, flags.instanceName, flags.databaseName)
  }

  val databaseClient: DatabaseClient by lazy {
    spanner.getDatabaseClient(databaseId)
  }

  class Flags {
    @CommandLine.Option(
      names = ["--spanner-project"],
      description = ["Name of the Spanner project to use."],
      required = true
    )
    lateinit var projectName: String
      private set

    @CommandLine.Option(
      names = ["--spanner-instance"],
      description = ["Name of the Spanner instance to create."],
      required = true
    )
    lateinit var instanceName: String
      private set

    @CommandLine.Option(
      names = ["--spanner-database"],
      description = ["Name of the Spanner database to create."],
      required = true
    )
    lateinit var databaseName: String
      private set
  }
}
