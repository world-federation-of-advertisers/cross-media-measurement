package org.wfanet.measurement.db.gcp

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.SpannerOptions
import org.wfanet.measurement.common.Flag
import org.wfanet.measurement.common.stringFlag

class SpannerFromFlags(
  projectFlagName: String = "spanner-project",
  instanceFlagName: String = "spanner-instance",
  databaseFlagName: String = "spanner-database"
) {
  private var projectFlag: Flag<String> = stringFlag(projectFlagName, default = "")
  private var instanceFlag: Flag<String> = stringFlag(instanceFlagName, default = "")
  private var databaseFlag: Flag<String> = stringFlag(databaseFlagName, default = "")

  val projectName: String by lazy { projectFlag.value }
  val instanceName: String by lazy { instanceFlag.value }
  val databaseName: String by lazy { databaseFlag.value }

  val spannerOptions: SpannerOptions by lazy {
    SpannerOptions.newBuilder()
      .setProjectId(projectName)
      .build()
  }

  val spanner: Spanner by lazy { spannerOptions.service }

  val databaseId: DatabaseId by lazy {
    DatabaseId.of(projectName, instanceName, databaseName)
  }

  val databaseClient: DatabaseClient by lazy {
    spanner.getDatabaseClient(databaseId)
  }
}
