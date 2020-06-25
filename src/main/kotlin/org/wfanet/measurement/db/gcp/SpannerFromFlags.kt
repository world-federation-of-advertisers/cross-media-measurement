package org.wfanet.measurement.db.gcp

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.SpannerOptions
import org.wfanet.measurement.common.Flag
import org.wfanet.measurement.common.stringFlag

class SpannerFromFlags(
  projectFlag: String = "spanner-project",
  instanceFlag: String = "spanner-instance",
  databaseFlag: String = "spanner-database"
) {
  private var project: Flag<String> = stringFlag(projectFlag, default = "")
  private var instance: Flag<String> = stringFlag(instanceFlag, default = "")
  private var database: Flag<String> = stringFlag(databaseFlag, default = "")

  val spannerOptions: SpannerOptions by lazy {
    SpannerOptions.newBuilder()
      .setProjectId(project.value)
      .build()
  }

  val spanner: Spanner by lazy { spannerOptions.service }

  val databaseId: DatabaseId by lazy {
    DatabaseId.of(project.value, instance.value, database.value)
  }

  val databaseClient: DatabaseClient by lazy {
    spanner.getDatabaseClient(databaseId)
  }
}
