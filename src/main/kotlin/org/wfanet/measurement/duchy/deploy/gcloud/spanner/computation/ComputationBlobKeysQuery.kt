package org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.common.SqlBasedQuery

class ComputationBlobKeysQuery(localId: Long) : SqlBasedQuery<String> {
  companion object {
    private val parameterizedQueryString =
      """
        SELECT PathToBlob,
        FROM ComputationBlobReferences
        WHERE ComputationId = @local_id
      """.trimIndent()
  }

  override val sql: Statement = Statement.newBuilder(parameterizedQueryString).bind("local_id").to(localId).build()

  override fun asResult(struct: Struct): String = struct.getString("PathToBlob")
}
