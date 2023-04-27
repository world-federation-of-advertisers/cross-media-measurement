package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.internal.kingdom.ModelSuite

class ModelSuiteReader : SpannerReader<ModelSuiteReader.Result>() {

  data class Result(val modelSuite: ModelSuite, val modelSuiteId: Long)

  override val baseSql: String =
    """
    SELECT
      ModelSuiteId,
      ExternalModelSuiteId,
      DisplayName,
      Description,
      CreateTime
      FROM ModelSuites
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(buildModelSuite(struct), struct.getLong("ModelSuiteId"))

  private fun buildModelSuite(struct: Struct): ModelSuite =
    ModelSuite.newBuilder()
      .apply {
        externalModelSuiteId = struct.getLong("ExternalModelSuiteId")
        displayName = struct.getString("DisplayName")
        description = struct.getString("Description")
        createTime = struct.getTimestamp("CreateTime").toProto()
      }
      .build()

  suspend fun readByExternalModelSuiteId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalModelSuiteId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause("WHERE ExternalModelSuiteId = @externalModelSuiteId")
        bind("externalModelSuiteId").to(externalModelSuiteId.value)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }
}
