package org.wfanet.measurement.duchy.deploy.postgres.writers

import java.time.Clock
import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.duchy.db.computation.BlobRef
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency

class RecordOutputBlobPath<ProtocolT, StageT>(
  private val clock: Clock,
  private val localId: Long,
  private val editVersion: Long,
  private val stage: StageT,
  private val blobRef: BlobRef,
  private val protocolStagesEnumHelper: ComputationProtocolStagesEnumHelper<ProtocolT, StageT>,
) : PostgresWriter<Unit>() {
  override suspend fun TransactionScope.runTransaction() {
    require(blobRef.key.isNotBlank()) { "Cannot insert blank path to blob. $blobRef" }

    checkComputationUnmodified(localId, editVersion)

    val stageLongValue = protocolStagesEnumHelper.computationStageEnumToLongValues(stage).stage
    val type: ComputationBlobDependency =
      readBlobDependency(localId, stageLongValue, blobRef.idInRelationalDatabase)
        ?: error(
          "No ComputationBlobReferences row for " +
            "($localId, $stage, ${blobRef.idInRelationalDatabase})"
        )
    require(type == ComputationBlobDependency.OUTPUT) { "Cannot write to $type blob" }

    updateComputation(localId = localId, updateTime = clock.instant())

    updateComputationBlobReference(
      localId = localId,
      stage = stageLongValue,
      blobId = blobRef.idInRelationalDatabase,
      pathToBlob = blobRef.key
    )
  }

  private suspend fun TransactionScope.readBlobDependency(
    localId: Long,
    stage: Long,
    blobId: Long,
  ): ComputationBlobDependency? {
    val sql =
      boundStatement(
        """
        SELECT DependencyType
        FROM ComputationBlobReferences
        WHERE
          ComputationId = $1
        AND
          ComputationStage = $2
        AND
          BlobId = $3
      """
          .trimIndent()
      ) {
        bind("$1", localId)
        bind("$2", stage)
        bind("$3", blobId)
      }

    return transactionContext
      .executeQuery(sql)
      .consume { row -> row.getProtoEnum("DependencyType", ComputationBlobDependency::forNumber) }
      .firstOrNull()
  }

  private suspend fun TransactionScope.updateComputationBlobReference(
    localId: Long,
    stage: Long,
    blobId: Long,
    pathToBlob: String? = null,
    dependencyType: ComputationBlobDependency? = null
  ) {
    val sql =
      boundStatement(
        """
        UPDATE ComputationBlobReferences SET
          PathToBlob = COALESCE($4, PathToBlob),
          DependencyType = COALESCE($5, DependencyType)
        WHERE
          ComputationId = $1
        AND
          ComputationStage = $2
        AND
          BlobId = $3
      """
          .trimIndent()
      ) {
        bind("$1", localId)
        bind("$2", stage)
        bind("$3", blobId)
        bind("$4", pathToBlob)
        bind("$5", dependencyType?.numberAsLong)
      }

    transactionContext.executeStatement(sql)
  }
}
