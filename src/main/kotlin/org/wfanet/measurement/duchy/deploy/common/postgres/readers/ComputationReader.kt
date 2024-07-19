// Copyright 2023 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.duchy.deploy.common.postgres.readers

import com.google.protobuf.Timestamp
import java.time.Instant
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.toSet
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.ValuesListBoundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.valuesListBoundStatement
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationStageLongValues
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ExternalRequisitionKey
import org.wfanet.measurement.internal.duchy.RequisitionMetadata
import org.wfanet.measurement.internal.duchy.computationToken

/**
 * Performs read operations on Computations and ComputationStages tables
 *
 * @param computationProtocolStagesEnumHelper [ComputationProtocolStagesEnumHelper] a helper class
 *   to work with Enum representations of [ComputationType] and [ComputationStage].
 */
class ComputationReader(
  private val computationProtocolStagesEnumHelper:
    ComputationProtocolStagesEnumHelper<ComputationType, ComputationStage>
) {

  private val blobReferenceReader = ComputationBlobReferenceReader()
  private val requisitionReader = RequisitionReader()

  data class Computation(
    val globalComputationId: String,
    val localComputationId: Long,
    val protocol: Long,
    val computationStage: Long,
    val nextAttempt: Int,
    val computationDetails: ComputationDetails,
    val version: Long,
    val stageSpecificDetails: ComputationStageDetails?,
    val lockOwner: String?,
    val lockExpirationTime: Timestamp?,
  ) {
    constructor(
      row: ResultRow
    ) : this(
      globalComputationId = row["GlobalComputationId"],
      localComputationId = row["ComputationId"],
      protocol = row["Protocol"],
      computationStage = row["ComputationStage"],
      nextAttempt = row["NextAttempt"],
      computationDetails = row.getProtoMessage("ComputationDetails", ComputationDetails.parser()),
      version = row.get<Instant>("UpdateTime").toEpochMilli(),
      stageSpecificDetails = row.getProtoMessage("StageDetails", ComputationStageDetails.parser()),
      lockOwner = row["LockOwner"],
      lockExpirationTime = row.get<Instant?>("LockExpirationTime")?.toProtoTime(),
    )
  }

  data class UnclaimedTaskQueryResult(
    val computationId: Long,
    val globalId: String,
    val computationStage: Long,
    val creationTime: Instant,
    val updateTime: Instant,
    val nextAttempt: Long,
  )

  private fun buildUnclaimedTaskQueryResult(row: ResultRow): UnclaimedTaskQueryResult =
    UnclaimedTaskQueryResult(
      row["ComputationId"],
      row["GlobalComputationId"],
      row["ComputationStage"],
      row["CreationTime"],
      row["UpdateTime"],
      row["NextAttempt"],
    )

  data class LockOwnerQueryResult(val lockOwner: String?, val updateTime: Instant) {
    constructor(row: ResultRow) : this(lockOwner = row["LockOwner"], updateTime = row["UpdateTime"])
  }

  private fun buildComputationToken(
    computation: Computation,
    blobs: List<ComputationStageBlobMetadata>,
    requisitions: List<RequisitionMetadata>,
  ): ComputationToken {
    return computationToken {
      globalComputationId = computation.globalComputationId
      localComputationId = computation.localComputationId
      computationStage =
        computationProtocolStagesEnumHelper.longValuesToComputationStageEnum(
          ComputationStageLongValues(computation.protocol, computation.computationStage)
        )
      attempt = computation.nextAttempt - 1
      computationDetails = computation.computationDetails
      version = computation.version
      computation.stageSpecificDetails?.let { stageSpecificDetails = it }
      computation.lockOwner?.let { lockOwner = it }
      computation.lockExpirationTime?.let { lockExpirationTime = it }

      if (blobs.isNotEmpty()) {
        this.blobs += blobs
      }

      if (requisitions.isNotEmpty()) {
        this.requisitions += requisitions
      }
    }
  }

  private suspend fun readComputation(
    readContext: ReadContext,
    globalComputationId: String,
  ): Computation? {
    val statement =
      boundStatement(
        """
      SELECT
        c.ComputationId,
        c.GlobalComputationId,
        c.LockOwner,
        c.LockExpirationTime,
        c.ComputationStage,
        c.ComputationDetails,
        c.Protocol,
        c.UpdateTime,
        cs.NextAttempt,
        cs.Details AS StageDetails
      FROM Computations AS c
      JOIN ComputationStages AS cs
        ON c.ComputationId = cs.ComputationId AND c.ComputationStage = cs.ComputationStage
      WHERE c.GlobalComputationId = $1
      """
      ) {
        bind("$1", globalComputationId)
      }
    return readContext.executeQuery(statement).consume(::Computation).firstOrNull()
  }

  private suspend fun readComputation(
    readContext: ReadContext,
    externalRequisitionKey: ExternalRequisitionKey,
  ): Computation? {
    val statement =
      boundStatement(
        """
      SELECT
        c.ComputationId,
        c.GlobalComputationId,
        c.LockOwner,
        c.LockExpirationTime,
        c.ComputationStage,
        c.ComputationDetails,
        c.Protocol,
        c.UpdateTime,
        cs.NextAttempt,
        cs.Details AS StageDetails
      FROM Computations AS c
      JOIN ComputationStages AS cs
        ON c.ComputationId = cs.ComputationId AND c.ComputationStage = cs.ComputationStage
      JOIN Requisitions AS r
        ON c.ComputationId = r.ComputationId
      WHERE r.ExternalRequisitionId = $1
        AND r.RequisitionFingerprint = $2
      """
      ) {
        bind("$1", externalRequisitionKey.externalRequisitionId)
        bind("$2", externalRequisitionKey.requisitionFingerprint.toByteArray())
      }
    return readContext.executeQuery(statement).consume(::Computation).firstOrNull()
  }

  /**
   * Reads a [ComputationToken] by globalComputationId.
   *
   * @param readContext The [ReadContext] for reading from the Postgres database.
   * @param globalComputationId A global identifier for a computation.
   * @return [ComputationToken] when a Computation with globalComputationId is found, or null.
   */
  suspend fun readComputationToken(
    readContext: ReadContext,
    globalComputationId: String,
  ): ComputationToken? {
    val computation: Computation = readComputation(readContext, globalComputationId) ?: return null

    val blobs =
      blobReferenceReader.readBlobMetadata(
        readContext,
        computation.localComputationId,
        computation.computationStage,
      )
    val requisitions =
      requisitionReader.readRequisitionMetadata(readContext, computation.localComputationId)

    return buildComputationToken(computation, blobs, requisitions)
  }

  /**
   * Reads a [ComputationToken] by globalComputationId in a new transaction.
   *
   * @param client The [DatabaseClient] to the Postgres database.
   * @param globalComputationId A global identifier for a computation.
   * @return [ComputationToken] when a Computation with globalComputationId is found, or null.
   */
  suspend fun readComputationToken(
    client: DatabaseClient,
    globalComputationId: String,
  ): ComputationToken? {
    val readContext = client.readTransaction()
    try {
      return readComputationToken(readContext, globalComputationId)
    } finally {
      readContext.close()
    }
  }

  /**
   * Reads a [ComputationToken] by externalRequisitionKey.
   *
   * @param readContext The [ReadContext] for reading from the Postgres database.
   * @param externalRequisitionKey The [ExternalRequisitionKey] for a computation.
   * @return [ComputationToken] when a Computation with externalRequisitionKey is found, or null.
   */
  suspend fun readComputationToken(
    readContext: ReadContext,
    externalRequisitionKey: ExternalRequisitionKey,
  ): ComputationToken? {
    val computation = readComputation(readContext, externalRequisitionKey) ?: return null

    val blobs =
      blobReferenceReader.readBlobMetadata(
        readContext,
        computation.localComputationId,
        computation.computationStage,
      )
    val requisitions =
      requisitionReader.readRequisitionMetadata(readContext, computation.localComputationId)

    return buildComputationToken(computation, blobs, requisitions)
  }

  /**
   * Reads a [ComputationToken] by externalRequisitionKey in a new transaction.
   *
   * @param client The [DatabaseClient] to the Postgres database.
   * @param externalRequisitionKey The [ExternalRequisitionKey] for a computation.
   * @return [ComputationToken] when a Computation with externalRequisitionKey is found, or null.
   */
  suspend fun readComputationToken(
    client: DatabaseClient,
    externalRequisitionKey: ExternalRequisitionKey,
  ): ComputationToken? {
    val readContext = client.readTransaction()
    try {
      return readComputationToken(readContext, externalRequisitionKey)
    } finally {
      readContext.close()
    }
  }

  /**
   * Reads a set of globalComputationIds
   *
   * @param readContext The transaction context for reading from the Postgres database.
   * @param stages A list of stage's long values
   * @param updatedBefore An [Instant] to filter for the computations that has been updated before
   *   this
   * @return A set of global computation Ids
   */
  suspend fun readGlobalComputationIds(
    readContext: ReadContext,
    stages: List<ComputationStage>,
    updatedBefore: Instant? = null,
  ): Set<String> {
    val computationTypes =
      stages.map { computationProtocolStagesEnumHelper.stageToProtocol(it) }.distinct()
    grpcRequire(computationTypes.count() == 1) {
      "All stages should have the same ComputationType."
    }

    /**
     * Binding list of String into the IN clause does not work as expected with r2dbc library.
     * Hence, manually joining targeting stages into a comma separated string and stub it into the
     * query.
     */
    val stagesString =
      stages
        .map { computationProtocolStagesEnumHelper.computationStageEnumToLongValues(it).stage }
        .toList()
        .joinToString(",")
    val baseSql =
      """
        SELECT GlobalComputationId
        FROM Computations
        WHERE
          ComputationStage IN ($stagesString)
        AND
          Protocol = $1
      """

    val query =
      if (updatedBefore == null) {
        boundStatement(baseSql) { bind("$1", computationTypes[0]) }
      } else {
        boundStatement("$baseSql AND UpdateTime <= $2") {
          bind("$1", computationTypes[0])
          bind("$2", updatedBefore)
        }
      }

    return readContext
      .executeQuery(query)
      .consume { row -> row.get<String>("GlobalComputationId") }
      .toSet()
  }

  /**
   * Reads a list of unclaimed computation tasks
   *
   * @param readContext The transaction context for reading from the Postgres database.
   * @param protocol The enum value of target computation type.
   * @param timestamp An [Instant] to filter for the expired computation locks.
   * @return a flow of [UnclaimedTaskQueryResult]
   */
  suspend fun listUnclaimedTasks(
    readContext: ReadContext,
    protocol: Long,
    timestamp: Instant,
    prioritizedStages: List<ComputationStage>,
  ): Flow<UnclaimedTaskQueryResult> {
    val baseSql =
      """
      SELECT c.ComputationId,  c.GlobalComputationId,
             c.Protocol, c.ComputationStage, c.UpdateTime,
             c.CreationTime, cs.NextAttempt
      FROM Computations AS c
        JOIN ComputationStages AS cs
      ON c.ComputationId = cs.ComputationId
        AND c.ComputationStage = cs.ComputationStage
      WHERE c.Protocol = $1
        AND c.LockExpirationTime IS NOT NULL
        AND c.LockExpirationTime <= $2
      """

    if (prioritizedStages.isEmpty()) {
      val baseSqlWithOrder =
        baseSql +
          """
        ORDER BY c.CreationTime ASC, c.LockExpirationTime ASC, c.UpdateTime ASC
        LIMIT 50;
        """
      val statement =
        boundStatement(baseSqlWithOrder) {
          bind("$1", protocol)
          bind("$2", timestamp)
        }

      return readContext.executeQuery(statement).consume(::buildUnclaimedTaskQueryResult)
    } else {
      val baseSqlWithOrder =
        baseSql +
          """
        ORDER BY
          CASE WHEN c.ComputationStage
            IN (VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}) THEN 0
          ELSE 1 END ASC,
        c.CreationTime ASC, c.LockExpirationTime ASC, c.UpdateTime ASC
        LIMIT 50;
        """
      val statement =
        valuesListBoundStatement(valuesStartIndex = 2, paramCount = 1, baseSqlWithOrder) {
          bind("$1", protocol)
          bind("$2", timestamp)
          for (stage in prioritizedStages) {
            val longValue =
              computationProtocolStagesEnumHelper.computationStageEnumToLongValues(stage).stage
            addValuesBinding { bindValuesParam(0, longValue) }
          }
        }
      return readContext.executeQuery(statement).consume(::buildUnclaimedTaskQueryResult)
    }
  }

  /**
   * Reads the LockOwner and UpdateTime of a computation.
   *
   * @param readContext The transaction context for reading from the Postgres database.
   * @param computationId The local identifier for a computation.
   * @return [LockOwnerQueryResult] if computation is found, otherwise null.
   */
  suspend fun readLockOwner(readContext: ReadContext, computationId: Long): LockOwnerQueryResult? {
    val readLockOwnerSql =
      boundStatement(
        """
      SELECT LockOwner, UpdateTime
      FROM Computations
      WHERE
        ComputationId = $1;
      """
      ) {
        bind("$1", computationId)
      }
    return readContext.executeQuery(readLockOwnerSql).consume(::LockOwnerQueryResult).firstOrNull()
  }
}
