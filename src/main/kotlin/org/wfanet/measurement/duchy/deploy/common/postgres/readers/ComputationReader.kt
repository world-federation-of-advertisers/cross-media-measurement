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
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
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
import org.wfanet.measurement.internal.duchy.RequisitionDetails
import org.wfanet.measurement.internal.duchy.RequisitionMetadata
import org.wfanet.measurement.internal.duchy.computationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.internal.duchy.externalRequisitionKey
import org.wfanet.measurement.internal.duchy.requisitionMetadata

/**
 * Performs read operations on Computations tables
 *
 * @param computationProtocolStagesEnumHelper [ComputationProtocolStagesEnumHelper] a helper class
 *   to work with Enum representations of [ComputationType] and [ComputationStage].
 */
class ComputationReader(
  private val computationProtocolStagesEnumHelper:
    ComputationProtocolStagesEnumHelper<ComputationType, ComputationStage>
) {

  private data class Computation(
    val globalComputationId: String,
    val localComputationId: Long,
    val protocol: Long,
    val computationStage: Long,
    val nextAttempt: Int,
    val computationDetails: ComputationDetails,
    val version: Long,
    val stageSpecificDetails: ComputationStageDetails?,
    val lockOwner: String?,
    val lockExpirationTime: Timestamp?
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
      lockExpirationTime = row.get<Instant>("LockExpirationTime").toProtoTime()
    )
  }

  private fun buildBlob(row: ResultRow): ComputationStageBlobMetadata {
    return computationStageBlobMetadata {
      blobId = row["BlobId"]
      path = row["PathToBlob"]
      dependencyType = row["DependencyType"]
    }
  }

  private fun buildRequisition(row: ResultRow): RequisitionMetadata {
    return requisitionMetadata {
      externalKey = externalRequisitionKey {
        externalRequisitionId = row["ExternalRequisitionId"]
        requisitionFingerprint = row["RequisitionFingerprint"]
      }
      row.get<String?>("PathToBlob")?.let { path = it }
      details = row.getProtoMessage("RequisitionDetails", RequisitionDetails.parser())
    }
  }

  private fun buildComputationToken(
    computation: Computation,
    blobs: List<ComputationStageBlobMetadata>,
    requisitions: List<RequisitionMetadata>
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

  private suspend fun readBlobs(
    readContext: ReadContext,
    localComputationId: Long,
    computationStage: Long
  ): List<ComputationStageBlobMetadata> {
    val statement =
      boundStatement(
        """
        SELECT BlobId, PathToBlob, DependencyType
        FROM ComputationBlobReferences
        WHERE
          ComputationId = $1
        AND
          ComputationStage = $2
      """
          .trimIndent()
      ) {
        bind("$1", localComputationId)
        bind("$2", computationStage)
      }

    return readContext.executeQuery(statement).consume(::buildBlob).toList()
  }

  private suspend fun readRequisitions(
    readContext: ReadContext,
    localComputationId: Long,
  ): List<RequisitionMetadata> {
    val statement =
      boundStatement(
        """
      SELECT
        ExternalRequisitionId, RequisitionFingerprint, PathToBlob, RequisitionDetails
      FROM Requisitions
        WHERE ComputationId = $1
      """
          .trimIndent()
      ) {
        bind("$1", localComputationId)
      }

    return readContext.executeQuery(statement).consume(::buildRequisition).toList()
  }

  private suspend fun readComputation(
    readContext: ReadContext,
    globalComputationId: String
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
    return readContext.executeQuery(statement).consume(ComputationReader::Computation).firstOrNull()
  }

  private suspend fun readComputation(
    readContext: ReadContext,
    externalRequisitionKey: ExternalRequisitionKey
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
        bind("$2", externalRequisitionKey.requisitionFingerprint)
      }
    return readContext.executeQuery(statement).consume(ComputationReader::Computation).firstOrNull()
  }

  /**
   * Gets a [ComputationToken] by globalComputationId.
   *
   * @param readContext The transaction context for reading from the Postgres database.
   * @param globalComputationId A global identifier for a computation.
   * @return [ReadComputationTokenResult] when a Computation with globalComputationId is found.
   * @return null otherwise.
   */
  suspend fun readComputationToken(
    client: DatabaseClient,
    globalComputationId: String
  ): ComputationToken? {
    val readContext = client.readTransaction()
    try {
      val computation: Computation =
        readComputation(readContext, globalComputationId) ?: return null

      val blobs =
        readBlobs(readContext, computation.localComputationId, computation.computationStage)
      val requisitions = readRequisitions(readContext, computation.localComputationId)

      return buildComputationToken(computation, blobs, requisitions)
    } finally {
      readContext.close()
    }
  }

  /**
   * Gets a [ComputationToken] by externalRequisitionKey.
   *
   * @param readContext The transaction context for reading from the Postgres database.
   * @param externalRequisitionKey The [ExternalRequisitionKey] for a computation.
   * @return [ReadComputationTokenResult] when a Computation with externalRequisitionKey is found.
   * @return null otherwise.
   */
  suspend fun readComputationToken(
    client: DatabaseClient,
    externalRequisitionKey: ExternalRequisitionKey
  ): ComputationToken? {
    val readContext = client.readTransaction()
    try {
      val computation = readComputation(readContext, externalRequisitionKey) ?: return null

      val blobs =
        readBlobs(readContext, computation.localComputationId, computation.computationStage)
      val requisitions = readRequisitions(readContext, computation.localComputationId)

      return buildComputationToken(computation, blobs, requisitions)
    } finally {
      readContext.close()
    }
  }
}
