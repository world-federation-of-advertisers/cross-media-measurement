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

import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.internal.duchy.ExternalRequisitionKey
import org.wfanet.measurement.internal.duchy.RequisitionDetails
import org.wfanet.measurement.internal.duchy.RequisitionMetadata
import org.wfanet.measurement.internal.duchy.externalRequisitionKey
import org.wfanet.measurement.internal.duchy.requisitionMetadata

/** Performs read operations on Requisitions tables */
class RequisitionReader {
  data class RequisitionResult(
    val computationId: Long,
    val requisitionId: Long,
    val requisitionDetails: RequisitionDetails,
  ) {
    constructor(
      row: ResultRow
    ) : this(
      computationId = row.get<Long>("ComputationId"),
      requisitionId = row.get<Long>("RequisitionId"),
      requisitionDetails = row.getProtoMessage("RequisitionDetails", RequisitionDetails.parser()),
    )
  }

  /**
   * Reads a set of globalComputationIds
   *
   * @param readContext The transaction context for reading from the Postgres database.
   * @param key [ExternalRequisitionKey] external requisition key that identifies a requisition.
   * @return [RequisitionResult] when the target requisition is found, or null.
   */
  suspend fun readRequisitionByExternalKey(
    readContext: ReadContext,
    key: ExternalRequisitionKey,
  ): RequisitionResult? {
    val sql =
      boundStatement(
        """
        SELECT ComputationId, RequisitionId, RequisitionDetails
        FROM Requisitions
        WHERE
          ExternalRequisitionId = $1
        AND
          RequisitionFingerprint = $2
      """
          .trimIndent()
      ) {
        bind("$1", key.externalRequisitionId)
        bind("$2", key.requisitionFingerprint.toByteArray())
      }
    return readContext.executeQuery(sql).consume(RequisitionReader::RequisitionResult).firstOrNull()
  }

  /**
   * Gets a list of requisitionBlobKeys by localComputationId
   *
   * @param readContext The transaction context for reading from the Postgres database.
   * @param localComputationId A local identifier for a computation
   * @return A list of requisition blob keys
   */
  suspend fun readRequisitionBlobKeys(
    readContext: ReadContext,
    localComputationId: Long,
  ): List<String> {
    val statement =
      boundStatement(
        """
        SELECT PathToBlob
        FROM Requisitions
        WHERE ComputationId = $1 AND PathToBlob IS NOT NULL
      """
          .trimIndent()
      ) {
        bind("$1", localComputationId)
      }

    return readContext
      .executeQuery(statement)
      .consume { row -> row.get<String>("PathToBlob") }
      .toList()
  }

  /**
   * Reads a list of [RequisitionMetadata] by localComputationId
   *
   * @param readContext The transaction context for reading from the Postgres database.
   * @param localComputationId A local identifier for a computation
   * @return A list of requisition blob keys
   */
  suspend fun readRequisitionMetadata(
    readContext: ReadContext,
    localComputationId: Long,
  ): List<RequisitionMetadata> {
    val statement =
      boundStatement(
        """
      SELECT
        ExternalRequisitionId,
        RequisitionFingerprint,
        PathToBlob,
        RequisitionDetails
      FROM Requisitions
        WHERE ComputationId = $1
      """
          .trimIndent()
      ) {
        bind("$1", localComputationId)
      }

    return readContext.executeQuery(statement).consume(::buildRequisitionMetadata).toList()
  }

  private fun buildRequisitionMetadata(row: ResultRow): RequisitionMetadata {
    return requisitionMetadata {
      externalKey = externalRequisitionKey {
        externalRequisitionId = row["ExternalRequisitionId"]
        requisitionFingerprint = row["RequisitionFingerprint"]
      }
      row.get<String?>("PathToBlob")?.let { path = it }
      details = row.getProtoMessage("RequisitionDetails", RequisitionDetails.parser())
    }
  }
}
