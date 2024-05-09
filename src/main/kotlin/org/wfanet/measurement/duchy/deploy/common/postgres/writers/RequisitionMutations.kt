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

package org.wfanet.measurement.duchy.deploy.common.postgres.writers

import com.google.protobuf.ByteString
import java.time.Instant
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.internal.duchy.RequisitionDetails

/** Inserts a new row into the Postgres Requisitions table. */
suspend fun PostgresWriter.TransactionScope.insertRequisition(
  localComputationId: Long,
  requisitionId: Long,
  externalRequisitionId: String,
  requisitionFingerprint: ByteString,
  creationTime: Instant,
  updateTime: Instant,
  pathToBlob: String? = null,
  requisitionDetails: RequisitionDetails = RequisitionDetails.getDefaultInstance(),
) {
  val sql =
    boundStatement(
      """
      INSERT INTO Requisitions
        (
          ComputationId,
          RequisitionId,
          ExternalRequisitionId,
          RequisitionFingerprint,
          PathToBlob,
          RequisitionDetails,
          RequisitionDetailsJSON,
          CreationTime,
          UpdateTime
        )
      VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
      """
    ) {
      bind("$1", localComputationId)
      bind("$2", requisitionId)
      bind("$3", externalRequisitionId)
      bind("$4", requisitionFingerprint.toByteArray())
      bind("$5", pathToBlob)
      bind("$6", requisitionDetails.toByteArray())
      bind("$7", requisitionDetails.toJson())
      bind("$8", creationTime)
      bind("$9", updateTime)
    }

  transactionContext.executeStatement(sql)
}

/**
 * Updates a row in the Postgres Requisitions table.
 *
 * If an argument is null, its corresponding field in the database will not be updated.
 */
suspend fun PostgresWriter.TransactionScope.updateRequisition(
  localComputationId: Long,
  requisitionId: Long,
  externalRequisitionId: String,
  requisitionFingerprint: ByteString,
  updateTime: Instant,
  pathToBlob: String? = null,
  requisitionDetails: RequisitionDetails? = null,
) {
  val sql =
    boundStatement(
      """
      UPDATE Requisitions SET
        PathToBlob = COALESCE($1, PathToBlob),
        RequisitionDetails = COALESCE($2, RequisitionDetails),
        RequisitionDetailsJSON = COALESCE($3::jsonb, RequisitionDetailsJSON),
        UpdateTime = $4
      WHERE
        ComputationId = $5
      AND
        RequisitionId = $6
      AND
        ExternalRequisitionId = $7
      AND
        RequisitionFingerprint = $8
    """
        .trimIndent()
    ) {
      bind("$1", pathToBlob)
      bind("$2", requisitionDetails?.toByteArray())
      bind("$3", requisitionDetails?.toJson())
      bind("$4", updateTime)
      bind("$5", localComputationId)
      bind("$6", requisitionId)
      bind("$7", externalRequisitionId)
      bind("$8", requisitionFingerprint.toByteArray())
    }

  transactionContext.executeStatement(sql)
}
