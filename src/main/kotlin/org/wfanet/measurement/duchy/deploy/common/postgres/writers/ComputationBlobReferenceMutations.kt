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

import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationStageAttemptDetails

/** Inserts a new row into the Postgres ComputationBlobReferences table */
suspend fun PostgresWriter.TransactionScope.insertComputationBlobReference(
  localId: Long,
  stage: Long,
  blobId: Long,
  pathToBlob: String?,
  dependencyType: ComputationBlobDependency,
): ComputationStageAttemptDetails? {
  val sql =
    boundStatement(
      """
        INSERT INTO ComputationBlobReferences
        (
          ComputationId,
          ComputationStage,
          BlobId,
          PathToBlob,
          DependencyType
        )
        VALUES ($1, $2, $3, $4, $5)
      """
        .trimIndent()
    ) {
      bind("$1", localId)
      bind("$2", stage)
      bind("$3", blobId)
      bind("$4", pathToBlob)
      bind("$5", dependencyType.numberAsLong)
    }
  return transactionContext
    .executeQuery(sql)
    .consume { row -> row.getProtoMessage("Details", ComputationStageAttemptDetails.parser()) }
    .firstOrNull()
}

/**
 * Updates a row in the Postgres ComputationBlobReferences table.
 *
 * If an argument is null, its corresponding field in the database will not be updated.
 */
suspend fun PostgresWriter.TransactionScope.updateComputationBlobReference(
  localId: Long,
  stage: Long,
  blobId: Long,
  pathToBlob: String? = null,
  dependencyType: ComputationBlobDependency? = null,
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
