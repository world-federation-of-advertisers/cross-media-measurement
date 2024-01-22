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
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.computationStageBlobMetadata

/** Performs read operations on ComputationBlobReferences tables */
class ComputationBlobReferenceReader {

  /**
   * Reads the [ComputationBlobDependency] of a computation.
   *
   * @param localId local identifier of the computation
   * @param stage stage enum of the computation
   * @param blobId local identifier of the blob
   * @return [ComputationBlobDependency] if the blob exists, or null.
   */
  suspend fun readBlobDependency(
    readContext: ReadContext,
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

    return readContext
      .executeQuery(sql)
      .consume { row -> row.getProtoEnum("DependencyType", ComputationBlobDependency::forNumber) }
      .firstOrNull()
  }

  /**
   * Reads a map of blobId to pathToBlob of a computation based on localComputationId.
   *
   * @param localId local identifier of the computation
   * @param stage stage enum of the computation
   * @param dependencyType enum value of the dependency type
   * @return map of blobId to pathToBlob
   */
  suspend fun readBlobIdToPathMap(
    readContext: ReadContext,
    localId: Long,
    stage: Long,
    dependencyType: Long,
  ): Map<Long, String?> {
    val sql =
      boundStatement(
        """
        SELECT BlobId, PathToBlob
        FROM ComputationBlobReferences
        WHERE
          ComputationId = $1
        AND
          ComputationStage = $2
        AND
          DependencyType = $3
      """
          .trimIndent()
      ) {
        bind("$1", localId)
        bind("$2", stage)
        bind("$3", dependencyType)
      }

    return readContext
      .executeQuery(sql)
      .consume { it.get<Long>("BlobId") to it.get<String?>("PathToBlob") }
      .toList()
      .toMap()
  }

  /**
   * Reads a list of computationBlobKeys by localComputationId
   *
   * @param readContext The transaction context for reading from the Postgres database.
   * @param localComputationId A local identifier for a computation
   * @return A list of computation blob keys
   */
  suspend fun readComputationBlobKeys(
    readContext: ReadContext,
    localComputationId: Long,
  ): List<String> {
    val statement =
      boundStatement(
        """
        SELECT PathToBlob
        FROM ComputationBlobReferences
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
   * Reads a list of [ComputationStageBlobMetadata] by localComputationId
   *
   * @param readContext The transaction context for reading from the Postgres database.
   * @param localComputationId A local identifier for a computation
   * @param computationStage stage enum of the computation
   * @return A list of [ComputationStageBlobMetadata]
   */
  suspend fun readBlobMetadata(
    readContext: ReadContext,
    localComputationId: Long,
    computationStage: Long,
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

    return readContext.executeQuery(statement).consume(::buildBlobMetadata).toList()
  }

  private fun buildBlobMetadata(row: ResultRow): ComputationStageBlobMetadata {
    val path = row.get<String?>("PathToBlob")
    return computationStageBlobMetadata {
      blobId = row["BlobId"]
      dependencyType = row.getProtoEnum("DependencyType", ComputationBlobDependency::forNumber)
      if (path != null) {
        this.path = path
      }
    }
  }
}
