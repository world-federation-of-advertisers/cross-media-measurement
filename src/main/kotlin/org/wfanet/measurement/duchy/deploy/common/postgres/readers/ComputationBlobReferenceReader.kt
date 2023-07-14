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
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency

class ComputationBlobReferenceReader {

  /**
   * Gets the [ComputationBlobDependency] of a computation.
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
   * Gets a map of blobId to pathToBlob of a computation based on localComputationId.
   *
   * @param localId local identifier of the computation
   * @param stage stage enum of the computation
   * @param dependencyType enum value of the dependency type
   * @return [Map<[Long], [String]?>] for all blobIds of a computation
   */
  suspend fun blobIdToPathMapByDepType(
    readContext: ReadContext,
    localId: Long,
    stage: Long,
    dependencyType: Long
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
      .consume { row -> row }
      .toList()
      .associate { it.get<Long>("BlobId") to it.get<String?>("PathToBlob") }
  }
}
