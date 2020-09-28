// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.db.duchy.computation.gcp

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import org.wfanet.measurement.db.gcp.getProtoMessage
import org.wfanet.measurement.db.gcp.toMillis
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationToken

/** Query for fields needed to make a [ComputationToken] .*/
class ComputationTokenProtoQuery(
  val parseStageEnum: (Long) -> ComputationStage,
  globalId: String
) :
  SqlBasedQuery<ComputationToken> {
  companion object {
    private val parameterizedQueryString =
      """
      SELECT c.ComputationId,
             c.GlobalComputationId,
             c.LockOwner,
             c.ComputationStage,
             c.ComputationDetails,
             c.UpdateTime,
             cs.NextAttempt,
             cs.Details AS StageDetails,
             ARRAY_AGG(b.BlobId) AS BlobIds,
             ARRAY_AGG(b.PathToBlob) AS BlobPaths,
             ARRAY_AGG(b.DependencyType) AS DependencyTypes
      FROM Computations AS c
      JOIN ComputationStages AS cs USING (ComputationId, ComputationStage)
      JOIN ComputationBlobReferences AS b USING (ComputationId, ComputationStage)
      WHERE c.GlobalComputationId = @global_id
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
      """.trimIndent()
  }

  override val sql: Statement =
    Statement.newBuilder(parameterizedQueryString).bind("global_id").to(globalId).build()

  override fun asResult(struct: Struct): ComputationToken {
    val blobIds: List<Long> = struct.getLongList("BlobIds")
    val blobPaths: List<String?> = struct.getStringList("BlobPaths")
    val dependencyTypes: List<Long> = struct.getLongList("DependencyTypes")
    require(blobIds.size == blobPaths.size)
    require(blobIds.size == dependencyTypes.size)

    val blobs =
      zip(blobIds, blobPaths, dependencyTypes)
        .map { (blobId, blobPath, dependencyType) ->
          ComputationStageBlobMetadata.newBuilder().apply {
            this.blobId = blobId
            this.dependencyType = ComputationBlobDependency.forNumber(dependencyType.toInt())
            blobPath?.let { path = blobPath }
          }.build()
        }
        .sortedBy { it.blobId }

    val computationDetails =
      struct.getProtoMessage("ComputationDetails", ComputationDetails.parser())
    val stageDetails = struct.getProtoMessage("StageDetails", ComputationStageDetails.parser())
    return ComputationToken.newBuilder().apply {
      globalComputationId = struct.getString("GlobalComputationId")
      localComputationId = struct.getLong("ComputationId")
      computationStage = parseStageEnum(struct.getLong("ComputationStage"))
      attempt = struct.getLong("NextAttempt").toInt() - 1
      nextDuchy = computationDetails.outgoingNodeId
      primaryDuchy = computationDetails.primaryNodeId
      version = struct.getTimestamp("UpdateTime").toMillis()
      role = computationDetails.role
      stageSpecificDetails = stageDetails
      addAllBlobs(blobs.asIterable())
    }.build()
  }
}

private fun <T1, T2, T3> zip(
  i1: Iterable<T1>,
  i2: Iterable<T2>,
  i3: Iterable<T3>
): Sequence<Triple<T1, T2, T3>> {
  return i1
    .asSequence()
    .zip(i2.asSequence())
    .zip(i3.asSequence())
    .map { Triple(it.first.first, it.first.second, it.second) }
}
