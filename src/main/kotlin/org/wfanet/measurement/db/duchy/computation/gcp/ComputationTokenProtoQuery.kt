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
import org.wfanet.measurement.db.gcp.getNullableString
import org.wfanet.measurement.db.gcp.getNullableStructList
import org.wfanet.measurement.db.gcp.getProtoEnum
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
    private const val parameterizedQueryString =
      """
      WITH current_stage_data AS (
        SELECT c.ComputationId, c.GlobalComputationId, c.LockOwner, c.ComputationStage,
               c.ComputationDetails, c.UpdateTime, cs.NextAttempt, cs.Details AS StageDetails
        FROM Computations AS c
        JOIN ComputationStages AS cs USING (ComputationId, ComputationStage)
        WHERE c.GlobalComputationId = @global_id
      ),
      blobs_for_stage as (
        SELECT ARRAY_AGG(STRUCT(b.BlobId, b.PathToBlob, b.DependencyType)) as blobs
        FROM ComputationBlobReferences AS b
        JOIN current_stage_data USING(ComputationId, ComputationStage)
      )
      SELECT current_stage_data.*, blobs_for_stage.*
      FROM current_stage_data, blobs_for_stage
      """
  }

  override val sql: Statement =
    Statement.newBuilder(parameterizedQueryString).bind("global_id").to(globalId).build()

  override fun asResult(struct: Struct): ComputationToken {
    val blobs = struct.getNullableStructList("blobs")?.map {
      ComputationStageBlobMetadata.newBuilder().apply {
        blobId = it.getLong("BlobId")
        dependencyType = it.getProtoEnum("DependencyType", ComputationBlobDependency::forNumber)
        it.getNullableString("PathToBlob")?.let { setPath(it) }
      }.build()
    }?.sortedBy { it.blobId }
      // Empty list if the column was null.
      ?: listOf()
    val computationDetails = struct.getProtoMessage(
      "ComputationDetails", ComputationDetails.parser()
    )
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
      addAllBlobs(blobs)
    }.build()
  }
}
