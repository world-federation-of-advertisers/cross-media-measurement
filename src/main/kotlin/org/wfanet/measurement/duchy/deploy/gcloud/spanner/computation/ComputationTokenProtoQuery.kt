// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import org.wfanet.measurement.duchy.db.computation.ComputationStageLongValues
import org.wfanet.measurement.duchy.db.computation.ExternalRequisitionKey
import org.wfanet.measurement.gcloud.common.toEpochMilli
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.RequisitionDetails
import org.wfanet.measurement.internal.duchy.RequisitionMetadata

/** Query for fields needed to make a [ComputationToken] using the global computation id. */
class ComputationTokenProtoQuery(
  val parseStageEnum: (ComputationStageLongValues) -> ComputationStage,
  globalId: String? = null,
  externalRequisitionKey: ExternalRequisitionKey? = null
) : SqlBasedQuery<ComputationToken> {
  companion object {
    private val parameterizedQueryUsingGlobalComputationIdString =
      """
      SELECT c.ComputationId,
             c.GlobalComputationId,
             c.LockOwner,
             c.ComputationStage,
             c.ComputationDetails,
             c.Protocol,
             c.UpdateTime,
             cs.NextAttempt,
             cs.Details AS StageDetails,
             ARRAY(
               SELECT AS STRUCT b.BlobId, IFNULL(b.PathToBlob, "") AS PathToBlob, b.DependencyType
               FROM ComputationBlobReferences AS b
               WHERE c.ComputationId = b.ComputationId
                 AND c.ComputationStage = b.ComputationStage
             ) AS Blobs,
             ARRAY(
               SELECT AS STRUCT
                 r2.ExternalDataProviderId,
                 r2.ExternalRequisitionId,
                 IFNULL(r2.PathToBlob, "") AS PathToBlob,
                 r2.RequisitionDetails
               FROM Requisitions AS r2
               WHERE c.ComputationId = r2.ComputationId
             ) AS Requisitions
      FROM Computations AS c
      JOIN ComputationStages AS cs USING (ComputationId, ComputationStage)
      WHERE c.GlobalComputationId = @global_id
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
      """.trimIndent()

    private val parameterizedQueryUsingExternalRequisitionKeyString =
      """
      SELECT c.ComputationId,
             c.GlobalComputationId,
             c.LockOwner,
             c.ComputationStage,
             c.ComputationDetails,
             c.Protocol,
             c.UpdateTime,
             cs.NextAttempt,
             cs.Details AS StageDetails,
             ARRAY(
               SELECT AS STRUCT b.BlobId, IFNULL(b.PathToBlob, "") AS PathToBlob, b.DependencyType
               FROM ComputationBlobReferences AS b
               WHERE c.ComputationId = b.ComputationId
                 AND c.ComputationStage = b.ComputationStage
             ) AS Blobs,
             ARRAY(
               SELECT AS STRUCT
                 r2.ExternalDataProviderId,
                 r2.ExternalRequisitionId,
                 IFNULL(r2.PathToBlob, "") AS PathToBlob,
                 r2.RequisitionDetails
               FROM Requisitions AS r2
               WHERE c.ComputationId = r2.ComputationId
             ) AS Requisitions
      FROM Computations AS c
      JOIN ComputationStages AS cs USING (ComputationId, ComputationStage)
      JOIN Requisitions AS r USING (ComputationId)
      WHERE r.ExternalDataProviderId = @external_data_provider_id
        AND r.ExternalRequisitionId = @external_requisition_id
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
      """.trimIndent()
  }

  override val sql: Statement =
    if (globalId != null) {
      Statement.newBuilder(parameterizedQueryUsingGlobalComputationIdString)
        .bind("global_id")
        .to(globalId)
        .build()
    } else {
      require(externalRequisitionKey != null) {
        "global computation id or external requisition key is required."
      }
      Statement.newBuilder(parameterizedQueryUsingExternalRequisitionKeyString)
        .bind("external_data_provider_id")
        .to(externalRequisitionKey.externalDataProviderId)
        .bind("external_requisition_id")
        .to(externalRequisitionKey.externalRequisitionId)
        .build()
    }

  override fun asResult(struct: Struct): ComputationToken {
    val blobs =
      struct
        .getStructList("Blobs")
        .map {
          ComputationStageBlobMetadata.newBuilder()
            .apply {
              blobId = it.getLong("BlobId")
              val blobPath = it.getString("PathToBlob")
              if (!blobPath.isNullOrBlank()) {
                path = blobPath
              }
              dependencyType =
                ComputationBlobDependency.forNumber(it.getLong("DependencyType").toInt())
            }
            .build()
        }
        .sortedBy { it.blobId }
        .toList()
    val requisitions =
      struct
        .getStructList("Requisitions")
        .map {
          RequisitionMetadata.newBuilder()
            .apply {
              externalDataProviderId = it.getString("ExternalDataProviderId")
              externalRequisitionId = it.getString("ExternalRequisitionId")
              path = it.getString("PathToBlob")
              details = it.getProtoMessage("RequisitionDetails", RequisitionDetails.parser())
            }
            .build()
        }
        .sortedBy { it.externalRequisitionId }
        .toList()

    val computationDetailsProto =
      struct.getProtoMessage("ComputationDetails", ComputationDetails.parser())
    val stageDetails = struct.getProtoMessage("StageDetails", ComputationStageDetails.parser())
    return ComputationToken.newBuilder()
      .apply {
        globalComputationId = struct.getString("GlobalComputationId")
        localComputationId = struct.getLong("ComputationId")
        computationStage =
          parseStageEnum(
            ComputationStageLongValues(
              struct.getLong("Protocol"),
              struct.getLong("ComputationStage")
            )
          )
        attempt = struct.getLong("NextAttempt").toInt() - 1
        computationDetails = computationDetailsProto
        version = struct.getTimestamp("UpdateTime").toEpochMilli()
        stageSpecificDetails = stageDetails

        if (blobs.isNotEmpty()) {
          addAllBlobs(blobs)
        }

        if (requisitions.isNotEmpty()) {
          addAllRequisitions(requisitions)
        }
      }
      .build()
  }
}
