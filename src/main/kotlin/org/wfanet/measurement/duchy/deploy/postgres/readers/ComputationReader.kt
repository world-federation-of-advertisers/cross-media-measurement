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

package org.wfanet.measurement.duchy.deploy.postgres.readers

import com.google.gson.JsonParser
import java.time.Instant
import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.common.base64MimeDecode
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationStageLongValues
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ExternalRequisitionKey
import org.wfanet.measurement.internal.duchy.RequisitionDetails
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

  /** A wrapper data class for ComputationToken query result */
  data class ReadComputationTokenResult(val computationToken: ComputationToken)

  private fun buildComputationToken(row: ResultRow) =
    ReadComputationTokenResult(
      computationToken {
        val blobs =
          row
            .get<Array<String>?>("Blobs")
            ?.map {
              val jsonObj = JsonParser.parseString(it).asJsonObject
              computationStageBlobMetadata {
                blobId = jsonObj.getAsJsonPrimitive("BlobId").asLong
                val blobPath: String = jsonObj.getAsJsonPrimitive("PathToBlob").asString
                if (blobPath.isNotEmpty()) {
                  path = blobPath
                }
                dependencyType =
                  ComputationBlobDependency.forNumber(
                    jsonObj.getAsJsonPrimitive("DependencyType").asInt
                  )
              }
            }
            ?.sortedBy { it.blobId }
        val requisitions =
          row
            .get<Array<String>?>("Requisitions")
            ?.map {
              val jsonObj = JsonParser.parseString(it).asJsonObject
              requisitionMetadata {
                externalKey = externalRequisitionKey {
                  externalRequisitionId =
                    jsonObj.getAsJsonPrimitive("ExternalRequisitionId").asString
                  requisitionFingerprint =
                    jsonObj.getAsJsonPrimitive("RequisitionFingerprint").base64MimeDecode()
                }
                jsonObj.get("PathToBlob").let { jsonElem ->
                  if (!jsonElem.isJsonNull) path = jsonElem.asString
                }
                details =
                  RequisitionDetails.parseFrom(
                    jsonObj.getAsJsonPrimitive("RequisitionDetails").base64MimeDecode()
                  )
              }
            }
            ?.sortedBy { it.externalKey.externalRequisitionId }

        val computationDetailsProto =
          row.getProtoMessage("ComputationDetails", ComputationDetails.parser())
        val stageDetails = row.getProtoMessage("StageDetails", ComputationStageDetails.parser())
        globalComputationId = row["GlobalComputationId"]
        localComputationId = row["ComputationId"]
        computationStage =
          computationProtocolStagesEnumHelper.longValuesToComputationStageEnum(
            ComputationStageLongValues(row["Protocol"], row["ComputationStage"])
          )
        attempt = row.get<Int>("NextAttempt") - 1
        computationDetails = computationDetailsProto
        version = row.get<Instant>("UpdateTime").toEpochMilli()
        stageSpecificDetails = stageDetails

        if (!row.get<String?>("LockOwner").isNullOrBlank()) {
          lockOwner = row["LockOwner"]
        }
        lockExpirationTime = row.get<Instant>("LockExpirationTime").toProtoTime()

        if (!blobs.isNullOrEmpty()) {
          this.blobs += blobs
        }

        if (!requisitions.isNullOrEmpty()) {
          this.requisitions += requisitions
        }
      }
    )

  /**
   * Gets a [ComputationToken] by globalComputationId.
   *
   * @param readContext The transaction context for reading from the Postgres database.
   * @param globalComputationId A global identifier for a computation.
   * @return [ReadComputationTokenResult] when a Computation with globalComputationId is found.
   * @return null otherwise.
   */
  suspend fun readComputationToken(
    readContext: ReadContext,
    globalComputationId: String
  ): ReadComputationTokenResult? {
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
        cs.Details AS StageDetails,
        (
          SELECT ARRAY_AGG(
            JSON_BUILD_OBJECT(
              'BlobId', b.BlobId,
              'BlobPath', b.PathToBlob,
              'DependencyType', b.DependencyType
            ))
          FROM ComputationBlobReferences AS b
          WHERE
            c.ComputationId = b.ComputationId
            AND
            c.ComputationStage = b.ComputationStage
        ) AS Blobs,
        (
          SELECT ARRAY_AGG(
            JSON_BUILD_OBJECT(
              'ExternalRequisitionId', r2.ExternalRequisitionId,
              'RequisitionFingerprint', encode(r2.RequisitionFingerprint, 'base64'),
              'PathToBlob', r2.PathToBlob,
              'RequisitionDetails', encode(r2.RequisitionDetails, 'base64')
            ))
          FROM Requisitions AS r2
          WHERE c.ComputationId = r2.ComputationId
        ) AS Requisitions
      FROM Computations AS c
      JOIN ComputationStages AS cs
        ON c.ComputationId = cs.ComputationId AND c.ComputationStage = cs.ComputationStage
      WHERE c.GlobalComputationId = $1
      """
      ) {
        bind("$1", globalComputationId)
      }
    return readContext.executeQuery(statement).consume(::buildComputationToken).firstOrNull()
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
    readContext: ReadContext,
    externalRequisitionKey: ExternalRequisitionKey
  ): ReadComputationTokenResult? {
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
        cs.Details AS StageDetails,
        (
          SELECT JSON_AGG(
            JSON_BUILD_OBJECT(
              'BlobId', b.BlobId,
              'BlobPath', b.PathToBlob,
              'DependencyType', b.DependencyType
            ))
          FROM ComputationBlobReferences AS b
          WHERE
            c.ComputationId = b.ComputationId
            AND
            c.ComputationStage = b.ComputationStage
        ) AS Blobs,
        (
          SELECT JSON_AGG(
            JSON_BUILD_OBJECT(
              'ExternalRequisitionId', r2.ExternalRequisitionId,
              'RequisitionFingerprint', r2.RequisitionFingerprint,
              'PathToBlob', r2.PathToBlob,
              'RequisitionDetails', r2.RequisitionDetails
            ))
          FROM Requisitions AS r2
          WHERE c.ComputationId = r2.ComputationId
        ) AS Requisitions
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
    return readContext.executeQuery(statement).consume(::buildComputationToken).firstOrNull()
  }
}
