/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Type
import com.google.type.Interval
import com.google.type.interval
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.getInternalId
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.struct
import org.wfanet.measurement.gcloud.spanner.to
import org.wfanet.measurement.gcloud.spanner.toInt64Array
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.ModelLineKey
import org.wfanet.measurement.internal.kingdom.modelLine
import org.wfanet.measurement.internal.kingdom.modelLineKey

class ModelLineReader : SpannerReader<ModelLineReader.Result>() {

  data class Result(
    val modelLine: ModelLine,
    val modelLineId: InternalId,
    val modelSuiteId: InternalId,
    val modelProviderId: InternalId,
  )

  data class ActiveIntervalResult(
    val key: ModelLineInternalKey,
    val externalKey: ModelLineKey,
    val activeInterval: Interval,
  )

  override val baseSql: String =
    """
    SELECT
      ModelLines.ModelProviderId,
      ModelLines.ModelSuiteId,
      ModelLines.ModelLineId,
      ModelLines.ExternalModelLineId,
      ModelLines.DisplayName,
      ModelLines.Description,
      ModelLines.ActiveStartTime,
      ModelLines.ActiveEndTime,
      ModelLines.Type,
      ModelLines.CreateTime,
      ModelLines.UpdateTime,
      ModelSuites.ExternalModelSuiteId,
      ModelProviders.ExternalModelProviderId,
      HoldbackModelLine.ExternalModelLineId as ExternalHoldbackModelLineId,
    FROM
      ModelProviders
      JOIN ModelSuites USING (ModelProviderId)
      JOIN ModelLines USING (ModelProviderId, ModelSuiteId)
      LEFT JOIN ModelLines AS HoldbackModelLine ON (
        ModelLines.ModelProviderId = HoldbackModelLine.ModelProviderId
        AND ModelLines.ModelSuiteId = HoldbackModelLine.ModelSuiteId
        AND ModelLines.HoldbackModelLineId = HoldbackModelLine.ModelLineId
      )
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result = buildResult(struct)

  suspend fun readByExternalModelLineId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalModelProviderId: ExternalId,
    externalModelSuiteId: ExternalId,
    externalModelLineId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
            WHERE ExternalModelSuiteId = @externalModelSuiteId
            AND ExternalModelProviderId = @externalModelProviderId
            AND ModelLines.ExternalModelLineId = @externalModelLineId
          """
            .trimIndent()
        )
        bind("externalModelSuiteId").to(externalModelSuiteId.value)
        bind("externalModelProviderId").to(externalModelProviderId.value)
        bind("externalModelLineId").to(externalModelLineId.value)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }

  companion object {
    private val VALID_MODEL_LINES_BASE_SQL =
      """
      WITH InRangeModelLineKeys AS (
        SELECT
          ModelProviderId,
          ModelSuiteId,
          ModelLineId,
        FROM
          DataProviders
          JOIN DataProviderAvailabilityIntervals USING (DataProviderId)
        WHERE
          ExternalDataProviderId IN UNNEST(@externalDataProviderIds)
        GROUP BY 1, 2, 3
        HAVING
          COUNT(DISTINCT DataProviderId) = ARRAY_LENGTH(@externalDataProviderIds)
          AND @intervalStartTime >= MAX(StartTime)
          AND @intervalEndTime <= MIN(EndTime)
      )
      SELECT
        ModelLines.ModelProviderId,
        ModelLines.ModelSuiteId,
        ModelLines.ModelLineId,
        ModelLines.ExternalModelLineId,
        ModelLines.DisplayName,
        ModelLines.Description,
        ModelLines.ActiveStartTime,
        ModelLines.ActiveEndTime,
        ModelLines.Type,
        ModelLines.CreateTime,
        ModelLines.UpdateTime,
        HoldbackModelLine.ExternalModelLineId AS ExternalHoldbackModelLineId,
        ModelSuites.ExternalModelSuiteId,
        ModelProviders.ExternalModelProviderId,
      FROM
        InRangeModelLineKeys
        JOIN ModelLines USING (ModelProviderId, ModelSuiteId, ModelLineId)
        JOIN ModelSuites USING (ModelProviderId, ModelSuiteId)
        JOIN ModelProviders USING (ModelProviderId)
        LEFT JOIN ModelLines AS HoldbackModelLine ON (
          ModelLines.ModelProviderId = HoldbackModelLine.ModelProviderId
          AND ModelLines.ModelSuiteId = HoldbackModelLine.ModelSuiteId
          AND ModelLines.HoldbackModelLineId = HoldbackModelLine.ModelLineId
        )
      WHERE
        ModelLines.Type IN UNNEST(@types)
      """
        .trimIndent()

    private val MODEL_LINE_KEY_STRUCT =
      Type.struct(
        Type.StructField.of("ExternalModelProviderId", Type.int64()),
        Type.StructField.of("ExternalModelSuiteId", Type.int64()),
        Type.StructField.of("ExternalModelLineId", Type.int64()),
      )

    private fun buildResult(struct: Struct) =
      Result(
        buildModelLine(struct),
        InternalId(struct.getLong("ModelLineId")),
        InternalId(struct.getLong("ModelSuiteId")),
        InternalId(struct.getLong("ModelProviderId")),
      )

    private fun buildModelLine(struct: Struct): ModelLine = modelLine {
      externalModelProviderId = struct.getLong("ExternalModelProviderId")
      externalModelSuiteId = struct.getLong("ExternalModelSuiteId")
      externalModelLineId = struct.getLong("ExternalModelLineId")
      if (!struct.isNull("DisplayName")) {
        displayName = struct.getString("DisplayName")
      }
      if (!struct.isNull("Description")) {
        description = struct.getString("Description")
      }
      activeStartTime = struct.getTimestamp("ActiveStartTime").toProto()
      if (!struct.isNull("ActiveEndTime")) {
        activeEndTime = struct.getTimestamp("ActiveEndTime").toProto()
      }
      type = struct.getProtoEnum("Type", ModelLine.Type::forNumber)
      if (!struct.isNull("ExternalHoldbackModelLineId")) {
        externalHoldbackModelLineId = struct.getLong("ExternalHoldbackModelLineId")
      }
      createTime = struct.getTimestamp("CreateTime").toProto()
      if (!struct.isNull("UpdateTime")) {
        updateTime = struct.getTimestamp("UpdateTime").toProto()
      }
    }

    suspend fun readActiveIntervals(
      readContext: AsyncDatabaseClient.ReadContext,
      keys: Iterable<ModelLineKey>,
    ): Map<ModelLineKey, ActiveIntervalResult> {
      val sql =
        """
        SELECT
          ModelProviderId,
          ModelSuiteId,
          ModelLineId,
          ExternalModelProviderId,
          ExternalModelSuiteId,
          ExternalModelLineId,
          ActiveStartTime,
          ActiveEndTime,
        FROM
          ModelProviders
          JOIN ModelSuites USING (ModelProviderId)
          JOIN ModelLines USING (ModelProviderId, ModelSuiteId)
        WHERE
          STRUCT(ExternalModelProviderId, ExternalModelSuiteId, ExternalModelLineId) IN UNNEST(@modelLineKeys)
        """
          .trimIndent()
      val query =
        statement(sql) {
          bind("modelLineKeys")
            .toStructArray(
              MODEL_LINE_KEY_STRUCT,
              keys.map {
                struct {
                  set("ExternalModelProviderId").to(it.externalModelProviderId)
                  set("ExternalModelSuiteId").to(it.externalModelSuiteId)
                  set("ExternalModelLineId").to(it.externalModelLineId)
                }
              },
            )
        }

      val results: Flow<Struct> =
        readContext.executeQuery(
          query,
          Options.tag("reader=ModelLineReader,action=readActiveIntervals"),
        )
      return buildMap {
        results.collect { row ->
          val externalKey = modelLineKey {
            externalModelProviderId = row.getLong("ExternalModelProviderId")
            externalModelSuiteId = row.getLong("ExternalModelSuiteId")
            externalModelLineId = row.getLong("ExternalModelLineId")
          }
          val key =
            ModelLineInternalKey(
              row.getInternalId("ModelProviderId"),
              row.getInternalId("ModelSuiteId"),
              row.getInternalId("ModelLineId"),
            )
          val activeInterval = interval {
            startTime = row.getTimestamp("ActiveStartTime").toProto()
            if (!row.isNull("ActiveEndTime")) {
              endTime = row.getTimestamp("ActiveEndTime").toProto()
            }
          }
          put(externalKey, ActiveIntervalResult(key, externalKey, activeInterval))
        }
      }
    }

    suspend fun readInternalKey(
      readContext: AsyncDatabaseClient.ReadContext,
      externalKey: ModelLineKey,
    ): ModelLineInternalKey? {
      val sql =
        """
        SELECT
          ModelProviderId,
          ModelSuiteId,
          ModelLineId,
        FROM
          ModelProviders
          JOIN ModelSuites USING (ModelProviderId)
          JOIN ModelLines USING (ModelProviderId, ModelSuiteId)
        WHERE
          ExternalModelProviderId = @externalModelProviderId
          AND ExternalModelSuiteId = @externalModelSuiteId
          AND ExternalModelLineId = @externalModelLineId
        """
          .trimIndent()
      val query =
        statement(sql) {
          bind("externalModelProviderId").to(externalKey.externalModelProviderId)
          bind("externalModelSuiteId").to(externalKey.externalModelSuiteId)
          bind("externalModelLineId").to(externalKey.externalModelLineId)
        }
      val row: Struct? =
        readContext
          .executeQuery(query, Options.tag("reader=ModelLineReader,action=readInternalKey"))
          .singleOrNull()

      return if (row == null) {
        null
      } else {
        ModelLineInternalKey(
          row.getInternalId("ModelProviderId"),
          row.getInternalId("ModelSuiteId"),
          row.getInternalId("ModelLineId"),
        )
      }
    }

    fun readValidModelLines(
      readContext: AsyncDatabaseClient.ReadContext,
      externalModelProviderId: ExternalId?,
      externalModelSuiteId: ExternalId?,
      timeInterval: Interval,
      types: List<ModelLine.Type>,
      externalDataProviderIds: List<ExternalId>,
    ): Flow<Result> {
      require(types.isNotEmpty()) { "types is required" }
      require(externalDataProviderIds.isNotEmpty()) { "externalDataProviderIds is required" }

      val statement =
        statement(VALID_MODEL_LINES_BASE_SQL) {
          bind("externalDataProviderIds").toInt64Array(externalDataProviderIds.map { it.value })
          bind("intervalStartTime").to(timeInterval.startTime.toGcloudTimestamp())
          bind("intervalEndTime").to(timeInterval.endTime.toGcloudTimestamp())
          bind("types").toInt64Array(types)

          if (externalModelProviderId != null) {
            appendClause("AND ExternalModelProviderId = @externalModelProviderId")
            bind("externalModelProviderId").to(externalModelProviderId)
          }
          if (externalModelSuiteId != null) {
            appendClause("AND ExternalModelSuiteId = @externalModelSuiteId")
            bind("externalModelSuiteId").to(externalModelSuiteId)
          }
        }

      return readContext
        .executeQuery(statement, Options.tag("reader=ModelLineReader,action=readValidModelLines"))
        .map(::buildResult)
    }
  }
}

data class ModelLineInternalKey(
  val modelProviderId: InternalId,
  val modelSuiteId: InternalId,
  val modelLineId: InternalId,
)
