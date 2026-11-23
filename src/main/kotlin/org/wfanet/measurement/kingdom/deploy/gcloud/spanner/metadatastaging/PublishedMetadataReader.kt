// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.metadatastaging

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct as SpannerStruct
import com.google.cloud.spanner.Value
import com.google.protobuf.Struct
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.kingdom.EventGroup

/** Reads visible metadata and search values through the supplied database snapshot. */
class PublishedMetadataReader(
  private val context: AsyncDatabaseClient.ReadContext,
  private val dataProviderId: Long,
  private val measurementConsumerId: Long,
) {
  suspend fun get(eventGroupId: Long): Struct? {
    return read("AND E.EventGroupId = @eventGroupId") { bind("eventGroupId").to(eventGroupId) }
      .singleOrNull()
      ?.let { metadata(it) }
  }

  suspend fun list(): Map<Long, Struct> =
    read().associate { it.getLong("EventGroupId") to metadata(it) }

  suspend fun search(query: String): Map<Long, Struct> {
    return context
      .executeQuery(
        scopedStatement(
          """
        ${PublishedMetadataSql.visibleVersions}
        SELECT E.EventGroupId, S.Metadata AS EntityMetadata
        FROM EventGroupMetadataVersions@{FORCE_INDEX=EventGroupMetadataVersionsByValues} AS S
        JOIN VisibleVersions AS V USING (OperationId, EventGroupId)
        JOIN EventGroups AS E ON E.EventGroupId = S.EventGroupId
          AND E.DataProviderId = V.DataProviderId AND E.MeasurementConsumerId = V.MeasurementConsumerId
        WHERE E.DataProviderId = @dataProviderId
          AND E.MeasurementConsumerId = @measurementConsumerId
          AND E.State != @deletedState
          AND SEARCH(S.Metadata_Tokens, @query)
        UNION ALL
        SELECT E.EventGroupId, E.EntityMetadata
        FROM EventGroups@{FORCE_INDEX=EventGroupsByEntityMetadata} AS E
        WHERE E.DataProviderId = @dataProviderId
          AND E.MeasurementConsumerId = @measurementConsumerId
          AND E.State != @deletedState
          AND NOT EXISTS (SELECT 1 FROM VisibleVersions AS V WHERE V.EventGroupId = E.EventGroupId AND V.DataProviderId = E.DataProviderId AND V.MeasurementConsumerId = E.MeasurementConsumerId)
          AND SEARCH(E.EntityMetadata_Tokens, @query)
        """
            .trimIndent()
        ) {
          bind("query").to(query)
        }
      )
      .toList()
      .associate { it.getLong("EventGroupId") to metadata(it) }
  }

  internal suspend fun readBatch(
    entityPath: String,
    lastEventGroupId: Long?,
    batchSize: Int,
  ): List<SpannerStruct> {
    return read(
      """
      AND STARTS_WITH(E.EntityPath, @entityPath)
      AND (@lastEventGroupId IS NULL OR E.EventGroupId > @lastEventGroupId)
      ORDER BY E.EventGroupId
      LIMIT @batchSize
      """
        .trimIndent()
    ) {
      bind("entityPath").to(entityPath)
      bind("lastEventGroupId").to(Value.int64(lastEventGroupId))
      bind("batchSize").to(batchSize.toLong())
    }
  }

  private suspend fun read(
    filter: String = "",
    bind: Statement.Builder.() -> Unit = {},
  ): List<SpannerStruct> {
    return context
      .executeQuery(
        scopedStatement(
          """
        ${PublishedMetadataSql.visibleVersions}
        SELECT E.EventGroupId,
          IF(V.OperationId IS NULL, E.EntityMetadata, S.Metadata) AS EntityMetadata
        FROM EventGroups AS E
        LEFT JOIN VisibleVersions AS V USING (DataProviderId, MeasurementConsumerId, EventGroupId)
        LEFT JOIN EventGroupMetadataVersions AS S
          ON S.OperationId = V.OperationId AND S.EventGroupId = V.EventGroupId
        WHERE E.DataProviderId = @dataProviderId
          AND E.MeasurementConsumerId = @measurementConsumerId
          AND E.State != @deletedState
        $filter
        """
            .trimIndent(),
          bind,
        )
      )
      .toList()
  }

  private fun scopedStatement(sql: String, bind: Statement.Builder.() -> Unit): Statement =
    statement(sql) {
      bind("dataProviderId").to(dataProviderId)
      bind("measurementConsumerId").to(measurementConsumerId)
      bind("deletedState").to(EventGroup.State.DELETED.number.toLong())
      bind()
    }

  private fun metadata(row: SpannerStruct): Struct =
    if (row.isNull("EntityMetadata")) Struct.getDefaultInstance()
    else row.getProtoMessage("EntityMetadata", Struct.getDefaultInstance())
}
