// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptor
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorDetails
import org.wfanet.measurement.internal.kingdom.eventGroupMetadataDescriptor

class EventGroupMetadataDescriptorReader :
  BaseSpannerReader<EventGroupMetadataDescriptorReader.Result>() {
  data class Result(
    val eventGroupMetadataDescriptor: EventGroupMetadataDescriptor,
    val internalDescriptorId: InternalId,
    val internalDataProviderId: InternalId,
  )

  override val builder: Statement.Builder = Statement.newBuilder(BASE_SQL)

  /** Fills [builder], returning this [EventGroupMetadataDescriptorReader] for chaining. */
  fun fillStatementBuilder(fill: Statement.Builder.() -> Unit): EventGroupMetadataDescriptorReader {
    builder.fill()
    return this
  }

  fun bindWhereClause(
    dataProviderId: Long,
    externalDescriptorId: Long,
  ): EventGroupMetadataDescriptorReader {
    return fillStatementBuilder {
      appendClause(
        """
        WHERE EventGroupMetadataDescriptors.DataProviderId = @data_provider_id
        AND EventGroupMetadataDescriptors.ExternalEventGroupMetadataDescriptorId = @external_descriptor_id
        """
          .trimIndent()
      )
      bind("data_provider_id" to dataProviderId)
      bind("external_descriptor_id" to externalDescriptorId)
    }
  }

  suspend fun readByExternalIds(
    readContext: AsyncDatabaseClient.ReadContext,
    externalDataProviderId: Long,
    externalDescriptorId: Long,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE
            ExternalDataProviderId = @$EXTERNAL_DATA_PROVIDER_ID_PARAM
            AND ExternalEventGroupMetadataDescriptorId = @$EXTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR_ID_PARAM
          """
            .trimIndent()
        )
        bind(EXTERNAL_DATA_PROVIDER_ID_PARAM to externalDataProviderId)
        bind(EXTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR_ID_PARAM to externalDescriptorId)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }

  suspend fun readByIdempotencyKey(
    readContext: AsyncDatabaseClient.ReadContext,
    dataProviderId: InternalId,
    idempotencyKey: String,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE
            DataProviderId = @$DATA_PROVIDER_ID_PARAM
            AND IdempotencyKey = @$IDEMPOTENCY_KEY_PARAM
          """
            .trimIndent()
        )
        bind(DATA_PROVIDER_ID_PARAM to dataProviderId)
        bind(IDEMPOTENCY_KEY_PARAM to idempotencyKey)

        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }

  override suspend fun translate(struct: Struct): Result =
    Result(
      buildEventGroupMetadataDescriptor(struct),
      InternalId(struct.getLong("EventGroupMetadataDescriptorId")),
      InternalId(struct.getLong("DataProviderId")),
    )

  private fun buildEventGroupMetadataDescriptor(struct: Struct): EventGroupMetadataDescriptor {
    return eventGroupMetadataDescriptor {
      externalDataProviderId = struct.getLong("ExternalDataProviderId")
      externalEventGroupMetadataDescriptorId =
        struct.getLong("ExternalEventGroupMetadataDescriptorId")
      if (!struct.isNull("IdempotencyKey")) {
        idempotencyKey = struct.getString("IdempotencyKey")
      }
      if (!struct.isNull("DescriptorDetails")) {
        details =
          struct.getProtoMessage(
            "DescriptorDetails",
            EventGroupMetadataDescriptorDetails.getDefaultInstance(),
          )
      }
    }
  }

  companion object {
    private const val IDEMPOTENCY_KEY_PARAM = "idempotencyKey"
    private const val DATA_PROVIDER_ID_PARAM = "dataProviderId"
    private const val EXTERNAL_DATA_PROVIDER_ID_PARAM = "externalDataProviderId"
    private const val EXTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR_ID_PARAM =
      "externalEventGroupMetadataDescriptorId"

    private val BASE_SQL =
      """
      SELECT
        EventGroupMetadataDescriptors.DataProviderId,
        EventGroupMetadataDescriptors.EventGroupMetadataDescriptorId,
        EventGroupMetadataDescriptors.ExternalEventGroupMetadataDescriptorId,
        EventGroupMetadataDescriptors.IdempotencyKey,
        EventGroupMetadataDescriptors.DescriptorDetails,
        DataProviders.ExternalDataProviderId
      FROM EventGroupMetadataDescriptors
      JOIN DataProviders USING (DataProviderId)
      """
        .trimIndent()
  }
}
