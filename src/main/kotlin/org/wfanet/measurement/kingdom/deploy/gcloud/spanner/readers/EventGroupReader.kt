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
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.eventGroup

class EventGroupReader : BaseSpannerReader<EventGroupReader.Result>() {
  data class Result(
    val eventGroup: EventGroup,
    val internalEventGroupId: InternalId,
    val internalDataProviderId: InternalId
  )

  override val builder: Statement.Builder = Statement.newBuilder(BASE_SQL)

  /** Fills [builder], returning this [RequisitionReader] for chaining. */
  fun fillStatementBuilder(fill: Statement.Builder.() -> Unit): EventGroupReader {
    builder.fill()
    return this
  }

  suspend fun readByCreateRequestId(
    readContext: AsyncDatabaseClient.ReadContext,
    dataProviderId: InternalId,
    createRequestId: String
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE
            DataProviderId = @${Params.DATA_PROVIDER_ID}
            AND CreateRequestId = @${Params.CREATE_REQUEST_ID}
          """
            .trimIndent()
        )
        bind(Params.DATA_PROVIDER_ID to dataProviderId)
        bind(Params.CREATE_REQUEST_ID to createRequestId)
      }
      .execute(readContext)
      .singleOrNull()
  }

  suspend fun readByExternalIds(
    readContext: AsyncDatabaseClient.ReadContext,
    externalDataProviderId: Long,
    externalEventGroupId: Long,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE
            ExternalEventGroupId = @${Params.EXTERNAL_EVENT_GROUP_ID}
            AND ExternalDataProviderId = @${Params.EXTERNAL_DATA_PROVIDER_ID}
          """
            .trimIndent()
        )
        bind(Params.EXTERNAL_EVENT_GROUP_ID to externalEventGroupId)
        bind(Params.EXTERNAL_DATA_PROVIDER_ID to externalDataProviderId)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }

  override suspend fun translate(struct: Struct): Result =
    Result(
      buildEventGroup(struct),
      InternalId(struct.getLong("EventGroupId")),
      InternalId(struct.getLong("DataProviderId"))
    )

  private fun buildEventGroup(struct: Struct): EventGroup = eventGroup {
    externalEventGroupId = struct.getLong("ExternalEventGroupId")
    externalDataProviderId = struct.getLong("ExternalDataProviderId")
    externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
    if (!struct.isNull("ExternalMeasurementConsumerCertificateId")) {
      externalMeasurementConsumerCertificateId =
        struct.getLong("ExternalMeasurementConsumerCertificateId")
    }
    if (!struct.isNull("ProvidedEventGroupId")) {
      providedEventGroupId = struct.getString("ProvidedEventGroupId")
    }
    createTime = struct.getTimestamp("CreateTime").toProto()
    updateTime = struct.getTimestamp("UpdateTime").toProto()
    if (!struct.isNull("EventGroupDetails")) {
      details = struct.getProtoMessage("EventGroupDetails", EventGroup.Details.parser())
    }
    state = struct.getProtoEnum("State", EventGroup.State::forNumber)
  }

  companion object {
    private val BASE_SQL =
      """
      SELECT
        EventGroups.EventGroupId,
        EventGroups.ExternalEventGroupId,
        EventGroups.MeasurementConsumerId,
        EventGroups.DataProviderId,
        EventGroups.ProvidedEventGroupId,
        EventGroups.CreateTime,
        EventGroups.UpdateTime,
        EventGroups.EventGroupDetails,
        MeasurementConsumers.ExternalMeasurementConsumerId,
        DataProviders.ExternalDataProviderId,
        MeasurementConsumerCertificates.ExternalMeasurementConsumerCertificateId,
        EventGroups.State
      FROM EventGroups
      JOIN MeasurementConsumers USING (MeasurementConsumerId)
      JOIN DataProviders USING (DataProviderId)
      LEFT JOIN MeasurementConsumerCertificates ON MeasurementConsumerCertificateId = CertificateId
        AND EventGroups.MeasurementConsumerId = MeasurementConsumerCertificates.MeasurementConsumerId
      """
        .trimIndent()

    private object Params {
      const val EXTERNAL_DATA_PROVIDER_ID = "externalDataProviderId"
      const val EXTERNAL_EVENT_GROUP_ID = "externalEventGroupId"
      const val DATA_PROVIDER_ID = "dataProviderId"
      const val CREATE_REQUEST_ID = "createRequestId"
    }
  }
}
