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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Statement
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.gcloud.spanner.toInt64Array
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupKey
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader

class StreamEventGroups(
  requestFilter: StreamEventGroupsRequest.Filter,
  private val orderBy: StreamEventGroupsRequest.OrderBy,
  limit: Int = 0,
) : SimpleSpannerQuery<EventGroupReader.Result>() {
  override val reader =
    EventGroupReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      val sortOrder = if (orderBy.descending) "DESC" else "ASC"
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf accessors cannot return null.
      when (orderBy.field) {
        StreamEventGroupsRequest.OrderBy.Field.FIELD_NOT_SPECIFIED ->
          appendClause("ORDER BY ExternalDataProviderId ASC, ExternalEventGroupId ASC")
        StreamEventGroupsRequest.OrderBy.Field.DATA_AVAILABILITY_START_TIME -> {
          appendClause(
            "ORDER BY DataAvailabilityStartTime $sortOrder, ExternalDataProviderId ASC, ExternalEventGroupId ASC"
          )
        }
        StreamEventGroupsRequest.OrderBy.Field.UNRECOGNIZED -> error("Unrecognized field")
      }
      if (limit > 0) {
        appendClause("LIMIT @$LIMIT")
        bind(LIMIT to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamEventGroupsRequest.Filter) {
    bind(TIMESTAMP_MAX).to(Timestamp.MAX_VALUE)
    val conjuncts = buildList {
      if (filter.externalDataProviderId != 0L) {
        add("ExternalDataProviderId = @$EXTERNAL_DATA_PROVIDER_ID")
        bind(EXTERNAL_DATA_PROVIDER_ID).to(filter.externalDataProviderId)
      }
      if (filter.externalMeasurementConsumerId != 0L) {
        add("ExternalMeasurementConsumerId = @$EXTERNAL_MEASUREMENT_CONSUMER_ID")
        bind(EXTERNAL_MEASUREMENT_CONSUMER_ID).to(filter.externalMeasurementConsumerId)
      }
      if (filter.externalMeasurementConsumerIdInList.isNotEmpty()) {
        add("ExternalMeasurementConsumerId IN UNNEST(@$EXTERNAL_MEASUREMENT_CONSUMER_IDS)")
        bind(EXTERNAL_MEASUREMENT_CONSUMER_IDS)
          .toInt64Array(filter.externalMeasurementConsumerIdInList)
      }
      if (filter.externalDataProviderIdInList.isNotEmpty()) {
        add("ExternalDataProviderId IN UNNEST(@$EXTERNAL_DATA_PROVIDER_IDS)")
        bind(EXTERNAL_DATA_PROVIDER_IDS).toInt64Array(filter.externalDataProviderIdInList)
      }
      if (!filter.showDeleted) {
        add("State != @$DELETED_STATE")
        bind(DELETED_STATE).toInt64(EventGroup.State.DELETED)
      }
      if (filter.mediaTypesIntersectList.isNotEmpty()) {
        add(
          """
          EXISTS(
            SELECT
              MediaType
            FROM
              EventGroupMediaTypes
            WHERE
              EventGroupMediaTypes.DataProviderId = EventGroups.DataProviderId
              AND EventGroupMediaTypes.EventGroupId = EventGroups.EventGroupId
              AND MediaType IN UNNEST(@$MEDIA_TYPES)
          )
          """
            .trimIndent()
        )
        bind(MEDIA_TYPES).toInt64Array(filter.mediaTypesIntersectList)
      }
      if (filter.hasDataAvailabilityStartTimeOnOrAfter()) {
        add("DataAvailabilityStartTime >= @$DATA_AVAILABILITY_START_TIME_GTE")
        bind(DATA_AVAILABILITY_START_TIME_GTE)
          .to(filter.dataAvailabilityStartTimeOnOrAfter.toGcloudTimestamp())
      }
      if (filter.hasDataAvailabilityEndTimeOnOrBefore()) {
        add("IFNULL(DataAvailabilityEndTime, @$TIMESTAMP_MAX) <= @$DATA_AVAILABILITY_END_TIME_LTE")
        bind(DATA_AVAILABILITY_END_TIME_LTE)
          .to(filter.dataAvailabilityEndTimeOnOrBefore.toGcloudTimestamp())
      }
      if (filter.hasDataAvailabilityStartTimeOnOrBefore()) {
        add("DataAvailabilityStartTime <= @$DATA_AVAILABILITY_START_TIME_LTE")
        bind(DATA_AVAILABILITY_START_TIME_LTE)
          .to(filter.dataAvailabilityStartTimeOnOrBefore.toGcloudTimestamp())
      }
      if (filter.hasDataAvailabilityEndTimeOnOrAfter()) {
        add("IFNULL(DataAvailabilityEndTime, @$TIMESTAMP_MAX) >= @$DATA_AVAILABILITY_END_TIME_GTE")
        bind(DATA_AVAILABILITY_END_TIME_GTE)
          .to(filter.dataAvailabilityEndTimeOnOrAfter.toGcloudTimestamp())
      }
      if (filter.metadataSearchQuery.isNotEmpty()) {
        add("SEARCH(Metadata_Tokens, @$METADATA_SEARCH_QUERY)")
        bind(METADATA_SEARCH_QUERY).to(filter.metadataSearchQuery)
      }

      // TODO(@SanjayVas): Stop reading the deprecated field once the replacement has been available
      // for at least one release.
      if (filter.hasAfter() || filter.hasEventGroupKeyAfter()) {
        val afterEventGroupKey: EventGroupKey =
          if (filter.hasAfter()) filter.after.eventGroupKey else filter.eventGroupKeyAfter
        val tieBreaker =
          """
          (
            ExternalDataProviderId > @${After.EXTERNAL_DATA_PROVIDER_ID}
            OR (
              ExternalDataProviderId = @${After.EXTERNAL_DATA_PROVIDER_ID}
              AND ExternalEventGroupId > @${After.EXTERNAL_EVENT_GROUP_ID}
            )
          )
          """
            .trimIndent()

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf accessors cannot return null.
        when (orderBy.field) {
          StreamEventGroupsRequest.OrderBy.Field.FIELD_NOT_SPECIFIED -> {
            add(tieBreaker)
          }
          StreamEventGroupsRequest.OrderBy.Field.DATA_AVAILABILITY_START_TIME -> {
            val operator = if (orderBy.descending) "<" else ">"
            add(
              """
              (
                DataAvailabilityStartTime $operator @${After.DATA_AVAILABILITY_START_TIME}
                OR (
                  DataAvailabilityStartTime = @${After.DATA_AVAILABILITY_START_TIME}
                  AND $tieBreaker
                )
              )
              """
                .trimIndent()
            )
            bind(After.DATA_AVAILABILITY_START_TIME)
              .to(filter.after.dataAvailabilityStartTime.toGcloudTimestamp())
          }
          StreamEventGroupsRequest.OrderBy.Field.UNRECOGNIZED -> error("Unrecognized field")
        }

        bind(After.EXTERNAL_DATA_PROVIDER_ID).to(afterEventGroupKey.externalDataProviderId)
        bind(After.EXTERNAL_EVENT_GROUP_ID).to(afterEventGroupKey.externalEventGroupId)
      }
    }

    if (conjuncts.isEmpty()) {
      return
    }

    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  private object After {
    const val EXTERNAL_EVENT_GROUP_ID = "after_externalEventGroupId"
    const val EXTERNAL_DATA_PROVIDER_ID = "after_externalDataProviderId"
    const val DATA_AVAILABILITY_START_TIME = "after_dataAvailabilityStartTime"
  }

  companion object {
    const val LIMIT = "limit"
    const val EXTERNAL_DATA_PROVIDER_ID = "externalDataProviderId"
    const val EXTERNAL_MEASUREMENT_CONSUMER_ID = "externalMeasurementConsumerId"
    const val EXTERNAL_MEASUREMENT_CONSUMER_IDS = "externalMeasurementConsumerIds"
    const val EXTERNAL_DATA_PROVIDER_IDS = "externalDataProviderIds"
    const val DELETED_STATE = "deletedState"
    const val MEDIA_TYPES = "mediaTypes"
    const val DATA_AVAILABILITY_START_TIME_LTE = "dataAvailabilityStartTimeLte"
    const val DATA_AVAILABILITY_END_TIME_GTE = "dataAvailabilityEndTimeGte"
    const val DATA_AVAILABILITY_START_TIME_GTE = "dataAvailabilityStartTimeGte"
    const val DATA_AVAILABILITY_END_TIME_LTE = "dataAvailabilityEndTimeLte"
    const val METADATA_SEARCH_QUERY = "metadataSearchQuery"
    const val TIMESTAMP_MAX = "timestampMax"
  }
}
