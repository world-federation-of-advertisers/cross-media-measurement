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

import com.google.cloud.spanner.Statement
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.BaseSpannerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader

class StreamMeasurements(
  view: Measurement.View,
  requestFilter: StreamMeasurementsRequest.Filter,
  limit: Int = 0,
) : SimpleSpannerQuery<MeasurementReader.Result>() {

  override val reader: BaseSpannerReader<MeasurementReader.Result> =
    buildReader(view, requestFilter, limit)

  companion object {
    private const val LIMIT_PARAM = "limit"
    private const val EXTERNAL_MEASUREMENT_CONSUMER_ID_PARAM = "externalMeasurementConsumerId"
    private const val EXTERNAL_MEASUREMENT_CONSUMER_CERTIFICATE_ID_PARAM =
      "externalMeasurementConsumerCertificateId"
    private const val UPDATED_AFTER = "updatedAfter"
    private const val UPDATED_BEFORE = "updatedBefore"
    private const val CREATED_BEFORE = "createdBefore"
    private const val CREATED_AFTER = "createdAfter"
    private const val STATES_PARAM = "states"
    private const val DUCHY_ID_PARAM = "duchyId"

    private object AfterParams {
      const val UPDATE_TIME = "after_updateTime"
      const val EXTERNAL_MEASUREMENT_CONSUMER_ID = "after_externalMeasurementConsumerId"
      const val EXTERNAL_MEASUREMENT_ID = "after_externalMeasurementId"
      const val EXTERNAL_COMPUTATION_ID = "after_externalComputationId"
    }

    private fun buildReader(
      view: Measurement.View,
      requestFilter: StreamMeasurementsRequest.Filter,
      limit: Int,
    ): MeasurementReader {
      val orderByClause = getOrderByClause(view)
      val index: MeasurementReader.Index =
        if (requiresExternalComputationId(view, requestFilter)) {
          // This is a NULL_FILTERED sharded index, so it can only be used when
          // ExternalComputationId is not null.
          MeasurementReader.Index.CONTINUATION_TOKEN
        } else {
          MeasurementReader.Index.NONE
        }
      return MeasurementReader(view, index).apply {
        this.orderByClause = orderByClause
        fillStatementBuilder {
          appendWhereClause(view, requestFilter)
          appendClause(orderByClause)
          if (limit > 0) {
            appendClause("LIMIT @$LIMIT_PARAM")
            bind(LIMIT_PARAM to limit.toLong())
          }
        }
      }
    }

    private fun requiresExternalComputationId(
      view: Measurement.View,
      requestFilter: StreamMeasurementsRequest.Filter,
    ): Boolean {
      return requestFilter.hasExternalComputationId ||
        view == Measurement.View.COMPUTATION ||
        view == Measurement.View.COMPUTATION_STATS
    }

    private fun getOrderByClause(view: Measurement.View): String {
      return when (view) {
        Measurement.View.COMPUTATION,
        Measurement.View.COMPUTATION_STATS ->
          "ORDER BY Measurements.UpdateTime ASC, ExternalComputationId ASC"
        Measurement.View.DEFAULT ->
          "ORDER BY Measurements.UpdateTime ASC, ExternalMeasurementConsumerId ASC, " +
            "ExternalMeasurementId ASC"
        Measurement.View.UNRECOGNIZED -> error("Unrecognized View")
      }
    }

    private fun Statement.Builder.appendWhereClause(
      view: Measurement.View,
      filter: StreamMeasurementsRequest.Filter,
    ) {
      val conjuncts = mutableListOf<String>()

      if (requiresExternalComputationId(view, filter)) {
        conjuncts.add("ExternalComputationId IS NOT NULL")
        conjuncts.add("MeasurementIndexShardId >= 0")
      }

      if (filter.externalMeasurementConsumerId != 0L) {
        conjuncts.add("ExternalMeasurementConsumerId = @$EXTERNAL_MEASUREMENT_CONSUMER_ID_PARAM")
        bind(EXTERNAL_MEASUREMENT_CONSUMER_ID_PARAM to filter.externalMeasurementConsumerId)
      }

      if (filter.externalMeasurementConsumerCertificateId != 0L) {
        conjuncts.add(
          """
          @$EXTERNAL_MEASUREMENT_CONSUMER_CERTIFICATE_ID_PARAM IN (
            SELECT ExternalMeasurementConsumerCertificateId
            FROM MeasurementConsumerCertificates
            WHERE
              MeasurementConsumerCertificates.MeasurementConsumerId = Measurements.MeasurementConsumerId
              AND MeasurementConsumerCertificates.CertificateId = Measurements.CertificateId
          )
          """
            .trimIndent()
        )
        bind(
          EXTERNAL_MEASUREMENT_CONSUMER_CERTIFICATE_ID_PARAM to
            filter.externalMeasurementConsumerCertificateId
        )
      }

      if (filter.statesValueList.isNotEmpty()) {
        conjuncts.add("Measurements.State IN UNNEST(@$STATES_PARAM)")
        bind(STATES_PARAM).toInt64Array(filter.statesValueList.map { it.toLong() })
      }

      if (filter.hasUpdatedAfter()) {
        conjuncts.add("Measurements.UpdateTime > @$UPDATED_AFTER")
        bind(UPDATED_AFTER).to(filter.updatedAfter.toGcloudTimestamp())
      }

      if (filter.hasUpdatedBefore()) {
        conjuncts.add("Measurements.UpdateTime < @$UPDATED_BEFORE")
        bind(UPDATED_BEFORE to filter.updatedBefore.toGcloudTimestamp())
      }

      if (filter.hasCreatedBefore()) {
        conjuncts.add("Measurements.CreateTime < @$CREATED_BEFORE")
        bind(CREATED_BEFORE to filter.createdBefore.toGcloudTimestamp())
      }

      if (filter.hasCreatedAfter()) {
        conjuncts.add("Measurements.CreateTime > @$CREATED_AFTER")
        bind(CREATED_AFTER to filter.createdAfter.toGcloudTimestamp())
      }

      if (filter.externalDuchyId.isNotEmpty()) {
        val duchyId: Long =
          DuchyIds.getInternalId(filter.externalDuchyId)
            ?: throw DuchyNotFoundException(filter.externalDuchyId)
        conjuncts.add(
          """
          @$DUCHY_ID_PARAM IN (
            SELECT DuchyId
            FROM ComputationParticipants
            WHERE
              ComputationParticipants.MeasurementConsumerId = Measurements.MeasurementConsumerId
              AND ComputationParticipants.MeasurementId = Measurements.MeasurementId
          )
          """
            .trimIndent()
        )
        bind(DUCHY_ID_PARAM).to(duchyId)
      }

      if (filter.hasAfter()) {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf case fields cannot be null.
        when (filter.after.keyCase) {
          StreamMeasurementsRequest.Filter.After.KeyCase.MEASUREMENT -> {
            // CASE implements short-circuiting, which fixes performance issues with this filter.
            conjuncts.add(
              """
              CASE
                WHEN Measurements.UpdateTime > @${AfterParams.UPDATE_TIME} THEN TRUE
                WHEN Measurements.UpdateTime = @${AfterParams.UPDATE_TIME}
                  AND ExternalMeasurementConsumerId > @${AfterParams.EXTERNAL_MEASUREMENT_CONSUMER_ID} THEN TRUE
                WHEN Measurements.UpdateTime = @${AfterParams.UPDATE_TIME}
                  AND ExternalMeasurementConsumerId = @${AfterParams.EXTERNAL_MEASUREMENT_CONSUMER_ID}
                  AND ExternalMeasurementId > @${AfterParams.EXTERNAL_MEASUREMENT_ID} THEN TRUE
                ELSE FALSE
              END
              """
                .trimIndent()
            )
            bind(AfterParams.UPDATE_TIME).to(filter.after.updateTime.toGcloudTimestamp())
            bind(
              AfterParams.EXTERNAL_MEASUREMENT_CONSUMER_ID to
                filter.after.measurement.externalMeasurementConsumerId
            )
            bind(
              AfterParams.EXTERNAL_MEASUREMENT_ID to filter.after.measurement.externalMeasurementId
            )
          }
          StreamMeasurementsRequest.Filter.After.KeyCase.COMPUTATION -> {
            conjuncts.add(
              """
              (
                Measurements.UpdateTime > @${AfterParams.UPDATE_TIME}
                OR (
                  Measurements.UpdateTime = @${AfterParams.UPDATE_TIME}
                  AND ExternalComputationId > @${AfterParams.EXTERNAL_COMPUTATION_ID}
                )
              )
              """
                .trimIndent()
            )
            bind(AfterParams.UPDATE_TIME).to(filter.after.updateTime.toGcloudTimestamp())
            bind(
              AfterParams.EXTERNAL_COMPUTATION_ID to filter.after.computation.externalComputationId
            )
          }
          StreamMeasurementsRequest.Filter.After.KeyCase.KEY_NOT_SET ->
            throw IllegalArgumentException("key not set")
        }
      }

      if (conjuncts.isEmpty()) {
        return
      }

      appendClause("WHERE ")
      append(conjuncts.joinToString(" AND "))
    }
  }
}
