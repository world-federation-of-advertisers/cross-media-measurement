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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader

class StreamMeasurements(
  view: Measurement.View,
  private val requestFilter: StreamMeasurementsRequest.Filter,
  limit: Int = 0
) : SpannerQuery<MeasurementReader.Result, MeasurementReader.Result>() {
  override val reader =
    MeasurementReader(view).fillStatementBuilder {
      appendWhereClause(requestFilter)
      when (view) {
        Measurement.View.COMPUTATION ->
          appendClause("ORDER BY UpdateTime ASC, ExternalComputationId ASC")
        Measurement.View.DEFAULT ->
          appendClause("ORDER BY UpdateTime ASC, ExternalMeasurementId ASC")
        Measurement.View.UNRECOGNIZED -> error("Unrecognized View")
      }
      if (limit > 0) {
        appendClause("LIMIT @$LIMIT_PARAM")
        bind(LIMIT_PARAM to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamMeasurementsRequest.Filter) {
    val conjuncts = mutableListOf<String>()

    if (filter.externalMeasurementConsumerId != 0L) {
      conjuncts.add("ExternalMeasurementConsumerId = @$EXTERNAL_MEASUREMENT_CONSUMER_ID_PARAM")
      bind(EXTERNAL_MEASUREMENT_CONSUMER_ID_PARAM to filter.externalMeasurementConsumerId)
    }

    if (filter.externalMeasurementConsumerCertificateId != 0L) {
      conjuncts.add(
        "ExternalMeasurementConsumerCertificateId = " +
          "@$EXTERNAL_MEASUREMENT_CONSUMER_CERTIFICATE_ID_PARAM"
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
      if (filter.hasExternalMeasurementIdAfter()) {
        conjuncts.add(
          """
          ((UpdateTime > @$UPDATED_AFTER)
          OR (UpdateTime = @$UPDATED_AFTER
          AND ExternalMeasurementId > @$EXTERNAL_MEASUREMENT_ID_AFTER))
        """
            .trimIndent()
        )
        bind(EXTERNAL_MEASUREMENT_ID_AFTER).to(filter.externalMeasurementIdAfter)
      } else if (filter.hasExternalComputationIdAfter()) {
        conjuncts.add(
          """
          ((UpdateTime > @$UPDATED_AFTER)
          OR (UpdateTime = @$UPDATED_AFTER
          AND ExternalComputationId > @$EXTERNAL_COMPUTATION_ID_AFTER))
        """
            .trimIndent()
        )
        bind(EXTERNAL_COMPUTATION_ID_AFTER).to(filter.externalComputationIdAfter)
      } else {
        error("external_measurement_id_after or external_measurement_id_after required")
      }
      bind(UPDATED_AFTER to filter.updatedAfter.toGcloudTimestamp())
    }

    if (conjuncts.isEmpty()) {
      return
    }

    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  override fun Flow<MeasurementReader.Result>.transform(): Flow<MeasurementReader.Result> {
    // TODO(@tristanvuong): determine how to do this in the SQL query instead
    if (requestFilter.externalDuchyId.isBlank()) {
      return this
    }

    return filter { value: MeasurementReader.Result ->
      value.measurement.computationParticipantsList
        .map { it.externalDuchyId }
        .contains(requestFilter.externalDuchyId)
    }
  }

  companion object {
    const val LIMIT_PARAM = "limit"
    const val EXTERNAL_MEASUREMENT_CONSUMER_ID_PARAM = "externalMeasurementConsumerId"
    const val EXTERNAL_MEASUREMENT_CONSUMER_CERTIFICATE_ID_PARAM =
      "externalMeasurementConsumerCertificateId"
    const val UPDATED_AFTER = "updatedAfter"
    const val STATES_PARAM = "states"
    const val EXTERNAL_MEASUREMENT_ID_AFTER = "externalMeasurementIdAfter"
    const val EXTERNAL_COMPUTATION_ID_AFTER = "externalComputationIdAfter"
  }
}
