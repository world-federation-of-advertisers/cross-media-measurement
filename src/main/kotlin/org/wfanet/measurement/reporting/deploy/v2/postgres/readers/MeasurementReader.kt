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

package org.wfanet.measurement.reporting.deploy.v2.postgres.readers

import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.common.db.r2dbc.BoundStatement
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.v2.Measurement
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.TimeInterval
import org.wfanet.measurement.internal.reporting.v2.measurement
import org.wfanet.measurement.internal.reporting.v2.timeInterval

class MeasurementReader(private val readContext: ReadContext) {
  data class Result(
    val measurementConsumerId: InternalId,
    val measurementId: InternalId,
    val measurement: Measurement
  )

  private data class MeasurementInfo(
    val measurementConsumerId: InternalId,
    val cmmsMeasurementConsumerId: String,
    val cmmsMeasurementId: String?,
    val cmmsCreateMeasurementRequestId: String,
    val timeInterval: TimeInterval,
    // Key is primitiveReportingSetBasisId.
    val primitiveReportingSetBasisInfoMap: MutableMap<InternalId, PrimitiveReportingSetBasisInfo>,
    val state: Measurement.State,
    val details: Measurement.Details,
  )

  private data class PrimitiveReportingSetBasisInfo(
    val externalReportingSetId: ExternalId,
    val filterSet: MutableSet<String>,
  )

  private val baseSql: String =
    """
    SELECT
      Measurements.MeasurementConsumerId,
      MeasurementConsumers.CmmsMeasurementConsumerId,
      Measurements.MeasurementId,
      Measurements.CmmsCreateMeasurementRequestId,
      Measurements.CmmsMeasurementId,
      Measurements.TimeIntervalStart AS MeasurementsTimeIntervalStart,
      Measurements.TimeIntervalEndExclusive AS MeasurementsTimeIntervalEndExclusive,
      Measurements.State,
      Measurements.MeasurementDetails,
      PrimitiveReportingSetBases.PrimitiveReportingSetBasisId,
      ReportingSets.ExternalReportingSetId AS PrimitiveExternalReportingSetId,
      PrimitiveReportingSetBasisFilters.Filter AS PrimitiveReportingSetBasisFilter
    FROM MeasurementConsumers
    JOIN Measurements USING(MeasurementConsumerId)
    JOIN MeasurementPrimitiveReportingSetBases USING(MeasurementConsumerId, MeasurementId)
    JOIN PrimitiveReportingSetBases USING(MeasurementConsumerId, PrimitiveReportingSetBasisId)
    JOIN ReportingSets
      ON PrimitiveReportingSetBases.MeasurementConsumerId = ReportingSets.MeasurementConsumerId
      AND PrimitiveReportingSetBases.PrimitiveReportingSetId = ReportingSets.ReportingSetId
    LEFT JOIN PrimitiveReportingSetBasisFilters
      ON PrimitiveReportingSetBases.MeasurementConsumerId = PrimitiveReportingSetBasisFilters.MeasurementConsumerId
      AND PrimitiveReportingSetBases.PrimitiveReportingSetBasisId = PrimitiveReportingSetBasisFilters.PrimitiveReportingSetBasisId
    """
      .trimIndent()

  fun readMeasurementsByCmmsCreateRequestId(
    measurementConsumerId: InternalId,
    cmmsCreateMeasurementRequestIds: Collection<String>
  ): Flow<Result> {
    if (cmmsCreateMeasurementRequestIds.isEmpty()) {
      return emptyFlow()
    }

    val sql =
      StringBuilder(
        baseSql +
          """
          WHERE Measurements.MeasurementConsumerId = $1
            AND CmmsCreateMeasurementRequestId::text IN
          """
      )

    var i = 2
    val bindingMap = mutableMapOf<String, String>()
    val inList =
      cmmsCreateMeasurementRequestIds.joinToString(separator = ",", prefix = "(", postfix = ")") {
        val index = "$$i"
        bindingMap[it] = index
        i++
        index
      }
    sql.append(inList)

    val statement =
      boundStatement(sql.toString()) {
        bind("$1", measurementConsumerId)
        cmmsCreateMeasurementRequestIds.forEach { bind(bindingMap.getValue(it), it) }
      }

    return createResultFlow(statement)
  }

  fun readMeasurementsByCmmsId(
    measurementConsumerId: InternalId,
    cmmsMeasurementIds: Collection<String>
  ): Flow<Result> {
    if (cmmsMeasurementIds.isEmpty()) {
      return emptyFlow()
    }

    val sql =
      StringBuilder(
        baseSql +
          """
          WHERE Measurements.MeasurementConsumerId = $1
            AND CmmsMeasurementId IN
          """
      )

    var i = 2
    val bindingMap = mutableMapOf<String, String>()
    val inList =
      cmmsMeasurementIds.joinToString(separator = ",", prefix = "(", postfix = ")") {
        val index = "$$i"
        bindingMap[it] = index
        i++
        index
      }
    sql.append(inList)

    val statement =
      boundStatement(sql.toString()) {
        bind("$1", measurementConsumerId)
        cmmsMeasurementIds.forEach { bind(bindingMap.getValue(it), it) }
      }

    return createResultFlow(statement)
  }

  private fun createResultFlow(statement: BoundStatement): Flow<Result> {
    return flow {
      val measurementInfoMap = buildResultMap(statement)

      for (entry in measurementInfoMap) {
        val measurementId = entry.key
        val measurementInfo = entry.value

        val measurement = measurement {
          cmmsMeasurementConsumerId = measurementInfo.cmmsMeasurementConsumerId
          if (measurementInfo.cmmsMeasurementId != null) {
            cmmsMeasurementId = measurementInfo.cmmsMeasurementId
          }
          cmmsCreateMeasurementRequestId = measurementInfo.cmmsCreateMeasurementRequestId
          timeInterval = measurementInfo.timeInterval
          measurementInfo.primitiveReportingSetBasisInfoMap.values.forEach {
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = it.externalReportingSetId.value
                filters += it.filterSet
              }
          }
          state = measurementInfo.state
          if (measurementInfo.details != Measurement.Details.getDefaultInstance()) {
            details = measurementInfo.details
          }
        }

        emit(
          Result(
            measurementConsumerId = measurementInfo.measurementConsumerId,
            measurementId = measurementId,
            measurement = measurement,
          )
        )
      }
    }
  }

  /** Returns a map that maintains the order of the query result. */
  private suspend fun buildResultMap(statement: BoundStatement): Map<InternalId, MeasurementInfo> {
    // Key is measurementId.
    val measurementInfoMap: MutableMap<InternalId, MeasurementInfo> = linkedMapOf()

    val translate: (row: ResultRow) -> Unit = { row: ResultRow ->
      val measurementConsumerId: InternalId = row["MeasurementConsumerId"]
      val cmmsMeasurementConsumerId: String = row["CmmsMeasurementConsumerId"]
      val measurementId: InternalId = row["MeasurementId"]
      val cmmsCreateMeasurementRequestId: UUID = row["CmmsCreateMeasurementRequestId"]
      val cmmsMeasurementId: String? = row["CmmsMeasurementId"]
      val measurementTimeIntervalStart: Instant = row["MeasurementsTimeIntervalStart"]
      val measurementTimeIntervalEnd: Instant = row["MeasurementsTimeIntervalEndExclusive"]
      val measurementState: Measurement.State = Measurement.State.forNumber(row["State"])
      val measurementDetails: Measurement.Details =
        row.getProtoMessage("MeasurementDetails", Measurement.Details.parser())
      val primitiveReportingSetBasisId: InternalId = row["PrimitiveReportingSetBasisId"]
      val primitiveExternalReportingSetId: ExternalId = row["PrimitiveExternalReportingSetId"]
      val primitiveReportingSetBasisFilter: String? = row["PrimitiveReportingSetBasisFilter"]

      val measurementInfo =
        measurementInfoMap.computeIfAbsent(measurementId) {
          val timeInterval = timeInterval {
            startTime = measurementTimeIntervalStart.toProtoTime()
            endTime = measurementTimeIntervalEnd.toProtoTime()
          }

          MeasurementInfo(
            measurementConsumerId = measurementConsumerId,
            cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
            cmmsMeasurementId = cmmsMeasurementId,
            cmmsCreateMeasurementRequestId = cmmsCreateMeasurementRequestId.toString(),
            timeInterval = timeInterval,
            state = measurementState,
            details = measurementDetails,
            primitiveReportingSetBasisInfoMap = mutableMapOf(),
          )
        }

      val primitiveReportingSetBasisInfo =
        measurementInfo.primitiveReportingSetBasisInfoMap.computeIfAbsent(
          primitiveReportingSetBasisId
        ) {
          PrimitiveReportingSetBasisInfo(
            externalReportingSetId = primitiveExternalReportingSetId,
            filterSet = mutableSetOf()
          )
        }

      if (primitiveReportingSetBasisFilter != null) {
        primitiveReportingSetBasisInfo.filterSet.add(primitiveReportingSetBasisFilter)
      }
    }

    readContext.executeQuery(statement).consume(translate).collect {}

    return measurementInfoMap
  }
}
