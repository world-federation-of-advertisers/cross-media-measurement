// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.postgres.readers

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.google.protobuf.duration
import com.google.protobuf.timestamp
import com.google.protobuf.util.JsonFormat
import java.time.Instant
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.Metric
import org.wfanet.measurement.internal.reporting.Metric.SetOperation
import org.wfanet.measurement.internal.reporting.MetricKt
import org.wfanet.measurement.internal.reporting.MetricKt.namedSetOperation
import org.wfanet.measurement.internal.reporting.MetricKt.setOperation
import org.wfanet.measurement.internal.reporting.PeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.Report
import org.wfanet.measurement.internal.reporting.StreamReportsRequest
import org.wfanet.measurement.internal.reporting.TimeIntervals
import org.wfanet.measurement.internal.reporting.metric
import org.wfanet.measurement.internal.reporting.periodicTimeInterval
import org.wfanet.measurement.internal.reporting.report
import org.wfanet.measurement.internal.reporting.timeInterval
import org.wfanet.measurement.internal.reporting.timeIntervals
import org.wfanet.measurement.reporting.service.internal.ReportNotFoundException

class ReportReader {
  data class Result(
    val measurementConsumerReferenceId: String,
    val reportId: InternalId,
    val report: Report
  )

  private val baseSql: String =
    """
    SELECT
      MeasurementConsumerReferenceId,
      ReportId,
      ExternalReportId,
      State,
      ReportDetails,
      ReportIdempotencyKey,
      CreateTime,
      (
        SELECT ARRAY(
          SELECT
            json_build_object(
              'timeIntervalId', TimeIntervalId,
              'startSeconds', StartSeconds,
              'startNanos', StartNanos,
              'endSeconds', EndSeconds,
              'endNanos', EndNanos
            )
          FROM TimeIntervals
          WHERE TimeIntervals.MeasurementConsumerReferenceId = Reports.MeasurementConsumerReferenceId
          AND TimeIntervals.ReportId = Reports.ReportId
        )
      ) AS TimeIntervals,
      (
        SELECT json_build_object(
          'intervalCount', IntervalCount,
          'startSeconds', StartSeconds,
          'startNanos', StartNanos,
          'incrementSeconds', IncrementSeconds,
          'incrementNanos', IncrementNanos
        )
        FROM PeriodicTimeIntervals
        WHERE PeriodicTimeIntervals.MeasurementConsumerReferenceId = Reports.MeasurementConsumerReferenceId
        AND PeriodicTimeIntervals.ReportId = Reports.ReportId
      ) AS PeriodicTimeInterval,
      (
        SELECT array_agg(
          json_build_object(
            'metricId', MetricId,
            'metricDetails', MetricDetailsJson,
            'namedSetOperations', NamedSetOperations,
            'setOperations', SetOperations
          )
        )
        FROM (
          SELECT
            MetricId,
            MetricDetailsJson,
            (
              SELECT json_agg(
                json_build_object(
                  'displayName', DisplayName,
                  'setOperationId', SetOperationId,
                  'measurementCalculations', MeasurementCalculations
                )
              )
              FROM (
                SELECT
                  DisplayName,
                  SetOperationId,
                  (
                    SELECT json_agg(
                      json_build_object(
                        'timeInterval', TimeInterval,
                        'weightedMeasurements', WeightedMeasurements
                      )
                    )
                    FROM (
                      SELECT
                        (
                          SELECT json_build_object(
                            'startSeconds', StartSeconds,
                            'startNanos', StartNanos,
                            'endSeconds', EndSeconds,
                            'endNanos', EndNanos
                          )
                          FROM TimeIntervals
                          WHERE MeasurementCalculations.MeasurementConsumerReferenceId = TimeIntervals.MeasurementConsumerReferenceId
                            AND MeasurementCalculations.ReportId = TimeIntervals.ReportId
                            AND MeasurementCalculations.TimeIntervalId = TimeIntervals.TimeIntervalId
                        ) AS TimeInterval,
                        (
                          SELECT json_agg(
                            json_build_object(
                              'measurementReferenceId', MeasurementReferenceId,
                              'coefficient', Coefficient
                            )
                          )
                          FROM WeightedMeasurements
                          Where WeightedMeasurements.MeasurementConsumerReferenceId = MeasurementCalculations.MeasurementConsumerReferenceId
                            AND WeightedMeasurements.ReportId = MeasurementCalculations.ReportId
                            AND WeightedMeasurements.MetricId = MeasurementCalculations.MetricId
                            AND WeightedMeasurements.NamedSetOperationId = MeasurementCalculations.NamedSetOperationId
                            AND WeightedMeasurements.MeasurementCalculationId = MeasurementCalculations.MeasurementCalculationId
                        ) AS WeightedMeasurements
                      FROM MeasurementCalculations
                      Where MeasurementCalculations.MeasurementConsumerReferenceId = NamedSetOperations.MeasurementConsumerReferenceId
                        AND MeasurementCalculations.ReportId = NamedSetOperations.ReportId
                        AND MeasurementCalculations.MetricId = NamedSetOperations.MetricId
                        AND MeasurementCalculations.NamedSetOperationId = NamedSetOperations.NamedSetOperationId
                    ) MeasurementCalculations
                  ) AS MeasurementCalculations
              FROM NamedSetOperations
              WHERE NamedSetOperations.MeasurementConsumerReferenceId = Metrics.MeasurementConsumerReferenceId
                AND NamedSetOperations.ReportId = Metrics.ReportId
                AND NamedSetOperations.MetricId = Metrics.MetricId
            ) AS NamedSetOperations) AS NamedSetOperations,
            (
              SELECT json_agg(
                json_build_object(
                  'type', Type,
                  'setOperationId', SetOperationId,
                  'leftHandSetOperationId', LeftHandSetOperationId,
                  'rightHandSetOperationId', RightHandSetOperationId,
                  'leftHandReportingSetId',
                  (
                    SELECT ExternalReportingSetId
                    FROM ReportingSets
                    WHERE SetOperations.MeasurementConsumerReferenceId = ReportingSets.MeasurementConsumerReferenceId
                    AND ReportingSetId = LeftHandReportingSetId
                  ),
                  'rightHandReportingSetId',
                  (
                    SELECT ExternalReportingSetId
                    FROM ReportingSets
                    WHERE SetOperations.MeasurementConsumerReferenceId = ReportingSets.MeasurementConsumerReferenceId
                    AND ReportingSetId = RightHandReportingSetId
                  )
                )
              )
              FROM SetOperations
              Where SetOperations.MeasurementConsumerReferenceId = Metrics.MeasurementConsumerReferenceId
                AND SetOperations.ReportId = Metrics.ReportId
                AND SetOperations.MetricId = Metrics.MetricId
            ) AS SetOperations
          FROM Metrics
          WHERE Metrics.MeasurementConsumerReferenceId = Reports.MeasurementConsumerReferenceId
          AND Metrics.ReportId = Reports.ReportId
        ) AS Metrics
      ) AS Metrics
    FROM Reports
    """

  fun translate(row: ResultRow): Result =
    Result(row["MeasurementConsumerReferenceId"], row["ReportId"], buildReport(row))

  /**
   * Gets the report by external report id.
   *
   * @throws [ReportNotFoundException]
   */
  suspend fun getReportByExternalId(
    readContext: ReadContext,
    measurementConsumerReferenceId: String,
    externalReportId: Long
  ): Result {
    val statement =
      boundStatement(
        (baseSql +
          """
        WHERE MeasurementConsumerReferenceId = $1
          AND ExternalReportId = $2
          """)
      ) {
        bind("$1", measurementConsumerReferenceId)
        bind("$2", externalReportId)
      }

    return readContext.executeQuery(statement).consume(::translate).singleOrNull()
      ?: throw ReportNotFoundException()
  }

  fun listReports(
    client: DatabaseClient,
    filter: StreamReportsRequest.Filter,
    limit: Int = 0
  ): Flow<Result> {
    val statement =
      boundStatement(
        baseSql +
          """
        WHERE MeasurementConsumerReferenceId = $1
          AND ExternalReportId > $2
        ORDER BY ExternalReportId ASC
        LIMIT $3
        """
      ) {
        bind("$1", filter.measurementConsumerReferenceId)
        bind("$2", filter.externalReportIdAfter)
        if (limit > 0) {
          bind("$3", limit)
        } else {
          bind("$3", 50)
        }
      }

    return flow {
      val readContext = client.readTransaction()
      try {
        emitAll(readContext.executeQuery(statement).consume(::translate))
      } finally {
        readContext.close()
      }
    }
  }

  private fun buildReport(row: ResultRow): Report {
    return report {
      measurementConsumerReferenceId = row["MeasurementConsumerReferenceId"]
      externalReportId = row["ExternalReportId"]
      state = Report.State.forNumber(row["State"])
      details = row.getProtoMessage("ReportDetails", Report.Details.parser())
      reportIdempotencyKey = row["ReportIdempotencyKey"]
      createTime = row.get<Instant>("CreateTime").toProtoTime()
      val periodicTimeInterval: String? = row["PeriodicTimeInterval"]
      if (periodicTimeInterval != null) {
        this.periodicTimeInterval =
          buildPeriodicTimeInterval(JsonParser.parseString(periodicTimeInterval).asJsonObject)
      } else {
        timeIntervals = buildTimeIntervals(row["TimeIntervals"])
      }
      metrics += buildMetrics(measurementConsumerReferenceId, row["Metrics"])
    }
  }

  private fun buildPeriodicTimeInterval(jsonObject: JsonObject): PeriodicTimeInterval {
    return periodicTimeInterval {
      startTime = timestamp {
        seconds = jsonObject.getAsJsonPrimitive("startSeconds").asLong
        nanos = jsonObject.getAsJsonPrimitive("startNanos").asInt
      }
      increment = duration {
        seconds = jsonObject.getAsJsonPrimitive("incrementSeconds").asLong
        nanos = jsonObject.getAsJsonPrimitive("incrementNanos").asInt
      }
      intervalCount = jsonObject.getAsJsonPrimitive("intervalCount").asInt
    }
  }

  private fun buildTimeIntervals(timeIntervalsArr: Array<String>): TimeIntervals {
    return timeIntervals {
      timeIntervalsArr.forEach {
        val timeIntervalObject = JsonParser.parseString(it).asJsonObject
        timeIntervals += timeInterval {
          startTime = timestamp {
            seconds = timeIntervalObject.getAsJsonPrimitive("startSeconds").asLong
            nanos = timeIntervalObject.getAsJsonPrimitive("startNanos").asInt
          }
          endTime = timestamp {
            seconds = timeIntervalObject.getAsJsonPrimitive("endSeconds").asLong
            nanos = timeIntervalObject.getAsJsonPrimitive("endNanos").asInt
          }
        }
      }
    }
  }

  private fun buildMetrics(
    measurementConsumerReferenceId: String,
    metricsArr: Array<String>
  ): Collection<Metric> {
    val metricsList = ArrayList<Metric>(metricsArr.size)
    metricsArr.forEach {
      val metricObject = JsonParser.parseString(it).asJsonObject
      metricsList.add(
        metric {
          val detailsBuilder = Metric.Details.newBuilder()
          JsonFormat.parser()
            .ignoringUnknownFields()
            .merge(metricObject.getAsJsonPrimitive("metricDetails").asString, detailsBuilder)
          details = detailsBuilder.build()

          val setOperationsArr = metricObject.getAsJsonArray("setOperations")
          val setOperationsMap = mutableMapOf<Long, JsonObject>()
          setOperationsArr.forEach { setOperationElement ->
            val setOperationObject = setOperationElement.asJsonObject
            setOperationsMap[setOperationObject.getAsJsonPrimitive("setOperationId").asLong] =
              setOperationObject
          }

          val namedSetOperationsArr = metricObject.getAsJsonArray("namedSetOperations")
          namedSetOperationsArr.forEach { namedSetOperationElement ->
            val namedSetOperationObject = namedSetOperationElement.asJsonObject
            namedSetOperations += namedSetOperation {
              displayName = namedSetOperationObject.getAsJsonPrimitive("displayName").asString
              val setOperationId =
                namedSetOperationObject.getAsJsonPrimitive("setOperationId").asLong
              setOperation =
                buildSetOperation(measurementConsumerReferenceId, setOperationId, setOperationsMap)
              val measurementCalculationsArr =
                namedSetOperationObject.getAsJsonArray("measurementCalculations")
              measurementCalculationsArr.forEach { measurementCalculationElement ->
                val measurementCalculationObject = measurementCalculationElement.asJsonObject
                measurementCalculations +=
                  MetricKt.measurementCalculation {
                    val timeIntervalObject =
                      measurementCalculationObject.getAsJsonObject("timeInterval")
                    timeInterval = timeInterval {
                      startTime = timestamp {
                        seconds = timeIntervalObject.getAsJsonPrimitive("startSeconds").asLong
                        nanos = timeIntervalObject.getAsJsonPrimitive("startNanos").asInt
                      }
                      endTime = timestamp {
                        seconds = timeIntervalObject.getAsJsonPrimitive("endSeconds").asLong
                        nanos = timeIntervalObject.getAsJsonPrimitive("endNanos").asInt
                      }
                    }
                    val weightedMeasurementsArr =
                      measurementCalculationObject.getAsJsonArray("weightedMeasurements")
                    weightedMeasurementsArr.forEach { weightedMeasurementElement ->
                      val weightedMeasurementObject = weightedMeasurementElement.asJsonObject
                      weightedMeasurements +=
                        MetricKt.MeasurementCalculationKt.weightedMeasurement {
                          measurementReferenceId =
                            weightedMeasurementObject
                              .getAsJsonPrimitive("measurementReferenceId")
                              .asString
                          coefficient =
                            weightedMeasurementObject.getAsJsonPrimitive("coefficient").asInt
                        }
                    }
                  }
              }
            }
          }
        }
      )
    }
    return metricsList
  }

  private fun buildSetOperation(
    measurementConsumerReferenceId: String,
    setOperationId: Long,
    setOperationMap: Map<Long, JsonObject>
  ): SetOperation {
    val setOperationObject = setOperationMap[setOperationId]
    return setOperation {
      type = SetOperation.Type.forNumber(setOperationObject!!.getAsJsonPrimitive("type").asInt)
      lhs =
        MetricKt.SetOperationKt.operand {
          if (setOperationObject.get("leftHandSetOperationId").isJsonNull) {
            if (!setOperationObject.get("leftHandReportingSetId").isJsonNull) {
              reportingSetId =
                MetricKt.SetOperationKt.reportingSetKey {
                  this.measurementConsumerReferenceId = measurementConsumerReferenceId
                  externalReportingSetId =
                    setOperationObject.getAsJsonPrimitive("leftHandReportingSetId").asLong
                }
            }
          } else {
            operation =
              buildSetOperation(
                measurementConsumerReferenceId,
                setOperationObject.getAsJsonPrimitive("leftHandSetOperationId").asLong,
                setOperationMap
              )
          }
        }
      rhs =
        MetricKt.SetOperationKt.operand {
          if (setOperationObject.get("rightHandSetOperationId").isJsonNull) {
            if (!setOperationObject.get("rightHandReportingSetId").isJsonNull) {
              reportingSetId =
                MetricKt.SetOperationKt.reportingSetKey {
                  this.measurementConsumerReferenceId = measurementConsumerReferenceId
                  externalReportingSetId =
                    setOperationObject.getAsJsonPrimitive("rightHandReportingSetId").asLong
                }
            }
          } else {
            operation =
              buildSetOperation(
                measurementConsumerReferenceId,
                setOperationObject.getAsJsonPrimitive("rightHandSetOperationId").asLong,
                setOperationMap
              )
          }
        }
    }
  }
}
