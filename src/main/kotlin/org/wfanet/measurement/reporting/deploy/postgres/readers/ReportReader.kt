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
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.identity.InternalId
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
      (SELECT json_build_object('TimeIntervals', json_agg(
        json_build_object('TimeIntervalId', TimeIntervalId,
                          'StartSeconds', StartSeconds,
                          'StartNanos', StartNanos,
                          'EndSeconds', EndSeconds,
                          'EndNanos', EndNanos)
      ))
      FROM TimeIntervals
      WHERE TimeIntervals.MeasurementConsumerReferenceId = Reports.MeasurementConsumerReferenceId
        AND TimeIntervals.ReportId = Reports.ReportId
      ) TimeIntervals,
      (SELECT
        json_build_object('IntervalCount', IntervalCount,
                          'StartSeconds', StartSeconds,
                          'StartNanos', StartNanos,
                          'IncrementSeconds', IncrementSeconds,
                          'IncrementNanos', IncrementNanos)
      FROM PeriodicTimeIntervals
      WHERE PeriodicTimeIntervals.MeasurementConsumerReferenceId = Reports.MeasurementConsumerReferenceId
        AND PeriodicTimeIntervals.ReportId = Reports.ReportId
      ) PeriodicTimeInterval,
      (SELECT json_build_object('Metrics', json_agg(
        json_build_object('MetricId', MetricId,
                          'MetricDetails', MetricDetailsJson,
                          'NamedSetOperations', NamedSetOperations,
                          'SetOperations', SetOperations)
      ))
      FROM (
        SELECT
          MetricId,
          MetricDetailsJson,
          (SELECT json_agg(
            json_build_object('DisplayName', DisplayName,
                              'SetOperationId', SetOperationId,
                              'MeasurementCalculations', MeasurementCalculations)
          )
          FROM (
            SELECT
              DisplayName,
              SetOperationId,
              (SELECT json_agg(
                json_build_object('TimeInterval', TimeInterval,
                                  'WeightedMeasurements', WeightedMeasurements)
              )
              FROM
              (
                SELECT
                  (
                    SELECT json_build_object('StartSeconds', StartSeconds,
                                              'StartNanos', StartNanos,
                                              'EndSeconds', EndSeconds,
                                              'EndNanos', EndNanos)
                    FROM TimeIntervals
                    WHERE MeasurementCalculations.MeasurementConsumerReferenceId = TimeIntervals.MeasurementConsumerReferenceId
                      AND MeasurementCalculations.ReportId = TimeIntervals.ReportId
                      AND MeasurementCalculations.TimeIntervalId = TimeIntervals.TimeIntervalId
                  ) TimeInterval,
                  (
                    SELECT json_agg(json_build_object('MeasurementReferenceId', MeasurementReferenceId,
                                                       'Coefficient', Coefficient))
                    FROM WeightedMeasurements
                    Where WeightedMeasurements.MeasurementConsumerReferenceId = MeasurementCalculations.MeasurementConsumerReferenceId
                      AND WeightedMeasurements.ReportId = MeasurementCalculations.ReportId
                      AND WeightedMeasurements.MetricId = MeasurementCalculations.MetricId
                      AND WeightedMeasurements.NamedSetOperationId = MeasurementCalculations.NamedSetOperationId
                      AND WeightedMeasurements.MeasurementCalculationId = MeasurementCalculations.MeasurementCalculationId
                  ) WeightedMeasurements
                FROM MeasurementCalculations
                Where MeasurementCalculations.MeasurementConsumerReferenceId = NamedSetOperations.MeasurementConsumerReferenceId
                  AND MeasurementCalculations.ReportId = NamedSetOperations.ReportId
                  AND MeasurementCalculations.MetricId = NamedSetOperations.MetricId
                  AND MeasurementCalculations.NamedSetOperationId = NamedSetOperations.NamedSetOperationId
              ) MeasurementCalculations) MeasurementCalculations
            FROM NamedSetOperations
            WHERE NamedSetOperations.MeasurementConsumerReferenceId = Metrics.MeasurementConsumerReferenceId
              AND NamedSetOperations.ReportId = Metrics.ReportId
              AND NamedSetOperations.MetricId = Metrics.MetricId
          ) NamedSetOperations) NamedSetOperations,
          (SELECT json_agg(
            json_build_object('Type', Type,
                              'SetOperationId', SetOperationId,
                              'LeftHandSetOperationId', LeftHandSetOperationId,
                              'RightHandSetOperationId', RightHandSetOperationId,
                              'LeftHandReportingSetId',
                                (SELECT ExternalReportingSetId
                                FROM ReportingSets
                                WHERE SetOperations.MeasurementConsumerReferenceId = ReportingSets.MeasurementConsumerReferenceId
                                  AND ReportingSetId = LeftHandReportingSetId),
                              'RightHandReportingSetId',
                                (SELECT ExternalReportingSetId
                                  FROM ReportingSets
                                  WHERE SetOperations.MeasurementConsumerReferenceId = ReportingSets.MeasurementConsumerReferenceId
                                    AND ReportingSetId = RightHandReportingSetId))
          )
          FROM SetOperations
          Where SetOperations.MeasurementConsumerReferenceId = Metrics.MeasurementConsumerReferenceId
            AND SetOperations.ReportId = Metrics.ReportId
            AND SetOperations.MetricId = Metrics.MetricId) SetOperations
        FROM Metrics
        WHERE Metrics.MeasurementConsumerReferenceId = Reports.MeasurementConsumerReferenceId
          AND Metrics.ReportId = Reports.ReportId
      ) Metrics) Metrics
    FROM Reports
    """

  fun translate(row: ResultRow): Result =
    Result(row["MeasurementConsumerReferenceId"], row["ReportId"], buildReport(row))

  suspend fun getReportByExternalId(
    client: DatabaseClient,
    measurementConsumerReferenceId: String,
    externalReportId: Long
  ): Result {
    val readContext = client.singleUse()
    return getReportByExternalId(readContext, measurementConsumerReferenceId, externalReportId)
  }

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
        readContext.executeQuery(statement).consume(::translate).collect { emit(it) }
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
      val createTime: Instant = row["CreateTime"]
      this.createTime = timestamp {
        seconds = createTime.epochSecond
        nanos = createTime.nano
      }
      val periodicTimeInterval: String? = row["PeriodicTimeInterval"]
      if (periodicTimeInterval != null) {
        this.periodicTimeInterval = buildPeriodicTimeInterval(periodicTimeInterval)
      } else {
        timeIntervals = buildTimeIntervals(row["TimeIntervals"])
      }
      metrics += buildMetrics(measurementConsumerReferenceId, row["Metrics"])
    }
  }

  private fun buildPeriodicTimeInterval(jsonString: String): PeriodicTimeInterval {
    val jsonObject = JsonParser.parseString(jsonString).asJsonObject
    return periodicTimeInterval {
      startTime = timestamp {
        seconds = jsonObject.getAsJsonPrimitive("StartSeconds").asLong
        nanos = jsonObject.getAsJsonPrimitive("StartNanos").asInt
      }
      increment = duration {
        seconds = jsonObject.getAsJsonPrimitive("IncrementSeconds").asLong
        nanos = jsonObject.getAsJsonPrimitive("IncrementNanos").asInt
      }
      intervalCount = jsonObject.getAsJsonPrimitive("IntervalCount").asInt
    }
  }

  private fun buildTimeIntervals(jsonString: String): TimeIntervals {
    val jsonObject = JsonParser.parseString(jsonString).asJsonObject
    val timeIntervalArr = jsonObject.getAsJsonArray("TimeIntervals")
    return timeIntervals {
      timeIntervalArr.forEach {
        val timeIntervalObject = it.asJsonObject
        timeIntervals += timeInterval {
          startTime = timestamp {
            seconds = timeIntervalObject.getAsJsonPrimitive("StartSeconds").asLong
            nanos = timeIntervalObject.getAsJsonPrimitive("StartNanos").asInt
          }
          endTime = timestamp {
            seconds = timeIntervalObject.getAsJsonPrimitive("EndSeconds").asLong
            nanos = timeIntervalObject.getAsJsonPrimitive("EndNanos").asInt
          }
        }
      }
    }
  }

  private fun buildMetrics(
    measurementConsumerReferenceId: String,
    jsonString: String
  ): List<Metric> {
    val jsonObject = JsonParser.parseString(jsonString).asJsonObject
    val metricsArr = jsonObject.getAsJsonArray("Metrics")
    val metricsList = ArrayList<Metric>(metricsArr.size())
    metricsArr.forEach {
      val metricObject = it.asJsonObject
      metricsList.add(
        metric {
          val detailsBuilder = Metric.Details.newBuilder()
          JsonFormat.parser()
            .ignoringUnknownFields()
            .merge(metricObject.getAsJsonPrimitive("MetricDetails").asString, detailsBuilder)
          details = detailsBuilder.build()

          val setOperationsArr = metricObject.getAsJsonArray("SetOperations")
          val setOperationsMap = HashMap<Long, JsonObject>()
          setOperationsArr.forEach { setOperationElement ->
            val setOperationObject = setOperationElement.asJsonObject
            setOperationsMap[setOperationObject.getAsJsonPrimitive("SetOperationId").asLong] =
              setOperationObject
          }

          val namedSetOperationsArr = metricObject.getAsJsonArray("NamedSetOperations")
          namedSetOperationsArr.forEach { namedSetOperationElement ->
            val namedSetOperationObject = namedSetOperationElement.asJsonObject
            namedSetOperations += namedSetOperation {
              displayName = namedSetOperationObject.getAsJsonPrimitive("DisplayName").asString
              val setOperationId =
                namedSetOperationObject.getAsJsonPrimitive("SetOperationId").asLong
              setOperation =
                buildSetOperation(measurementConsumerReferenceId, setOperationId, setOperationsMap)
              val measurementCalculationsArr =
                namedSetOperationObject.getAsJsonArray("MeasurementCalculations")
              measurementCalculationsArr.forEach { measurementCalculationElement ->
                val measurementCalculationObject = measurementCalculationElement.asJsonObject
                measurementCalculations +=
                  MetricKt.measurementCalculation {
                    val timeIntervalObject =
                      measurementCalculationObject.getAsJsonObject("TimeInterval")
                    timeInterval = timeInterval {
                      startTime = timestamp {
                        seconds = timeIntervalObject.getAsJsonPrimitive("StartSeconds").asLong
                        nanos = timeIntervalObject.getAsJsonPrimitive("StartNanos").asInt
                      }
                      endTime = timestamp {
                        seconds = timeIntervalObject.getAsJsonPrimitive("EndSeconds").asLong
                        nanos = timeIntervalObject.getAsJsonPrimitive("EndNanos").asInt
                      }
                    }
                    val weightedMeasurementsArr =
                      measurementCalculationObject.getAsJsonArray("WeightedMeasurements")
                    weightedMeasurementsArr.forEach { weightedMeasurementElement ->
                      val weightedMeasurementObject = weightedMeasurementElement.asJsonObject
                      weightedMeasurements +=
                        MetricKt.MeasurementCalculationKt.weightedMeasurement {
                          measurementReferenceId =
                            weightedMeasurementObject
                              .getAsJsonPrimitive("MeasurementReferenceId")
                              .asString
                          coefficient =
                            weightedMeasurementObject.getAsJsonPrimitive("Coefficient").asInt
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
      type = SetOperation.Type.forNumber(setOperationObject!!.getAsJsonPrimitive("Type").asInt)
      lhs =
        MetricKt.SetOperationKt.operand {
          if (setOperationObject.get("LeftHandSetOperationId").isJsonNull) {
            if (!setOperationObject.get("LeftHandReportingSetId").isJsonNull) {
              reportingSetId =
                MetricKt.SetOperationKt.reportingSetKey {
                  this.measurementConsumerReferenceId = measurementConsumerReferenceId
                  externalReportingSetId =
                    setOperationObject.getAsJsonPrimitive("LeftHandReportingSetId").asLong
                }
            }
          } else {
            operation =
              buildSetOperation(
                measurementConsumerReferenceId,
                setOperationObject.getAsJsonPrimitive("LeftHandSetOperationId").asLong,
                setOperationMap
              )
          }
        }
      rhs =
        MetricKt.SetOperationKt.operand {
          if (setOperationObject.get("RightHandSetOperationId").isJsonNull) {
            if (!setOperationObject.get("RightHandReportingSetId").isJsonNull) {
              reportingSetId =
                MetricKt.SetOperationKt.reportingSetKey {
                  this.measurementConsumerReferenceId = measurementConsumerReferenceId
                  externalReportingSetId =
                    setOperationObject.getAsJsonPrimitive("RightHandReportingSetId").asLong
                }
            }
          } else {
            operation =
              buildSetOperation(
                measurementConsumerReferenceId,
                setOperationObject.getAsJsonPrimitive("RightHandSetOperationId").asLong,
                setOperationMap
              )
          }
        }
    }
  }
}
