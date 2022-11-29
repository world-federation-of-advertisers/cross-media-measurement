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
import com.google.gson.JsonPrimitive
import com.google.protobuf.duration
import com.google.protobuf.timestamp
import java.time.Instant
import java.util.Base64
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
import org.wfanet.measurement.internal.reporting.Measurement
import org.wfanet.measurement.internal.reporting.Metric
import org.wfanet.measurement.internal.reporting.Metric.SetOperation
import org.wfanet.measurement.internal.reporting.MetricKt
import org.wfanet.measurement.internal.reporting.MetricKt.namedSetOperation
import org.wfanet.measurement.internal.reporting.MetricKt.setOperation
import org.wfanet.measurement.internal.reporting.PeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.Report
import org.wfanet.measurement.internal.reporting.StreamReportsRequest
import org.wfanet.measurement.internal.reporting.TimeIntervals
import org.wfanet.measurement.internal.reporting.measurement
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
      ARRAY(
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
      ) AS TimeIntervals,
      ARRAY(
        SELECT
          json_build_object(
            'measurementReferenceId', MeasurementReferenceId,
            'state', Measurements.State,
            'failure', encode(Measurements.failure, 'base64'),
            'result', encode(Measurements.result, 'base64')
          )
        FROM (
          SELECT
            MeasurementConsumerReferenceId,
            MeasurementReferenceId
          FROM ReportMeasurements
          WHERE ReportMeasurements.ReportId = Reports.ReportId
        ) AS ReportMeasurements
          JOIN Measurements USING(MeasurementConsumerReferenceId, MeasurementReferenceId)
      ) AS Measurements,
      IntervalCount,
      StartSeconds,
      StartNanos,
      IncrementSeconds,
      IncrementNanos,
      (
        SELECT array_agg(
          json_build_object(
            'metricId', MetricId,
            'metricDetails', MetricDetails,
            'namedSetOperations', NamedSetOperations,
            'setOperations', SetOperations
          )
        )
        FROM (
          SELECT
            MetricId,
            encode(MetricDetails, 'base64') AS MetricDetails,
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
      LEFT JOIN PeriodicTimeIntervals USING(MeasurementConsumerReferenceId, ReportId)
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

  /**
   * Gets the report by report id.
   *
   * @throws [ReportNotFoundException]
   */
  suspend fun getReportById(
    readContext: ReadContext,
    measurementConsumerReferenceId: String,
    reportId: Long
  ): Result {
    val statement =
      boundStatement(
        (baseSql +
          """
        WHERE MeasurementConsumerReferenceId = $1
          AND ReportId = $2
          """)
      ) {
        bind("$1", measurementConsumerReferenceId)
        bind("$2", reportId)
      }

    return readContext.executeQuery(statement).consume(::translate).singleOrNull()
      ?: throw ReportNotFoundException()
  }

  /**
   * Gets the report by report idempotency key.
   *
   * @throws [ReportNotFoundException]
   */
  suspend fun getReportByIdempotencyKey(
    readContext: ReadContext,
    measurementConsumerReferenceId: String,
    reportIdempotencyKey: String
  ): Result {
    val statement =
      boundStatement(
        (baseSql +
          """
        WHERE MeasurementConsumerReferenceId = $1
          AND ReportIdempotencyKey = $2
          """)
      ) {
        bind("$1", measurementConsumerReferenceId)
        bind("$2", reportIdempotencyKey)
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
      val intervalCount: Int? = row["IntervalCount"]
      if (intervalCount != null) {
        this.periodicTimeInterval = buildPeriodicTimeInterval(row)
      } else {
        timeIntervals = buildTimeIntervals(row["TimeIntervals"])
      }
      metrics += buildMetrics(measurementConsumerReferenceId, row["Metrics"])
      measurements.putAll(buildMeasurements(measurementConsumerReferenceId, row["Measurements"]))
    }
  }

  private fun buildPeriodicTimeInterval(row: ResultRow): PeriodicTimeInterval {
    return periodicTimeInterval {
      startTime = timestamp {
        seconds = row["StartSeconds"]
        nanos = row["StartNanos"]
      }
      increment = duration {
        seconds = row["IncrementSeconds"]
        nanos = row["IncrementNanos"]
      }
      intervalCount = row["IntervalCount"]
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

  private fun buildMeasurements(
    measurementConsumerReferenceId: String,
    measurementsArr: Array<String>
  ): Map<String, Measurement> {
    return measurementsArr
      .map { JsonParser.parseString(it).asJsonObject }
      .associateBy(
        { it.getAsJsonPrimitive("measurementReferenceId").asString },
        {
          measurement {
            this.measurementConsumerReferenceId = measurementConsumerReferenceId
            measurementReferenceId = it.getAsJsonPrimitive("measurementReferenceId").asString
            state = Measurement.State.forNumber(it.getAsJsonPrimitive("state").asInt)
            if (!it.get("failure").isJsonNull) {
              failure =
                Measurement.Failure.parseFrom(
                  it.getAsJsonPrimitive("failure").decodePostgresBase64()
                )
            }
            if (!it.get("result").isJsonNull) {
              result =
                Measurement.Result.parseFrom(it.getAsJsonPrimitive("result").decodePostgresBase64())
            }
          }
        }
      )
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
          details =
            Metric.Details.parseFrom(
              metricObject.getAsJsonPrimitive("metricDetails").decodePostgresBase64()
            )

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

  /**
   * Postgres base64 encoding follows MIME encoding standards from RFC 2045 by adding \n to break up
   * the text. The MIME decoder ignores the \n.
   */
  private fun JsonPrimitive.decodePostgresBase64(): ByteArray {
    return Base64.getMimeDecoder().decode(this.asString)
  }
}
