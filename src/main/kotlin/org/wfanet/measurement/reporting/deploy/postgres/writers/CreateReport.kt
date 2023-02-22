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

package org.wfanet.measurement.reporting.deploy.postgres.writers

import com.google.protobuf.util.Timestamps
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.reporting.CreateReportRequest
import org.wfanet.measurement.internal.reporting.CreateReportRequest.MeasurementKey
import org.wfanet.measurement.internal.reporting.Measurement
import org.wfanet.measurement.internal.reporting.Metric
import org.wfanet.measurement.internal.reporting.Metric.MeasurementCalculation.WeightedMeasurement
import org.wfanet.measurement.internal.reporting.Metric.NamedSetOperation
import org.wfanet.measurement.internal.reporting.Metric.SetOperation
import org.wfanet.measurement.internal.reporting.PeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.Report
import org.wfanet.measurement.internal.reporting.TimeInterval
import org.wfanet.measurement.internal.reporting.copy
import org.wfanet.measurement.internal.reporting.timeInterval
import org.wfanet.measurement.reporting.deploy.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.service.internal.MeasurementCalculationTimeIntervalNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

/**
 * Inserts a Report into the database.
 *
 * Throws the following on [execute]:
 * * [ReportingSetNotFoundException] ReportingSet not found
 * * [MeasurementCalculationTimeIntervalNotFoundException] MeasurementCalculation TimeInterval not
 *   found.
 */
class CreateReport(private val request: CreateReportRequest) : PostgresWriter<Report>() {
  override suspend fun TransactionScope.runTransaction(): Report {
    val report = request.report
    val internalReportId = idGenerator.generateInternalId().value
    val externalReportId = idGenerator.generateExternalId().value

    val timeIntervals: List<TimeInterval>
    val isPeriodic = report.timeCase == Report.TimeCase.PERIODIC_TIME_INTERVAL
    if (isPeriodic) {
      timeIntervals = ArrayList(report.periodicTimeInterval.intervalCount)
      timeIntervals.add(
        timeInterval {
          startTime = report.periodicTimeInterval.startTime
          endTime = Timestamps.add(startTime, report.periodicTimeInterval.increment)
        }
      )
      for (i in 1 until report.periodicTimeInterval.intervalCount) {
        timeIntervals.add(
          timeInterval {
            startTime = timeIntervals[i - 1].endTime
            endTime = Timestamps.add(startTime, report.periodicTimeInterval.increment)
          }
        )
      }
    } else {
      timeIntervals = report.timeIntervals.timeIntervalsList
    }

    val statement =
      boundStatement(
        """
      INSERT INTO Reports (MeasurementConsumerReferenceId, ReportId, ExternalReportId, State, ReportDetails, ReportIdempotencyKey, CreateTime)
        VALUES ($1, $2, $3, $4, $5, $6, now() at time zone 'utc')
      """
      ) {
        bind("$1", report.measurementConsumerReferenceId)
        bind("$2", internalReportId)
        bind("$3", externalReportId)
        bind("$4", Report.State.RUNNING_VALUE)
        bind("$5", report.details)
        bind("$6", report.reportIdempotencyKey)
      }

    transactionContext.run {
      executeStatement(statement)
      if (isPeriodic) {
        insertPeriodicTimeInterval(
          report.measurementConsumerReferenceId,
          internalReportId,
          report.periodicTimeInterval
        )
      }
      insertMeasurements(request.measurementsList)
      val timeIntervalMap =
        insertTimeIntervals(report.measurementConsumerReferenceId, internalReportId, timeIntervals)

      report.metricsList.forEach {
        insertMetric(report.measurementConsumerReferenceId, internalReportId, timeIntervalMap, it)
      }
      insertReportMeasurements(request.measurementsList, internalReportId)
    }

    return report.copy {
      this.externalReportId = externalReportId
      state = Report.State.RUNNING
    }
  }

  private suspend fun TransactionScope.insertMeasurements(
    measurements: Collection<MeasurementKey>
  ) {
    val sql =
      StringBuilder(
        """
            INSERT INTO Measurements (MeasurementConsumerReferenceId, MeasurementReferenceId, State)
              VALUES ($1, $2, $3)
            """
      )
    val numParameters = 3
    var firstParam = 1
    for (i in 1 until measurements.size) {
      firstParam += numParameters
      sql.append(",($$firstParam, $${firstParam + 1}, $${firstParam + 2})")
    }
    sql.append("ON CONFLICT DO NOTHING")

    firstParam = 1
    val statement =
      boundStatement(sql.toString()) {
        measurements.forEach {
          bind("$$firstParam", it.measurementConsumerReferenceId)
          bind("$${firstParam + 1}", it.measurementReferenceId)
          bind("$${firstParam + 2}", Measurement.State.PENDING_VALUE)
          firstParam += numParameters
        }
      }
    transactionContext.executeStatement(statement)
  }

  private suspend fun TransactionScope.insertReportMeasurements(
    measurements: Collection<MeasurementKey>,
    reportId: Long
  ) {
    val sql =
      StringBuilder(
        """
      INSERT INTO ReportMeasurements (MeasurementConsumerReferenceId, MeasurementReferenceId, ReportId)
        VALUES ($1, $2, $3)
      """
      )
    val numParameters = 3
    var firstParam = 1
    for (i in 1 until measurements.size) {
      firstParam += numParameters
      sql.append(",($$firstParam, $${firstParam + 1}, $${firstParam + 2})")
    }

    firstParam = 1
    val statement =
      boundStatement(sql.toString()) {
        measurements.forEach {
          bind("$$firstParam", it.measurementConsumerReferenceId)
          bind("$${firstParam + 1}", it.measurementReferenceId)
          bind("$${firstParam + 2}", reportId)
          firstParam += numParameters
        }
      }
    transactionContext.executeStatement(statement)
  }

  private suspend fun TransactionScope.insertPeriodicTimeInterval(
    measurementConsumerReferenceId: String,
    reportId: Long,
    periodicTimeInterval: PeriodicTimeInterval
  ) {
    val statement =
      boundStatement(
        """
      INSERT INTO PeriodicTimeIntervals (MeasurementConsumerReferenceId, ReportId, StartSeconds, StartNanos, IncrementSeconds, IncrementNanos, IntervalCount)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
      """
      ) {
        bind("$1", measurementConsumerReferenceId)
        bind("$2", reportId)
        bind("$3", periodicTimeInterval.startTime.seconds)
        bind("$4", periodicTimeInterval.startTime.nanos)
        bind("$5", periodicTimeInterval.increment.seconds)
        bind("$6", periodicTimeInterval.increment.nanos)
        bind("$7", periodicTimeInterval.intervalCount)
      }

    transactionContext.executeStatement(statement)
  }

  private suspend fun TransactionScope.insertTimeIntervals(
    measurementConsumerReferenceId: String,
    reportId: Long,
    timeIntervals: Collection<TimeInterval>
  ): Map<TimeInterval, Long> {
    val sql =
      StringBuilder(
        """
      INSERT INTO TimeIntervals (MeasurementConsumerReferenceId, ReportId, TimeIntervalId, StartSeconds, StartNanos, EndSeconds, EndNanos)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
      """
      )
    val numParameters = 7
    var firstParam = 1
    for (i in 1 until timeIntervals.size) {
      firstParam += numParameters
      sql.append(
        """,($$firstParam, $${firstParam + 1}, $${firstParam + 2}, $${firstParam + 3},
        $${firstParam + 4}, $${firstParam + 5}, $${firstParam + 6})
        """
      )
    }

    val timeIntervalMap = mutableMapOf<TimeInterval, Long>()
    firstParam = 1
    val statement =
      boundStatement(sql.toString()) {
        timeIntervals.forEach {
          val timeIntervalId = idGenerator.generateInternalId().value
          timeIntervalMap[it] = timeIntervalId
          bind("$$firstParam", measurementConsumerReferenceId)
          bind("$${firstParam + 1}", reportId)
          bind("$${firstParam + 2}", timeIntervalId)
          bind("$${firstParam + 3}", it.startTime.seconds)
          bind("$${firstParam + 4}", it.startTime.nanos)
          bind("$${firstParam + 5}", it.endTime.seconds)
          bind("$${firstParam + 6}", it.endTime.nanos)
          firstParam += numParameters
        }
      }
    transactionContext.executeStatement(statement)
    return timeIntervalMap
  }

  private suspend fun TransactionScope.insertMetric(
    measurementConsumerReferenceId: String,
    reportId: Long,
    timeIntervalMap: Map<TimeInterval, Long>,
    metric: Metric
  ) {
    val metricId = idGenerator.generateInternalId().value

    val statement =
      boundStatement(
        """
          INSERT INTO Metrics (MeasurementConsumerReferenceId, ReportId, MetricId, MetricDetails)
          VALUES ($1, $2, $3, $4)
          """
      ) {
        bind("$1", measurementConsumerReferenceId)
        bind("$2", reportId)
        bind("$3", metricId)
        bind("$4", metric.details)
      }

    transactionContext.executeStatement(statement)

    metric.namedSetOperationsList.forEach {
      insertNamedSetOperation(
        measurementConsumerReferenceId,
        reportId,
        metricId,
        timeIntervalMap,
        it
      )
    }
  }

  private suspend fun TransactionScope.insertNamedSetOperation(
    measurementConsumerReferenceId: String,
    reportId: Long,
    metricId: Long,
    timeIntervalMap: Map<TimeInterval, Long>,
    namedSetOperation: NamedSetOperation
  ) {
    val namedSetOperationId = idGenerator.generateInternalId().value
    val setOperationId =
      insertSetOperation(
        measurementConsumerReferenceId,
        reportId,
        metricId,
        namedSetOperation.setOperation
      )

    val statement =
      boundStatement(
        """
        INSERT INTO NamedSetOperations(MeasurementConsumerReferenceId, ReportId, MetricId, NamedSetOperationId, DisplayName, SetOperationId)
        VALUES ($1, $2, $3, $4, $5, $6)
        """
      ) {
        bind("$1", measurementConsumerReferenceId)
        bind("$2", reportId)
        bind("$3", metricId)
        bind("$4", namedSetOperationId)
        bind("$5", namedSetOperation.displayName)
        bind("$6", setOperationId)
      }

    transactionContext.executeStatement(statement)

    namedSetOperation.measurementCalculationsList.forEach {
      insertMeasurementCalculations(
        measurementConsumerReferenceId,
        reportId,
        metricId,
        namedSetOperationId,
        timeIntervalMap[it.timeInterval]
          ?: throw MeasurementCalculationTimeIntervalNotFoundException(),
        it
      )
    }
  }

  private suspend fun TransactionScope.insertMeasurementCalculations(
    measurementConsumerReferenceId: String,
    reportId: Long,
    metricId: Long,
    namedSetOperationId: Long,
    timeIntervalId: Long,
    measurementCalculation: Metric.MeasurementCalculation
  ) {
    val measurementCalculationId = idGenerator.generateInternalId().value
    val statement =
      boundStatement(
        """
        INSERT INTO MeasurementCalculations(MeasurementConsumerReferenceId, ReportId, MetricId, NamedSetOperationId, MeasurementCalculationId, TimeIntervalId)
        VALUES ($1, $2, $3, $4, $5, $6)
        """
      ) {
        bind("$1", measurementConsumerReferenceId)
        bind("$2", reportId)
        bind("$3", metricId)
        bind("$4", namedSetOperationId)
        bind("$5", measurementCalculationId)
        bind("$6", timeIntervalId)
      }

    transactionContext.executeStatement(statement)
    insertWeightedMeasurements(
      measurementConsumerReferenceId,
      reportId,
      metricId,
      namedSetOperationId,
      measurementCalculationId,
      measurementCalculation.weightedMeasurementsList
    )
  }

  private suspend fun TransactionScope.insertWeightedMeasurements(
    measurementConsumerReferenceId: String,
    reportId: Long,
    metricId: Long,
    namedSetOperationId: Long,
    measurementCalculationId: Long,
    weightedMeasurements: Collection<WeightedMeasurement>
  ) {
    transactionContext.run {
      weightedMeasurements.forEach {
        val weightedMeasurementId = idGenerator.generateInternalId().value
        val statement =
          boundStatement(
            """
        INSERT INTO WeightedMeasurements(MeasurementConsumerReferenceId, ReportId, MetricId, NamedSetOperationId, MeasurementCalculationId, WeightedMeasurementId, MeasurementReferenceId, Coefficient)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """
          ) {
            bind("$1", measurementConsumerReferenceId)
            bind("$2", reportId)
            bind("$3", metricId)
            bind("$4", namedSetOperationId)
            bind("$5", measurementCalculationId)
            bind("$6", weightedMeasurementId)
            bind("$7", it.measurementReferenceId)
            bind("$8", it.coefficient)
          }
        executeStatement(statement)
      }
    }
  }

  private suspend fun TransactionScope.insertSetOperation(
    measurementConsumerReferenceId: String,
    reportId: Long,
    metricId: Long,
    setOperation: SetOperation
  ): Long {
    val setOperationId = idGenerator.generateInternalId().value
    val lhsReportingSetId: Long?
    val rhsReportingSetId: Long?
    val lhsSetOperationId: Long?
    val rhsSetOperationId: Long?

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (setOperation.lhs.operandCase) {
      SetOperation.Operand.OperandCase.REPORTINGSETID -> {
        lhsSetOperationId = null
        val reportingSetResult =
          ReportingSetReader()
            .readReportingSetByExternalId(
              transactionContext,
              measurementConsumerReferenceId,
              ExternalId(setOperation.lhs.reportingSetId.externalReportingSetId)
            )
        lhsReportingSetId = reportingSetResult.reportingSetId.value
      }
      SetOperation.Operand.OperandCase.OPERATION -> {
        lhsSetOperationId =
          insertSetOperation(
            measurementConsumerReferenceId,
            reportId,
            metricId,
            setOperation.lhs.operation
          )
        lhsReportingSetId = null
      }
      SetOperation.Operand.OperandCase.OPERAND_NOT_SET -> {
        lhsSetOperationId = null
        lhsReportingSetId = null
      }
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (setOperation.rhs.operandCase) {
      SetOperation.Operand.OperandCase.REPORTINGSETID -> {
        rhsSetOperationId = null
        val reportingSetResult =
          ReportingSetReader()
            .readReportingSetByExternalId(
              transactionContext,
              measurementConsumerReferenceId,
              ExternalId(setOperation.rhs.reportingSetId.externalReportingSetId)
            )
        rhsReportingSetId = reportingSetResult.reportingSetId.value
      }
      SetOperation.Operand.OperandCase.OPERATION -> {
        rhsSetOperationId =
          insertSetOperation(
            measurementConsumerReferenceId,
            reportId,
            metricId,
            setOperation.rhs.operation
          )
        rhsReportingSetId = null
      }
      SetOperation.Operand.OperandCase.OPERAND_NOT_SET -> {
        rhsSetOperationId = null
        rhsReportingSetId = null
      }
    }

    val statement =
      boundStatement(
        """
        INSERT INTO SetOperations(MeasurementConsumerReferenceId, ReportId, MetricId, SetOperationId, Type, LeftHandSetOperationId, LeftHandReportingSetId, RightHandSetOperationId, RightHandReportingSetId)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """
      ) {
        bind("$1", measurementConsumerReferenceId)
        bind("$2", reportId)
        bind("$3", metricId)
        bind("$4", setOperationId)
        bind("$5", setOperation.typeValue)
        bind("$6", lhsSetOperationId)
        bind("$7", lhsReportingSetId)
        bind("$8", rhsSetOperationId)
        bind("$9", rhsReportingSetId)
      }

    transactionContext.executeStatement(statement)

    return setOperationId
  }
}
