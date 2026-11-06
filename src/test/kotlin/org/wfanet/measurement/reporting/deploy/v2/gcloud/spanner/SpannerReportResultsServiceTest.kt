/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner

import com.google.cloud.spanner.Options
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.BindableService
import io.grpc.Status
import io.grpc.StatusException
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertFailsWith
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.spy
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.TransactionWork
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.reporting.v2.AddProcessedResultValuesRequest
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultView
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.getBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.listReportingSetResultsRequest
import org.wfanet.measurement.internal.reporting.v2.setExternalReportIdRequest
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMeasurementConsumersService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportingSetsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata as PostgresSchemata
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping
import org.wfanet.measurement.reporting.service.internal.testing.v2.ReportResultsServiceTest
import org.wfanet.measurement.reporting.service.internal.testing.v2.ReportResultsServiceTest.Companion.BATCH_CREATE_REPORTING_SET_RESULTS_REQUEST
import org.wfanet.measurement.reporting.service.internal.testing.v2.ReportResultsServiceTest.Companion.CREATE_REPORT_RESULT_REQUEST

class SpannerReportResultsServiceTest : ReportResultsServiceTest() {
  private var duplicateGeneratedIds = false
  private val afterNextProcessedBatch = AtomicReference<(() -> Unit)?>(null)

  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.REPORTING_CHANGELOG_PATH)

  override fun createServices(
    idGenerator: IdGenerator,
    impressionQualificationFilterMapping: ImpressionQualificationFilterMapping,
  ): List<BindableService> {
    val spannerClient = spy(spannerDatabase.databaseClient)
    doAnswer { invocation ->
        val runner = invocation.callRealMethod() as AsyncDatabaseClient.TransactionRunner
        val callback = afterNextProcessedBatch.getAndSet(null)
        if (callback == null) runner
        else
          object : AsyncDatabaseClient.TransactionRunner by runner {
            override suspend fun <R> run(doWork: TransactionWork<R>): R {
              val result = runner.run(doWork)
              callback()
              return result
            }
          }
      }
      .whenever(spannerClient)
      .readWriteTransaction(any<Options.TransactionOption>())
    val postgresClient = postgresDatabaseProvider.createDatabase()
    val integerIdGenerator = org.wfanet.measurement.common.identity.RandomIdGenerator()
    val reportResultsIdGenerator =
      if (duplicateGeneratedIds) {
        object : IdGenerator {
          private var repeatedId: Long? = null

          override fun generateId(): Long {
            repeatedId?.let {
              repeatedId = null
              return it
            }
            return idGenerator.generateId().also { repeatedId = it }
          }
        }
      } else {
        idGenerator
      }

    return listOf(
      SpannerReportResultsService(
        spannerClient,
        impressionQualificationFilterMapping,
        idGenerator = reportResultsIdGenerator,
        processedResultBatchSize = 2,
      ),
      SpannerBasicReportsService(
        spannerClient,
        postgresClient,
        impressionQualificationFilterMapping,
        idGenerator = idGenerator,
      ),
      PostgresReportingSetsService(integerIdGenerator, postgresClient),
      PostgresMeasurementConsumersService(integerIdGenerator, postgresClient),
    )
  }

  @Test
  fun `batch creation retries window ID collisions within the same transaction`() {
    duplicateGeneratedIds = true
    super.`batchCreateReportingSetResults creates ReportingSetResults`()
  }

  @Test
  fun `concurrent different correction requests cannot mix published batches`() {
    val fixture = createCorrectionFixture()
    val conflicting =
      fixture.request.copy {
        for ((id, results) in fixture.request.reportingSetResultsMap) {
          reportingSetResults[id] =
            results.copy {
              reportingWindowResults.clear()
              reportingWindowResults +=
                results.reportingWindowResultsList.map { entry ->
                  entry.copy {
                    value =
                      entry.value.copy {
                        nonCumulativeResults = nonCumulativeResults.copy { impressions += 100 }
                      }
                  }
                }
            }
        }
      }
    val firstCommitted = CountDownLatch(1)
    val secondCommittedOrRejected = CountDownLatch(1)
    val resumeFirst = CountDownLatch(1)
    val resumeSecond = CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(2)
    afterNextProcessedBatch.set {
      firstCommitted.countDown()
      check(resumeFirst.await(30, TimeUnit.SECONDS))
    }
    try {
      val first =
        executor.submit(Callable { reportResultsStub.addProcessedResultValues(fixture.request) })
      check(firstCommitted.await(30, TimeUnit.SECONDS))
      assertThat(getBasicReport(fixture).state)
        .isEqualTo(BasicReport.State.UNPROCESSED_RESULTS_READY)
      afterNextProcessedBatch.set {
        secondCommittedOrRejected.countDown()
        check(resumeSecond.await(30, TimeUnit.SECONDS))
      }
      val second =
        executor.submit(
          Callable {
            try {
              reportResultsStub.addProcessedResultValues(conflicting)
            } finally {
              secondCommittedOrRejected.countDown()
            }
          }
        )
      check(secondCommittedOrRejected.await(30, TimeUnit.SECONDS))
      resumeFirst.countDown()
      first.get(30, TimeUnit.SECONDS)
      resumeSecond.countDown()
      val error = assertFailsWith<ExecutionException> { second.get(30, TimeUnit.SECONDS) }
      assertThat((error.cause as StatusException).status.code)
        .isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(getBasicReport(fixture).state).isEqualTo(BasicReport.State.SUCCEEDED)
      assertCorrectedValues(fixture)
    } finally {
      resumeFirst.countDown()
      resumeSecond.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `interrupted correction resumes identical reordered request with associated BasicReport`() {
    val fixture = createCorrectionFixture()
    afterNextProcessedBatch.set { throw Status.UNAVAILABLE.asRuntimeException() }
    val interruption =
      assertFailsWith<StatusException> {
        reportResultsStub.addProcessedResultValues(fixture.request)
      }
    assertThat(interruption.status.code).isEqualTo(Status.Code.UNAVAILABLE)
    assertThat(getBasicReport(fixture).state).isEqualTo(BasicReport.State.UNPROCESSED_RESULTS_READY)
    val full =
      reportResultsStub.listReportingSetResults(
        listReportingSetResultsRequest {
          cmmsMeasurementConsumerId = fixture.request.cmmsMeasurementConsumerId
          externalReportResultId = fixture.request.externalReportResultId
          view = ReportingSetResultView.REPORTING_SET_RESULT_VIEW_FULL
        }
      )
    assertThat(
        full.reportingSetResultsList
          .flatMap { it.reportingWindowResultsList }
          .count { it.value.hasProcessedReportResultValues() }
      )
      .isEqualTo(2)
    val fullById = full.reportingSetResultsList.associateBy { it.externalReportingSetResultId }
    val (unwrittenResultId, unwrittenWindow) =
      fixture.request.reportingSetResultsMap
        .flatMap { (id, requested) ->
          val storedByWindow =
            fullById.getValue(id).reportingWindowResultsList.associateBy { it.key }
          requested.reportingWindowResultsList
            .filter { !storedByWindow.getValue(it.key).value.hasProcessedReportResultValues() }
            .map { id to it.key }
        }
        .single()
    val originalValues = fixture.request.reportingSetResultsMap.getValue(unwrittenResultId)
    val conflicting =
      fixture.request.copy {
        reportingSetResults[unwrittenResultId] =
          originalValues.copy {
            reportingWindowResults.clear()
            reportingWindowResults +=
              originalValues.reportingWindowResultsList.map { entry ->
                if (entry.key == unwrittenWindow)
                  entry.copy {
                    value =
                      entry.value.copy {
                        nonCumulativeResults = nonCumulativeResults.copy { impressions += 100 }
                      }
                  }
                else entry
              }
          }
      }
    val conflict =
      assertFailsWith<StatusException> { reportResultsStub.addProcessedResultValues(conflicting) }
    assertThat(conflict.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    val reordered =
      fixture.request.copy {
        reportingSetResults.clear()
        for ((id, results) in fixture.request.reportingSetResultsMap.entries.reversed()) {
          reportingSetResults[id] =
            results.copy {
              reportingWindowResults.clear()
              reportingWindowResults += results.reportingWindowResultsList.reversed()
            }
        }
      }
    reportResultsStub.addProcessedResultValues(reordered)
    assertThat(getBasicReport(fixture).state).isEqualTo(BasicReport.State.SUCCEEDED)
    assertCorrectedValues(fixture)
  }

  private data class CorrectionFixture(
    val basicReport: BasicReport,
    val request: AddProcessedResultValuesRequest,
  )

  private fun createCorrectionFixture(): CorrectionFixture {
    val basicReport = ensureMeasurementConsumer()
    basicReportsStub.setExternalReportId(
      setExternalReportIdRequest {
        cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
        externalBasicReportId = basicReport.externalBasicReportId
        externalReportId = "report-correction"
      }
    )
    val reportResult = reportResultsStub.createReportResult(CREATE_REPORT_RESULT_REQUEST)
    val results =
      reportResultsStub
        .batchCreateReportingSetResults(
          BATCH_CREATE_REPORTING_SET_RESULTS_REQUEST.withExternalReportResultId(
              reportResult.externalReportResultId
            )
            .copy { externalBasicReportId = basicReport.externalBasicReportId }
        )
        .reportingSetResultsList
    return CorrectionFixture(basicReport, buildAddProcessedResultValuesRequest(results))
  }

  private fun getBasicReport(fixture: CorrectionFixture): BasicReport =
    basicReportsStub.getBasicReport(
      getBasicReportRequest {
        cmmsMeasurementConsumerId = fixture.basicReport.cmmsMeasurementConsumerId
        externalBasicReportId = fixture.basicReport.externalBasicReportId
      }
    )

  private fun assertCorrectedValues(fixture: CorrectionFixture) {
    val full =
      reportResultsStub
        .listReportingSetResults(
          listReportingSetResultsRequest {
            cmmsMeasurementConsumerId = fixture.request.cmmsMeasurementConsumerId
            externalReportResultId = fixture.request.externalReportResultId
            view = ReportingSetResultView.REPORTING_SET_RESULT_VIEW_FULL
          }
        )
        .reportingSetResultsList
        .associateBy { it.externalReportingSetResultId }
    for ((id, expected) in fixture.request.reportingSetResultsMap) {
      val windows = full.getValue(id).reportingWindowResultsList.associateBy { it.key }
      for (entry in expected.reportingWindowResultsList) {
        assertThat(windows.getValue(entry.key).value.processedReportResultValues)
          .isEqualTo(entry.value)
      }
    }
  }

  companion object {
    @get:JvmStatic @get:ClassRule val spannerEmulator = SpannerEmulatorRule()

    @get:JvmStatic
    @get:ClassRule
    val postgresDatabaseProvider =
      PostgresDatabaseProviderRule(PostgresSchemata.REPORTING_CHANGELOG_PATH)
  }
}
