/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.tools

import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.connection.SpannerPool
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import com.google.protobuf.Duration as ProtoDuration
import com.google.type.dateTime
import java.io.File
import java.sql.DriverManager
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExternalResource
import org.junit.rules.TemporaryFolder
import org.wfanet.measurement.common.db.liquibase.Liquibase
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.buildSpanner
import org.wfanet.measurement.gcloud.spanner.getAsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulator
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt
import org.wfanet.measurement.internal.reporting.v2.basicReport
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertBasicReport
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertMeasurementConsumer
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertNoisyReportResultValues
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertReportResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertReportResultValues
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertReportingSetResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertReportingWindowResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing.Schemata

class CleanupReportResultsTest {
  @get:Rule val temporaryFolder = TemporaryFolder()

  private val databaseName = "cleanup-test-${databaseCounter.incrementAndGet()}"
  private lateinit var client: AsyncDatabaseClient

  @Before
  fun createDatabase() = runBlocking {
    DriverManager.getConnection(
        SpannerEmulator.buildJdbcConnectionString(emulatorHost, PROJECT, INSTANCE, databaseName)
      )
      .use { connection -> Liquibase.update(connection, Schemata.REPORTING_CHANGELOG_PATH) }
    client =
      spanner.getAsyncDatabaseClient(
        com.google.cloud.spanner.DatabaseId.of(PROJECT, INSTANCE, databaseName)
      )
    client.readWriteTransaction().run { txn ->
      for (id in 1L..2L) {
        txn.insertMeasurementConsumer(
          id,
          measurementConsumer { cmmsMeasurementConsumerId = "mc-$id" },
        )
      }
    }
  }

  @After
  fun dropDatabase() {
    spanner.databaseAdminClient.dropDatabase(INSTANCE, databaseName)
  }

  @Test
  fun `main defaults to dry run and leaves every row unchanged`() {
    insertTree(10)
    val before = counts(10)

    val result = main()

    assertWithMessage(result.err).that(result.status).isEqualTo(0)
    assertThat(result.out).contains("would_delete external_report_result_id=10")
    assertThat(result.out).doesNotContain("99990")
    assertThat(result.out).contains("examined=1 deleted=0 skipped=0 dry_run=true")
    assertThat(counts(10)).isEqualTo(before)
  }

  @Test
  fun `main explicit dry run does not require paused assembly`() {
    insertTree(10)

    val result = main("--dry-run")

    assertWithMessage(result.err).that(result.status).isEqualTo(0)
    assertThat(counts(10)).containsExactly(1L, 1L, 1L, 1L, 1L).inOrder()
  }

  @Test
  fun `main leaves owned assembly attempts to automatic recovery`() {
    insertTree(10)
    insertTree(20)
    insertTree(30)
    runBlocking {
      client.readWriteTransaction().run { txn ->
        for (id in listOf(10L, 20L)) {
          txn.bufferUpdateMutation("ReportResults") {
            set("MeasurementConsumerId").to(1L)
            set("ReportResultId").to(internalReportResultId(id))
            set("AssemblyBasicReportId").to(id)
            set("AssemblyRequestId").to("00000000-0000-0000-0000-000000000000")
            set("AssemblyLeaseExpirationTime")
              .to(Instant.parse("2026-01-01T00:00:00Z").toGcloudTimestamp())
          }
        }
      }
    }

    val result = main("--dry-run=false", "--assembly-paused")

    assertWithMessage(result.err).that(result.status).isEqualTo(0)
    assertThat(result.out).contains("examined=1 deleted=1")
    assertThat(counts(10)).containsExactly(1L, 1L, 1L, 1L, 1L).inOrder()
    assertThat(counts(20)).containsExactly(1L, 1L, 1L, 1L, 1L).inOrder()
    assertThat(counts(30)).containsExactly(0L, 0L, 0L, 0L, 0L).inOrder()
  }

  @Test
  fun `main deletion removes parent and all child rows`() {
    insertTree(10)

    val result = main("--dry-run=false", "--assembly-paused")

    assertWithMessage(result.err).that(result.status).isEqualTo(0)
    assertThat(result.out).contains("examined=1 deleted=1 skipped=0 dry_run=false")
    assertThat(counts(10)).containsExactly(0L, 0L, 0L, 0L, 0L).inOrder()
  }

  @Test
  fun `main preserves every associated tree regardless of report state`() {
    for ((index, state) in
      listOf(
          BasicReport.State.UNPROCESSED_RESULTS_READY,
          BasicReport.State.SUCCEEDED,
          BasicReport.State.FAILED,
        )
        .withIndex()) {
      val id = 10L + index
      insertTree(id)
      associate(id, state)
    }
    insertTree(20)

    val result = main("--dry-run=false", "--assembly-paused")

    assertWithMessage(result.err).that(result.status).isEqualTo(0)
    assertThat(result.out).contains("examined=1 deleted=1")
    for (id in 10L..12L) {
      assertThat(counts(id)).containsExactly(1L, 1L, 1L, 1L, 1L).inOrder()
    }
    assertThat(counts(20)).containsExactly(0L, 0L, 0L, 0L, 0L).inOrder()
  }

  @Test
  fun `main preserves a tree associated after a dry run preview`() {
    insertTree(10)
    assertThat(main().out).contains("external_report_result_id=10")
    associate(10, BasicReport.State.UNPROCESSED_RESULTS_READY)

    val result = main("--dry-run=false", "--assembly-paused")

    assertWithMessage(result.err).that(result.status).isEqualTo(0)
    assertThat(result.out).contains("examined=0 deleted=0")
    assertThat(counts(10)).containsExactly(1L, 1L, 1L, 1L, 1L).inOrder()
  }

  @Test
  fun `main uses an exclusive cutoff preserving its complete timestamp`() {
    insertTree(10, createdAt = CUTOFF.minusNanos(1000))
    insertTree(20, createdAt = CUTOFF)
    insertTree(30, createdAt = CUTOFF.plusNanos(1000))

    val result = main("--dry-run=false", "--assembly-paused")

    assertWithMessage(result.err).that(result.status).isEqualTo(0)
    assertThat(counts(10).first()).isEqualTo(0L)
    assertThat(counts(20).first()).isEqualTo(1L)
    assertThat(counts(30).first()).isEqualTo(1L)
  }

  @Test
  fun `main scopes deletion to the requested consumer`() {
    insertTree(10)
    insertTree(10, consumerId = 2)

    val result = main("--dry-run=false", "--assembly-paused")

    assertWithMessage(result.err).that(result.status).isEqualTo(0)
    assertThat(counts(10).first()).isEqualTo(0L)
    assertThat(counts(10, consumerId = 2).first()).isEqualTo(1L)
  }

  @Test
  fun `main paginates dry run without repeating rows sharing timestamps`() {
    for (id in 201L downTo 1L) {
      insertTree(id, children = false, createdAt = CUTOFF.minusSeconds(if (id <= 50) 2 else 1))
    }

    val result = main("--limit=150")

    assertWithMessage(result.err).that(result.status).isEqualTo(0)
    val ids =
      Regex("would_delete external_report_result_id=(\\d+)")
        .findAll(result.out)
        .map { it.groupValues[1].toLong() }
        .toList()
    assertThat(ids).containsExactlyElementsIn(1L..150L).inOrder()
    assertThat(result.out).contains("examined=150 deleted=0")
    assertThat(result.out).contains("limit_reached=true")
    assertThat(parentCount()).isEqualTo(201L)
  }

  @Test
  fun `main limits deletion and repeated runs reclaim the remainder`() {
    for (id in listOf(30L, 20L, 10L)) {
      insertTree(id, children = false)
    }
    val args = arrayOf("--dry-run=false", "--assembly-paused", "--limit=2")

    assertThat(main(*args).out).contains("examined=2 deleted=2")
    assertThat(counts(30).first()).isEqualTo(1L)
    assertThat(parentCount()).isEqualTo(1L)
    assertThat(main(*args).out).contains("examined=1 deleted=1")
    assertThat(main(*args).out).contains("examined=0 deleted=0")
    assertThat(parentCount()).isEqualTo(0L)
  }

  @Test
  fun `main relative age excludes recent unassociated attempts`() {
    insertTree(10)
    insertTree(20, createdAt = Instant.now().minusSeconds(60))

    val result = main("--older-than=7d", "--dry-run=false", "--assembly-paused")

    assertWithMessage(result.err).that(result.status).isEqualTo(0)
    assertThat(counts(10).first()).isEqualTo(0L)
    assertThat(counts(20).first()).isEqualTo(1L)
  }

  @Test
  fun `main rejects deletion without confirming assembly is paused`() {
    insertTree(10)

    val result = main("--dry-run=false")

    assertThat(result.status).isEqualTo(2)
    assertThat(result.err).contains("--assembly-paused")
    assertThat(counts(10).first()).isEqualTo(1L)
  }

  @Test
  fun `main rejects invalid cutoffs and limits before making changes`() {
    insertTree(10)
    for (arg in
      listOf(
        "--created-before=invalid",
        "--created-before=9999-01-01T00:00:00Z",
        "--older-than=0s",
        "--older-than=-1d",
        "--older-than=garbage1d",
        "--older-than=1d-2h",
        "--limit=0",
        "--limit=-1",
      )) {
      val result = main(arg)
      assertWithMessage("$arg: ${result.err}").that(result.status).isEqualTo(2)
      assertThat(counts(10).first()).isEqualTo(1L)
    }
  }

  @Test
  fun `main requires exactly one cutoff and a consumer scope`() {
    for (args in
      listOf(
        arrayOf("--cmms-measurement-consumer-id=mc-1"),
        arrayOf("--created-before=$CUTOFF"),
        arrayOf(
          "--cmms-measurement-consumer-id=mc-1",
          "--created-before=$CUTOFF",
          "--older-than=7d",
        ),
      )) {
      assertThat(invokeMain(args.toList()).status).isEqualTo(2)
    }
  }

  @Test
  fun `main reports an unknown consumer instead of claiming cleanup succeeded`() {
    insertTree(10)

    val result = main("--cmms-measurement-consumer-id=unknown")

    assertThat(result.status).isNotEqualTo(0)
    assertThat(counts(10).first()).isEqualTo(1L)
  }

  @Test
  fun `main help does not require a database connection`() {
    val result = invokeMain(listOf("--help"), withDatabase = false)

    assertWithMessage(result.err).that(result.status).isEqualTo(0)
    assertThat(result.out).contains("--dry-run")
    assertThat(result.out).contains("--created-before")
  }

  private fun insertTree(
    id: Long,
    consumerId: Long = 1,
    createdAt: Instant = CUTOFF.minusSeconds(1),
    children: Boolean = true,
  ) = runBlocking {
    client.readWriteTransaction().run { txn ->
      txn.insertReportResult(
        consumerId,
        internalReportResultId(id),
        id,
        dateTime {
          year = 2020
          month = 1
          day = 1
          utcOffset = ProtoDuration.getDefaultInstance()
        },
        assembly = null,
      )
      if (children) {
        txn.insertReportingSetResult(
          consumerId,
          internalReportResultId(id),
          1,
          1,
          ReportingSetResultKt.dimension {
            externalReportingSetId = "rs-1"
            vennDiagramRegionType = ReportingSetResult.Dimension.VennDiagramRegionType.PRIMITIVE
            metricFrequencySpec = metricFrequencySpec { total = true }
            custom = true
          },
          null,
          1,
          1,
          null,
          1,
        )
        txn.insertReportingWindowResult(
          consumerId,
          internalReportResultId(id),
          1,
          1,
          null,
          java.time.LocalDate.of(2020, 1, 1),
        )
        txn.insertNoisyReportResultValues(
          consumerId,
          internalReportResultId(id),
          1,
          1,
          ReportingSetResult.ReportingWindowResult.NoisyReportResultValues.getDefaultInstance(),
        )
        txn.insertReportResultValues(
          consumerId,
          internalReportResultId(id),
          1,
          1,
          ReportingSetResult.ReportingWindowResult.ReportResultValues.getDefaultInstance(),
        )
      }
    }
    client.readWriteTransaction().run { txn ->
      txn.bufferUpdateMutation("ReportResults") {
        set("MeasurementConsumerId").to(consumerId)
        set("ReportResultId").to(internalReportResultId(id))
        set("CreateTime").to(createdAt.toGcloudTimestamp())
      }
    }
  }

  private fun associate(id: Long, state: BasicReport.State) = runBlocking {
    client.readWriteTransaction().run { txn ->
      txn.insertBasicReport(
        id,
        1,
        basicReport {
          externalBasicReportId = "br-$id"
          externalCampaignGroupId = "cg"
        },
        state,
        null,
      )
    }
    client.readWriteTransaction().run { txn ->
      txn.bufferUpdateMutation("BasicReports") {
        set("MeasurementConsumerId").to(1L)
        set("BasicReportId").to(id)
        set("ReportResultId").to(internalReportResultId(id))
      }
    }
  }

  private fun internalReportResultId(externalId: Long): Long = 100000L - externalId

  private fun counts(id: Long, consumerId: Long = 1): List<Long> = runBlocking {
    TABLES.map { table ->
      client
        .singleUse()
        .executeQuery(
          statement(
            "SELECT COUNT(*) AS N FROM $table WHERE MeasurementConsumerId = @mc AND ReportResultId = @id"
          ) {
            bind("mc").to(consumerId)
            bind("id").to(internalReportResultId(id))
          }
        )
        .single()
        .getLong("N")
    }
  }

  private fun parentCount(): Long = runBlocking {
    client
      .singleUse()
      .executeQuery(statement("SELECT COUNT(*) AS N FROM ReportResults"))
      .single()
      .getLong("N")
  }

  private fun main(vararg options: String): Result {
    val cutoffArgs =
      if (options.any { it.startsWith("--created-before") || it.startsWith("--older-than") }) {
        emptyList()
      } else {
        listOf("--created-before=$CUTOFF")
      }
    val scopeArgs =
      if (options.any { it.startsWith("--cmms-measurement-consumer-id") }) {
        emptyList()
      } else {
        listOf("--cmms-measurement-consumer-id=mc-1")
      }
    return invokeMain(scopeArgs + cutoffArgs + options)
  }

  private fun invokeMain(options: List<String>, withDatabase: Boolean = true): Result {
    val out = temporaryFolder.newFile()
    val err = temporaryFolder.newFile()
    val databaseArgs =
      if (withDatabase)
        listOf(
          "--spanner-project=$PROJECT",
          "--spanner-instance=$INSTANCE",
          "--spanner-database=$databaseName",
          "--spanner-emulator-host=$emulatorHost",
        )
      else emptyList()
    val process =
      ProcessBuilder(
          File(System.getProperty("java.home"), "bin/java").path,
          "-cp",
          System.getProperty("java.class.path"),
          CleanupReportResults::class.java.name,
          *(databaseArgs + options).toTypedArray(),
        )
        .redirectOutput(out)
        .redirectError(err)
        .start()
    check(process.waitFor(60, TimeUnit.SECONDS)) {
      process.destroyForcibly().waitFor()
      "Cleanup CLI did not finish within 60 seconds"
    }
    return Result(process.exitValue(), out.readText(), err.readText())
  }

  private data class Result(val status: Int, val out: String, val err: String)

  companion object {
    private const val PROJECT = "cleanup-project"
    private const val INSTANCE = "cleanup-instance"
    private val CUTOFF = Instant.parse("2020-02-01T00:00:00.123456Z")
    private val TABLES =
      listOf(
        "ReportResults",
        "ReportingSetResults",
        "ReportingWindowResults",
        "NoisyReportResultValues",
        "ReportResultValues",
      )
    private val databaseCounter = AtomicInteger()
    private val emulator = SpannerEmulator()
    private lateinit var emulatorHost: String
    private lateinit var spanner: Spanner

    @get:JvmStatic
    @get:ClassRule
    val emulatorRule =
      object : ExternalResource() {
        override fun before() {
          emulatorHost = emulator.start()
          spanner = buildSpanner(PROJECT, emulatorHost)
        }

        override fun after() {
          spanner.close()
          SpannerPool.closeSpannerPool()
          emulator.close()
        }
      }
  }
}
