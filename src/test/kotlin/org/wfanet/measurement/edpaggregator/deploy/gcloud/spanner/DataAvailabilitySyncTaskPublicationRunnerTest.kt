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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.type.date
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.Collections
import java.util.logging.Handler
import java.util.logging.LogRecord
import java.util.logging.Logger
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncTaskAttempts
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncTaskPublisher
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.claimDataAvailabilitySyncTaskPublication
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertDataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTaskFailureCategory
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTaskState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.createDataAvailabilitySyncTaskRequest
import org.wfanet.measurement.internal.edpaggregator.dataAvailabilitySyncTask
import org.wfanet.measurement.internal.edpaggregator.getDataAvailabilitySyncTaskRequest

@RunWith(JUnit4::class)
class DataAvailabilitySyncTaskPublicationRunnerTest {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  @Test
  fun `only one task per data provider is published`() =
    runBlocking<Unit> {
      insertUpload()
      val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
      repeat(5) { index -> service.createDataAvailabilitySyncTask(createRequest(index = index)) }
      val publisher = RecordingPublisher()
      val runner = newRunner(publisher)

      assertThat(runner.publishPendingTasks()).isEqualTo(1)

      assertThat(publisher.taskNames).hasSize(1)
      assertThat(providerSlotCount(DATA_PROVIDER_ID)).isEqualTo(1)
      assertThat(unpublishedCount(DATA_PROVIDER_ID)).isEqualTo(4)
    }

  @Test
  fun `tasks for different data providers are published concurrently`() =
    runBlocking<Unit> {
      insertUpload()
      insertUpload(
        OTHER_DATA_PROVIDER_ID,
        rawImpressionUploadId = 2L,
        rawImpressionUploadResourceId = OTHER_UPLOAD_ID,
      )
      val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
      repeat(2) { index -> service.createDataAvailabilitySyncTask(createRequest(index = index)) }
      service.createDataAvailabilitySyncTask(
        createRequest(
          dataProviderResourceId = OTHER_DATA_PROVIDER_ID,
          rawImpressionUploadId = 2L,
          rawImpressionUploadResourceId = OTHER_UPLOAD_ID,
        )
      )
      val publisher = RecordingPublisher()
      val runner = newRunner(publisher)

      assertThat(runner.publishPendingTasks()).isEqualTo(2)

      assertThat(publisher.taskNames.count { it.startsWith("dataProviders/$DATA_PROVIDER_ID/") })
        .isEqualTo(1)
      assertThat(
          publisher.taskNames.count { it.startsWith("dataProviders/$OTHER_DATA_PROVIDER_ID/") }
        )
        .isEqualTo(1)
    }

  @Test
  fun `racing runners publish only one task for a data provider`() =
    runBlocking<Unit> {
      insertUpload()
      val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
      repeat(5) { index -> service.createDataAvailabilitySyncTask(createRequest(index = index)) }
      val publisher = RecordingPublisher()
      val runners = listOf(newRunner(publisher), newRunner(publisher))

      val publishedCounts =
        runners.map { runner -> async { runner.publishPendingTasks(1) } }.awaitAll()

      assertThat(publishedCounts.sum()).isEqualTo(1)
      assertThat(publisher.taskNames).hasSize(1)
      assertThat(providerSlotCount(DATA_PROVIDER_ID)).isEqualTo(1)
    }

  @Test
  fun `expired publication lease reacquires its data provider slot`() =
    runBlocking<Unit> {
      insertUpload()
      val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
      service.createDataAvailabilitySyncTask(createRequest())
      val publisher = RecordingPublisher()
      val clock = MutableClock(Instant.now().plusSeconds(10))
      spannerDatabase.databaseClient.readWriteTransaction().run { transaction ->
        transaction.claimDataAvailabilitySyncTaskPublication(
          "abandoned-lease",
          clock.instant(),
          clock.instant().plusSeconds(1),
        )
      }
      assertThat(providerSlotCount(DATA_PROVIDER_ID)).isEqualTo(1)
      clock.advance(Duration.ofSeconds(2))

      assertThat(newRunner(publisher, clock).publishPendingTasks()).isEqualTo(1)

      assertThat(publisher.taskNames).containsExactly(TASK_NAME)
      assertThat(providerSlotCount(DATA_PROVIDER_ID)).isEqualTo(1)
    }

  @Test
  fun `stale pending task releases and reacquires its data provider slot`() =
    runBlocking<Unit> {
      insertUpload()
      val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
      service.createDataAvailabilitySyncTask(createRequest())
      val publisher = RecordingPublisher()
      val clock = MutableClock(Instant.now().plusSeconds(10))
      val runner = newRunner(publisher, clock)

      assertThat(runner.publishPendingTasks()).isEqualTo(1)
      clock.advance(Duration.ofHours(2))

      assertThat(runner.publishPendingTasks()).isEqualTo(1)
      assertThat(publisher.taskNames).containsExactly(TASK_NAME, TASK_NAME).inOrder()
      assertThat(providerSlotCount(DATA_PROVIDER_ID)).isEqualTo(1)
    }

  @Test
  fun `terminal task is not republished`() =
    runBlocking<Unit> {
      insertUpload()
      val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
      service.createDataAvailabilitySyncTask(createRequest())
      val publisher = RecordingPublisher()
      val clock = MutableClock(Instant.now().plusSeconds(10))
      val runner = newRunner(publisher, clock)

      spannerDatabase.databaseClient.write(
        listOf(
          Mutation.newUpdateBuilder("DataAvailabilitySyncTask")
            .set("DataProviderResourceId")
            .to(DATA_PROVIDER_ID)
            .set("RawImpressionUploadId")
            .to(1L)
            .set("DataAvailabilitySyncTaskResourceId")
            .to(TASK_ID)
            .set("State")
            .to(DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_SUPERSEDED)
            .set("UpdateTime")
            .to(Value.COMMIT_TIMESTAMP)
            .build()
        )
      )
      clock.advance(Duration.ofHours(2))

      assertThat(runner.publishPendingTasks()).isEqualTo(0)
      assertThat(publisher.taskNames).isEmpty()
      assertThat(providerSlotCount(DATA_PROVIDER_ID)).isEqualTo(0)
    }

  @Test
  fun `published task is reconciled only after becoming stale`() =
    runBlocking<Unit> {
      insertUpload()
      val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
      service.createDataAvailabilitySyncTask(createRequest())
      val publisher = RecordingPublisher()
      val clock = MutableClock(Instant.now().plusSeconds(10))
      val runner = newRunner(publisher, clock)

      assertThat(runner.publishPendingTasks()).isEqualTo(1)
      assertThat(runner.publishPendingTasks()).isEqualTo(0)
      spannerDatabase.databaseClient.write(
        listOf(
          Mutation.newUpdateBuilder("DataAvailabilitySyncTask")
            .set("DataProviderResourceId")
            .to(DATA_PROVIDER_ID)
            .set("RawImpressionUploadId")
            .to(1L)
            .set("DataAvailabilitySyncTaskResourceId")
            .to(TASK_ID)
            .set("State")
            .to(DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_RUNNING)
            .set("AttemptCount")
            .to(1L)
            .set("UpdateTime")
            .to(Value.COMMIT_TIMESTAMP)
            .build()
        )
      )
      clock.advance(Duration.ofHours(2))
      assertThat(runner.publishPendingTasks()).isEqualTo(1)
      assertThat(publisher.taskNames).containsExactly(TASK_NAME, TASK_NAME).inOrder()
      assertThat(publicationPublished()).isTrue()
    }

  @Test
  fun `stale running task with active attempt lease is not republished`() =
    runBlocking<Unit> {
      insertUpload()
      val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
      service.createDataAvailabilitySyncTask(createRequest())
      val publisher = RecordingPublisher()
      val clock = MutableClock(Instant.now().plusSeconds(10))
      val runner = newRunner(publisher, clock)

      assertThat(runner.publishPendingTasks()).isEqualTo(1)
      spannerDatabase.databaseClient.write(
        listOf(
          Mutation.newUpdateBuilder("DataAvailabilitySyncTask")
            .set("DataProviderResourceId")
            .to(DATA_PROVIDER_ID)
            .set("RawImpressionUploadId")
            .to(1L)
            .set("DataAvailabilitySyncTaskResourceId")
            .to(TASK_ID)
            .set("State")
            .to(DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_RUNNING)
            .set("AttemptCount")
            .to(1L)
            .set("UpdateTime")
            .to(Value.COMMIT_TIMESTAMP)
            .build()
        )
      )
      spannerDatabase.databaseClient.readWriteTransaction().run { transaction ->
        transaction.insertDataAvailabilitySyncLease(
          DATA_PROVIDER_ID,
          DataAvailabilitySyncTaskAttempts.leaseId(TASK_NAME, attemptCount = 1),
          clock.instant().plus(Duration.ofHours(3)),
          LEASE_REQUEST_ID,
          ByteString.EMPTY,
        )
      }
      clock.advance(Duration.ofHours(2))

      assertThat(runner.publishPendingTasks()).isEqualTo(0)
      assertThat(publisher.taskNames).containsExactly(TASK_NAME)
      assertThat(providerSlotCount(DATA_PROVIDER_ID)).isEqualTo(1)
    }

  @Test
  fun `failed publication is retried after its backoff`() =
    runBlocking<Unit> {
      val logRecords = mutableListOf<LogRecord>()
      val handler =
        object : Handler() {
          override fun publish(record: LogRecord) {
            logRecords += record
          }

          override fun flush() {}

          override fun close() {}
        }
      val logger = Logger.getLogger(DataAvailabilitySyncTaskPublicationRunner::class.java.name)
      logger.addHandler(handler)
      try {
        insertUpload()
        val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
        service.createDataAvailabilitySyncTask(createRequest())
        val publisher = RecordingPublisher(fail = true)
        val clock = MutableClock(Instant.now().plusSeconds(10))
        val runner = newRunner(publisher, clock)

        assertThat(runner.publishPendingTasks()).isEqualTo(0)
        val failedTask =
          service.getDataAvailabilitySyncTask(
            getDataAvailabilitySyncTaskRequest {
              dataProviderResourceId = DATA_PROVIDER_ID
              rawImpressionUploadResourceId = UPLOAD_ID
              dataAvailabilitySyncTaskResourceId = TASK_ID
            }
          )
        assertThat(failedTask.state)
          .isEqualTo(DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_FAILED)
        assertThat(failedTask.failureCategory)
          .isEqualTo(
            DataAvailabilitySyncTaskFailureCategory
              .DATA_AVAILABILITY_SYNC_TASK_FAILURE_CATEGORY_PUBLICATION
          )
        assertThat(runner.publishPendingTasks()).isEqualTo(0)
        publisher.fail = false
        clock.advance(Duration.ofSeconds(2))

        assertThat(runner.publishPendingTasks()).isEqualTo(1)
        assertThat(publisher.taskNames).containsExactly(TASK_NAME)
      } finally {
        logger.removeHandler(handler)
      }
      val failureLog = logRecords.single { it.message.contains("xmm.outcome=retryable_failure") }
      assertThat(failureLog.message).contains("xmm.lifecycle.stage=availability_task_publication")
      assertThat(failureLog.message)
        .contains("xmm.edpa.data_availability_sync_task.publication_attempt=1")
      assertThat(failureLog.message)
        .contains("xmm.edpa.data_availability_sync_task.failure_category=PUBLICATION")
      assertThat(failureLog.message)
        .contains(
          "xmm.gcs.object.path_hash=" + VidLabelingTraceAttributes.gcsObjectPathHash(DONE_URI)
        )
      assertThat(failureLog.message).contains("xmm.gcs.object.generation=$GENERATION")
      assertThat(failureLog.message).doesNotContain(DONE_URI)
      val retryLog = logRecords.single { it.message.contains("xmm.outcome=retry_scheduled") }
      assertThat(retryLog.message).contains("xmm.edpa.data_availability_sync_task.state=FAILED")
    }

  @Test
  fun `repeated failed publication remains retryable`() =
    runBlocking<Unit> {
      insertUpload()
      val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
      service.createDataAvailabilitySyncTask(createRequest())
      val publisher = RecordingPublisher(fail = true)
      val clock = MutableClock(Instant.now().plusSeconds(10))
      val runner = newRunner(publisher, clock)

      assertThat(runner.publishPendingTasks(limit = 1)).isEqualTo(0)
      clock.advance(Duration.ofSeconds(2))
      assertThat(runner.publishPendingTasks(limit = 1)).isEqualTo(0)
      assertThat(providerSlotCount(DATA_PROVIDER_ID)).isEqualTo(0)
      assertThat(publisher.attemptedTaskNames).containsExactly(TASK_NAME, TASK_NAME).inOrder()

      publisher.fail = false
      clock.advance(Duration.ofSeconds(2))

      assertThat(runner.publishPendingTasks(limit = 1)).isEqualTo(1)
      assertThat(publisher.taskNames).containsExactly(TASK_NAME)
    }

  @Test
  fun `failed publication releases the data provider slot`() =
    runBlocking<Unit> {
      insertUpload()
      val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
      service.createDataAvailabilitySyncTask(createRequest())
      val publisher = RecordingPublisher(fail = true)
      val clock = MutableClock(Instant.now().plusSeconds(10))
      val runner = newRunner(publisher, clock)

      assertThat(runner.publishPendingTasks(limit = 1)).isEqualTo(0)
      assertThat(providerSlotCount(DATA_PROVIDER_ID)).isEqualTo(0)

      service.createDataAvailabilitySyncTask(createRequest(index = 1))
      publisher.fail = false

      assertThat(runner.publishPendingTasks(limit = 1)).isEqualTo(1)
      assertThat(publisher.taskNames).hasSize(1)
    }

  @Test
  fun `lost publication response preserves slot after delivery starts`() =
    runBlocking<Unit> {
      insertUpload()
      val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
      repeat(2) { index -> service.createDataAvailabilitySyncTask(createRequest(index = index)) }
      val publisher =
        RecordingPublisher(
          fail = true,
          beforeFailure = { taskName ->
            spannerDatabase.databaseClient.write(
              listOf(
                Mutation.newUpdateBuilder("DataAvailabilitySyncTask")
                  .set("DataProviderResourceId")
                  .to(DATA_PROVIDER_ID)
                  .set("RawImpressionUploadId")
                  .to(1L)
                  .set("DataAvailabilitySyncTaskResourceId")
                  .to(taskName.substringAfterLast('/'))
                  .set("State")
                  .to(DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_RUNNING)
                  .set("UpdateTime")
                  .to(Value.COMMIT_TIMESTAMP)
                  .build()
              )
            )
          },
        )
      val runner = newRunner(publisher)

      assertThat(runner.publishPendingTasks()).isEqualTo(1)

      val deliveredTaskId = publisher.attemptedTaskNames.single().substringAfterLast('/')
      assertThat(taskState(deliveredTaskId))
        .isEqualTo(
          DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_RUNNING.number.toLong()
        )
      assertThat(publicationPublished(deliveredTaskId)).isTrue()
      assertThat(providerSlotCount(DATA_PROVIDER_ID)).isEqualTo(1)
      assertThat(runner.publishPendingTasks()).isEqualTo(0)
    }

  private fun newRunner(
    publisher: DataAvailabilitySyncTaskPublisher,
    clock: Clock = Clock.systemUTC(),
  ) =
    DataAvailabilitySyncTaskPublicationRunner(
      spannerDatabase.databaseClient,
      publisher,
      clock = clock,
      leaseDuration = Duration.ofMinutes(1),
      initialRetryDelay = Duration.ofSeconds(1),
      maxRetryDelay = Duration.ofMinutes(1),
    )

  private fun createRequest(
    dataProviderResourceId: String = DATA_PROVIDER_ID,
    rawImpressionUploadId: Long = 1L,
    rawImpressionUploadResourceId: String = UPLOAD_ID,
    index: Int = 0,
  ) = createDataAvailabilitySyncTaskRequest {
    val generation = GENERATION + index
    val doneUri = "$DONE_URI_PREFIX/output-$index/done"
    val taskId =
      RequestIds.forDataAvailabilitySyncTask(
        VidLabelingTraceAttributes.gcsObjectPathHash(doneUri),
        generation,
      )
    this.dataProviderResourceId = dataProviderResourceId
    this.rawImpressionUploadResourceId = rawImpressionUploadResourceId
    dataAvailabilitySyncTaskResourceId = taskId
    requestId = taskId
    dataAvailabilitySyncTask = dataAvailabilitySyncTask {
      doneBlobUri = doneUri
      doneBlobGeneration = generation
      cmmsModelLine = MODEL_LINE
      eventDate = date {
        year = 2026
        month = 9
        day = 30
      }
    }
  }

  private suspend fun providerSlotCount(dataProviderResourceId: String): Int =
    spannerDatabase.databaseClient
      .singleUse()
      .executeQuery(
        statement(
          """
          SELECT COUNT(*) AS SlotCount
          FROM DataAvailabilitySyncTaskPublication
          WHERE DataProviderResourceId = @dataProviderResourceId
            AND ProviderSlot = TRUE
          """
            .trimIndent()
        ) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
        }
      )
      .toList()
      .single()
      .getLong("SlotCount")
      .toInt()

  private suspend fun unpublishedCount(dataProviderResourceId: String): Int =
    spannerDatabase.databaseClient
      .singleUse()
      .executeQuery(
        statement(
          """
          SELECT COUNT(*) AS UnpublishedCount
          FROM DataAvailabilitySyncTaskPublication
          WHERE DataProviderResourceId = @dataProviderResourceId
            AND PublishedTime IS NULL
            AND ProviderSlot IS NULL
          """
            .trimIndent()
        ) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
        }
      )
      .toList()
      .single()
      .getLong("UnpublishedCount")
      .toInt()

  private suspend fun publicationPublished(taskResourceId: String = TASK_ID): Boolean {
    val row =
      spannerDatabase.databaseClient
        .singleUse()
        .executeQuery(
          statement(
            """
            SELECT PublishedTime
            FROM DataAvailabilitySyncTaskPublication
            WHERE DataProviderResourceId = @dataProviderResourceId
              AND RawImpressionUploadId = 1
              AND DataAvailabilitySyncTaskResourceId = @taskResourceId
            """
              .trimIndent()
          ) {
            bind("dataProviderResourceId").to(DATA_PROVIDER_ID)
            bind("taskResourceId").to(taskResourceId)
          }
        )
        .toList()
        .single()
    return !row.isNull("PublishedTime")
  }

  private suspend fun taskState(taskResourceId: String): Long =
    spannerDatabase.databaseClient
      .singleUse()
      .executeQuery(
        statement(
          """
          SELECT CAST(State AS INT64) AS State
          FROM DataAvailabilitySyncTask
          WHERE DataProviderResourceId = @dataProviderResourceId
            AND RawImpressionUploadId = 1
            AND DataAvailabilitySyncTaskResourceId = @taskResourceId
          """
            .trimIndent()
        ) {
          bind("dataProviderResourceId").to(DATA_PROVIDER_ID)
          bind("taskResourceId").to(taskResourceId)
        }
      )
      .toList()
      .single()
      .getLong("State")

  private suspend fun insertUpload(
    dataProviderResourceId: String = DATA_PROVIDER_ID,
    rawImpressionUploadId: Long = 1L,
    rawImpressionUploadResourceId: String = UPLOAD_ID,
  ) {
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newInsertBuilder("RawImpressionUpload")
          .set("DataProviderResourceId")
          .to(dataProviderResourceId)
          .set("RawImpressionUploadId")
          .to(rawImpressionUploadId)
          .set("RawImpressionUploadResourceId")
          .to(rawImpressionUploadResourceId)
          .set("DoneBlobUri")
          .to("gs://bucket/raw/done")
          .set("DoneBlobGeneration")
          .to(1L)
          .set("DoneBlobCreateTime")
          .to(com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1L, 0))
          .set("RegistrationComplete")
          .to(true)
          .set("State")
          .to(Value.protoEnum(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED))
          .set("CreateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build(),
        Mutation.newInsertBuilder("RawImpressionUploadModelLine")
          .set("DataProviderResourceId")
          .to(dataProviderResourceId)
          .set("RawImpressionUploadId")
          .to(rawImpressionUploadId)
          .set("RawImpressionUploadModelLineId")
          .to(1L)
          .set("RawImpressionUploadModelLineResourceId")
          .to("upload-model-line")
          .set("CmmsModelLine")
          .to(MODEL_LINE)
          .set("State")
          .to(
            Value.protoEnum(
              RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED
            )
          )
          .set("CreateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build(),
      )
    )
  }

  private class RecordingPublisher(
    var fail: Boolean = false,
    private val beforeFailure: suspend (String) -> Unit = {},
  ) : DataAvailabilitySyncTaskPublisher {
    val taskNames: MutableList<String> = Collections.synchronizedList(mutableListOf<String>())
    val attemptedTaskNames: MutableList<String> =
      Collections.synchronizedList(mutableListOf<String>())

    override suspend fun publish(taskName: String) {
      attemptedTaskNames += taskName
      if (fail) {
        beforeFailure(taskName)
        error("publish failed")
      }
      taskNames += taskName
    }
  }

  private class MutableClock(private var instant: Instant) : Clock() {
    override fun getZone(): ZoneId = ZoneId.of("UTC")

    override fun withZone(zone: ZoneId): Clock = this

    override fun instant(): Instant = instant

    fun advance(duration: Duration) {
      instant = instant.plus(duration)
    }
  }

  companion object {
    @ClassRule @JvmField val spannerEmulator = SpannerEmulatorRule()
    private const val DATA_PROVIDER_ID = "data-provider"
    private const val OTHER_DATA_PROVIDER_ID = "other-data-provider"
    private const val UPLOAD_ID = "upload"
    private const val OTHER_UPLOAD_ID = "other-upload"
    private const val DONE_URI_PREFIX = "gs://bucket/labeled/2026-09-30"
    private const val DONE_URI = "gs://bucket/labeled/2026-09-30/output-0/done"
    private const val GENERATION = 123L
    private const val LEASE_REQUEST_ID = "99999999-9999-4999-8999-999999999999"
    private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml"
    private val TASK_ID =
      RequestIds.forDataAvailabilitySyncTask(
        VidLabelingTraceAttributes.gcsObjectPathHash(DONE_URI),
        GENERATION,
      )
    private val TASK_NAME =
      "dataProviders/$DATA_PROVIDER_ID/rawImpressionUploads/$UPLOAD_ID/" +
        "dataAvailabilitySyncTasks/$TASK_ID"
  }
}
