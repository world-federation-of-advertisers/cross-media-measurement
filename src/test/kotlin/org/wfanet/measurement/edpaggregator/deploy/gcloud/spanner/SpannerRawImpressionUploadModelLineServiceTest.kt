// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.util.UUID
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.internal.testing.RawImpressionUploadModelLineServiceTest
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.insertMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.BlobType
import org.wfanet.measurement.internal.edpaggregator.EncryptedDek
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLine
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineFailureReason
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.VidLabelingEvictionFenceState
import org.wfanet.measurement.internal.edpaggregator.createRawImpressionUploadModelLineRequest
import org.wfanet.measurement.internal.edpaggregator.getRawImpressionUploadModelLineRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionUploadModelLineAvailabilitySyncingRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionUploadModelLineFailedRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionUploadModelLinePoolAssigningRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionUploadModelLineRankingRequest
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadModelLine

class SpannerRawImpressionUploadModelLineServiceTest : RawImpressionUploadModelLineServiceTest() {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  private var nextUploadId: Long = 1L
  private val uploadIdsByResourceId = mutableMapOf<String, Long>()

  override fun newService(
    idGenerator: IdGenerator
  ): RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase {
    val databaseClient: AsyncDatabaseClient = spannerDatabase.databaseClient
    return SpannerRawImpressionUploadModelLineService(databaseClient, idGenerator = idGenerator)
  }

  override suspend fun createParentUpload(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
  ) {
    val uploadId = nextUploadId++
    uploadIdsByResourceId[rawImpressionUploadResourceId] = uploadId
    val mutation =
      insertMutation("RawImpressionUpload") {
        set("DataProviderResourceId").to(dataProviderResourceId)
        set("RawImpressionUploadId").to(uploadId)
        set("RawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
        set("DoneBlobUri").to("gs://bucket/done-$uploadId")
        set("State")
          .to(Value.protoEnum(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED))
        set("CreateTime").to(Value.COMMIT_TIMESTAMP)
        set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
      }
    spannerDatabase.databaseClient.write(listOf(mutation))
  }

  override suspend fun setEvictionFence(
    dataProviderResourceId: String,
    evictionOperationId: String,
    state: VidLabelingEvictionFenceState,
  ) {
    spannerDatabase.databaseClient.write(
      listOf(
        insertMutation("VidLabelingEvictionFence") {
          set("DataProviderResourceId").to(dataProviderResourceId)
          set("EvictionOperationId").to(evictionOperationId)
          set("Etag").to(evictionOperationId)
          set("State").to(Value.protoEnum(state))
          set("CreateTime").to(Value.COMMIT_TIMESTAMP)
        }
      )
    )
  }

  override suspend fun setParentUploadEvictionDisposition(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
    evictionOperationId: String?,
    processingDeferred: Boolean,
  ) {
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("RawImpressionUpload")
          .set("DataProviderResourceId")
          .to(dataProviderResourceId)
          .set("RawImpressionUploadId")
          .to(uploadIdsByResourceId.getValue(rawImpressionUploadResourceId))
          .set("EvictionOperationId")
          .to(evictionOperationId)
          .set("ProcessingDeferred")
          .to(processingDeferred)
          .build()
      )
    )
  }

  override suspend fun setParentUploadState(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
    state: RawImpressionUploadState,
  ) {
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("RawImpressionUpload")
          .set("DataProviderResourceId")
          .to(dataProviderResourceId)
          .set("RawImpressionUploadId")
          .to(uploadIdsByResourceId.getValue(rawImpressionUploadResourceId))
          .set("State")
          .to(Value.protoEnum(state))
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    )
  }

  override suspend fun getParentUploadState(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
  ): RawImpressionUploadState {
    return spannerDatabase.databaseClient.singleUse().use { txn ->
      val row =
        txn
          .executeQuery(
            statement(
              """
              SELECT CAST(State AS INT64) AS StateValue
              FROM RawImpressionUpload
              WHERE DataProviderResourceId = @dataProviderResourceId
                AND RawImpressionUploadResourceId = @rawImpressionUploadResourceId
              """
                .trimIndent()
            ) {
              bind("dataProviderResourceId").to(dataProviderResourceId)
              bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
            }
          )
          .single()
      RawImpressionUploadState.forNumber(row.getLong("StateValue").toInt())
    }
  }

  private val inputDate = date {
    year = 2026
    month = 1
    day = 5
  }
  private val testProvider = "atomic-gap"
  private val testModel = "modelProviders/mp/modelSuites/ms/modelLines/ml"

  private suspend fun registeredModel(
    upload: String,
    uri: String?,
    generation: Long = 1L,
    eventDay: Int = 5,
    model: String = testModel,
  ): RawImpressionUploadModelLine {
    createParentUpload(testProvider, upload)
    val created =
      newService(IdGenerator.Default)
        .createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            dataProviderResourceId = testProvider
            rawImpressionUploadResourceId = upload
            requestId = UUID.randomUUID().toString()
            rawImpressionUploadModelLine = rawImpressionUploadModelLine { cmmsModelLine = model }
          }
        )
    val mutations = mutableListOf<Mutation>()
    if (uri != null)
      mutations +=
        insertMutation("RawImpressionUploadFile") {
          set("DataProviderResourceId").to(testProvider)
          set("RawImpressionUploadId").to(uploadIdsByResourceId.getValue(upload))
          set("FileId").to(1L)
          set("FileResourceId").to("file-$upload")
          set("BlobUri").to(uri)
          set("BlobGeneration").to(generation)
          set("SizeBytes").to(1L)
          set("EventDate").to(com.google.cloud.Date.fromYearMonthDay(2026, 1, eventDay))
          set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        }
    mutations +=
      Mutation.newUpdateBuilder("RawImpressionUpload")
        .set("DataProviderResourceId")
        .to(testProvider)
        .set("RawImpressionUploadId")
        .to(uploadIdsByResourceId.getValue(upload))
        .set("RegistrationComplete")
        .to(true)
        .build()
    spannerDatabase.databaseClient.write(mutations)
    return created
  }

  private suspend fun labeling(model: RawImpressionUploadModelLine): RawImpressionUploadModelLine =
    newService(IdGenerator.Default)
      .markRawImpressionUploadModelLineLabeling(
        markRawImpressionUploadModelLineLabelingRequest {
          dataProviderResourceId = testProvider
          rawImpressionUploadResourceId = model.rawImpressionUploadResourceId
          rawImpressionUploadModelLineResourceId = model.rawImpressionUploadModelLineResourceId
          etag = model.etag
          requestId = UUID.randomUUID().toString()
        }
      )

  private suspend fun syncing(model: RawImpressionUploadModelLine): RawImpressionUploadModelLine =
    newService(IdGenerator.Default)
      .markRawImpressionUploadModelLineAvailabilitySyncing(
        markRawImpressionUploadModelLineAvailabilitySyncingRequest {
          dataProviderResourceId = testProvider
          rawImpressionUploadResourceId = model.rawImpressionUploadResourceId
          rawImpressionUploadModelLineResourceId = model.rawImpressionUploadModelLineResourceId
          etag = model.etag
          pendingAvailabilityDates += inputDate
          requestId = UUID.randomUUID().toString()
        }
      )

  @Test
  fun `atomic claim permits noncolliding input after availability`() = runBlocking {
    syncing(labeling(registeredModel("prior", "gs://input/prior")))
    val next = labeling(registeredModel("next", "gs://input/new"))
    assertThat(next.state)
      .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING)
    assertThat(syncing(next).state)
      .isEqualTo(
        RawImpressionUploadModelLineState
          .RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_AVAILABILITY_SYNCING
      )
  }

  @Test
  fun `late input admits computation beside two disjoint pending availability owners`(): Unit =
    runBlocking {
      val first = syncing(labeling(registeredModel("first-pending", "gs://input/first-pending")))
      val second = syncing(labeling(registeredModel("second-pending", "gs://input/second-pending")))
      val late = labeling(registeredModel("late-independent", "gs://input/late-independent"))
      assertThat(late.state)
        .isEqualTo(
          RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING
        )
      for (owner in listOf(first, second)) {
        val current =
          newService(IdGenerator.Default)
            .getRawImpressionUploadModelLine(
              getRawImpressionUploadModelLineRequest {
                dataProviderResourceId = testProvider
                rawImpressionUploadResourceId = owner.rawImpressionUploadResourceId
                rawImpressionUploadModelLineResourceId =
                  owner.rawImpressionUploadModelLineResourceId
              }
            )
        assertThat(current.state)
          .isEqualTo(
            RawImpressionUploadModelLineState
              .RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_AVAILABILITY_SYNCING
          )
      }
    }

  @Test
  fun `simultaneous noncolliding computation claims admit exactly one upload after availability`():
    Unit = runBlocking {
    syncing(labeling(registeredModel("prior", "gs://input/prior")))
    val candidates =
      listOf(
        registeredModel("first", "gs://input/first"),
        registeredModel("second", "gs://input/second"),
      )
    val outcomes = coroutineScope {
      val ready = Channel<Unit>(candidates.size)
      val start = CompletableDeferred<Unit>()
      val claims =
        candidates.map { candidate ->
          async(Dispatchers.Default) {
            ready.send(Unit)
            start.await()
            runCatching {
              newService(IdGenerator.Default)
                .markRawImpressionUploadModelLinePoolAssigning(
                  markRawImpressionUploadModelLinePoolAssigningRequest {
                    dataProviderResourceId = testProvider
                    rawImpressionUploadResourceId = candidate.rawImpressionUploadResourceId
                    rawImpressionUploadModelLineResourceId =
                      candidate.rawImpressionUploadModelLineResourceId
                    etag = candidate.etag
                    requestId = UUID.randomUUID().toString()
                  }
                )
            }
          }
        }
      repeat(candidates.size) { ready.receive() }
      start.complete(Unit)
      claims.awaitAll()
    }
    assertThat(outcomes.count { it.isSuccess }).isEqualTo(1)
    val failure = outcomes.single { it.isFailure }.exceptionOrNull()
    assertThat(failure).isInstanceOf(StatusRuntimeException::class.java)
    assertThat((failure as StatusRuntimeException).status.code)
      .isEqualTo(Status.Code.FAILED_PRECONDITION)
    val states =
      candidates.map { candidate ->
        newService(IdGenerator.Default)
          .getRawImpressionUploadModelLine(
            getRawImpressionUploadModelLineRequest {
              dataProviderResourceId = testProvider
              rawImpressionUploadResourceId = candidate.rawImpressionUploadResourceId
              rawImpressionUploadModelLineResourceId =
                candidate.rawImpressionUploadModelLineResourceId
            }
          )
          .state
      }
    assertThat(states)
      .containsExactly(
        RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING,
        RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED,
      )
  }

  @Test
  fun `atomic claim rejects output overlap even when input generation changes`() = runBlocking {
    syncing(labeling(registeredModel("prior", "gs://input/shared")))
    val next = registeredModel("next", "gs://input/shared", generation = 2L)
    assertThat(assertFailsWith<StatusRuntimeException> { labeling(next) }.status.code)
      .isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `atomic claim keeps availability owner under failed sibling parent`() = runBlocking {
    val prior = registeredModel("prior", "gs://input/shared")
    syncing(labeling(prior))
    val sibling =
      newService(IdGenerator.Default)
        .createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            dataProviderResourceId = testProvider
            rawImpressionUploadResourceId = "prior"
            requestId = UUID.randomUUID().toString()
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = "$testModel-sibling"
            }
          }
        )
    newService(IdGenerator.Default)
      .markRawImpressionUploadModelLineFailed(
        markRawImpressionUploadModelLineFailedRequest {
          dataProviderResourceId = testProvider
          rawImpressionUploadResourceId = "prior"
          rawImpressionUploadModelLineResourceId = sibling.rawImpressionUploadModelLineResourceId
          etag = sibling.etag
          requestId = UUID.randomUUID().toString()
        }
      )
    val next = registeredModel("next", "gs://input/shared")
    assertThat(assertFailsWith<StatusRuntimeException> { labeling(next) }.status.code)
      .isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `atomic claim rejects unknown candidate manifest`() = runBlocking {
    syncing(labeling(registeredModel("prior", "gs://input/prior")))
    val next = registeredModel("next", null)
    assertThat(assertFailsWith<StatusRuntimeException> { labeling(next) }.status.code)
      .isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `availability processing failure cannot invalidate an active computation predecessor`():
    Unit = runBlocking {
    val prior = syncing(labeling(registeredModel("prior", "gs://input/prior")))
    labeling(registeredModel("next", "gs://input/new"))
    val error =
      assertFailsWith<StatusRuntimeException> {
        newService(IdGenerator.Default)
          .markRawImpressionUploadModelLineFailed(
            markRawImpressionUploadModelLineFailedRequest {
              dataProviderResourceId = testProvider
              rawImpressionUploadResourceId = "prior"
              rawImpressionUploadModelLineResourceId = prior.rawImpressionUploadModelLineResourceId
              etag = prior.etag
              requestId = UUID.randomUUID().toString()
              failureReason =
                RawImpressionUploadModelLineFailureReason
                  .RAW_IMPRESSION_UPLOAD_MODEL_LINE_FAILURE_REASON_PROCESSING_FAILURE
            }
          )
      }
    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `atomic claim permits same URI on a different event date`(): Unit = runBlocking {
    syncing(labeling(registeredModel("prior", "gs://input/shared")))
    val next = labeling(registeredModel("next", "gs://input/shared", eventDay = 4))
    assertThat(next.state)
      .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING)
  }

  @Test
  fun `atomic claim rejects unknown availability owner manifest`(): Unit = runBlocking {
    syncing(labeling(registeredModel("prior", null)))
    val next = registeredModel("next", "gs://input/new")
    assertThat(assertFailsWith<StatusRuntimeException> { labeling(next) }.status.code)
      .isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `atomic claim rejects incomplete availability owner registration`(): Unit = runBlocking {
    syncing(labeling(registeredModel("prior", "gs://input/prior")))
    setRegistrationComplete("prior", false)
    val next = registeredModel("next", "gs://input/new")
    assertThat(assertFailsWith<StatusRuntimeException> { labeling(next) }.status.code)
      .isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `atomic claim rejects incomplete candidate registration`(): Unit = runBlocking {
    syncing(labeling(registeredModel("prior", "gs://input/prior")))
    val next = registeredModel("next", "gs://input/new")
    setRegistrationComplete("next", false)
    assertThat(assertFailsWith<StatusRuntimeException> { labeling(next) }.status.code)
      .isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `atomic claim rejects empty input identity`(): Unit = runBlocking {
    syncing(labeling(registeredModel("prior", "gs://input/prior")))
    val next = registeredModel("next", "")
    assertThat(assertFailsWith<StatusRuntimeException> { labeling(next) }.status.code)
      .isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `availability processing failure remains legal without a same-model consumer`(): Unit =
    runBlocking {
      val prior = syncing(labeling(registeredModel("prior", "gs://input/prior")))
      labeling(registeredModel("other", "gs://input/other", model = "$testModel-other"))
      val failed =
        newService(IdGenerator.Default)
          .markRawImpressionUploadModelLineFailed(
            markRawImpressionUploadModelLineFailedRequest {
              dataProviderResourceId = testProvider
              rawImpressionUploadResourceId = "prior"
              rawImpressionUploadModelLineResourceId = prior.rawImpressionUploadModelLineResourceId
              etag = prior.etag
              requestId = UUID.randomUUID().toString()
            }
          )
      assertThat(failed.state)
        .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED)
    }

  @Test
  fun `deferred ordinary upload clears its gate atomically after fence release`(): Unit =
    runBlocking {
      val next = registeredModel("next", "gs://input/new")
      setParentUploadEvictionDisposition(testProvider, "next", null, true)
      labeling(next)
      val deferred =
        spannerDatabase.databaseClient.singleUse().use { txn ->
          txn
            .executeQuery(
              statement(
                "SELECT ProcessingDeferred FROM RawImpressionUpload WHERE DataProviderResourceId = @provider AND RawImpressionUploadId = @upload"
              ) {
                bind("provider").to(testProvider)
                bind("upload").to(uploadIdsByResourceId.getValue("next"))
              }
            )
            .single()
            .getBoolean("ProcessingDeferred")
        }
      assertThat(deferred).isFalse()
    }

  @Test
  fun `deferred correction upload cannot clear its gate without authorization`(): Unit =
    runBlocking {
      val next = registeredModel("next", "gs://input/new")
      setParentUploadEvictionDisposition(testProvider, "next", "correction", true)
      assertThat(assertFailsWith<StatusRuntimeException> { labeling(next) }.status.code)
        .isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  private suspend fun setRegistrationComplete(upload: String, complete: Boolean) {
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("RawImpressionUpload")
          .set("DataProviderResourceId")
          .to(testProvider)
          .set("RawImpressionUploadId")
          .to(uploadIdsByResourceId.getValue(upload))
          .set("RegistrationComplete")
          .to(complete)
          .build()
      )
    )
  }

  private suspend fun failedRanking(withSnapshot: Boolean): RawImpressionUploadModelLine {
    val created = registeredModel("failed", "gs://input/failed")
    val pool =
      newService(IdGenerator.Default)
        .markRawImpressionUploadModelLinePoolAssigning(
          markRawImpressionUploadModelLinePoolAssigningRequest {
            dataProviderResourceId = testProvider
            rawImpressionUploadResourceId = "failed"
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = created.etag
            requestId = UUID.randomUUID().toString()
          }
        )
    val ranked =
      newService(IdGenerator.Default)
        .markRawImpressionUploadModelLineRanking(
          markRawImpressionUploadModelLineRankingRequest {
            dataProviderResourceId = testProvider
            rawImpressionUploadResourceId = "failed"
            rawImpressionUploadModelLineResourceId = pool.rawImpressionUploadModelLineResourceId
            etag = pool.etag
            requestId = UUID.randomUUID().toString()
          }
        )
    if (withSnapshot)
      spannerDatabase.databaseClient.write(
        listOf(
          insertMutation("RankIndexBlob") {
            set("DataProviderResourceId").to(testProvider)
            set("RawImpressionUploadId").to(uploadIdsByResourceId.getValue("failed"))
            set("RankIndexBlobId").to(1L)
            set("RankIndexBlobResourceId").to("snapshot")
            set("CmmsModelLine").to(testModel)
            set("BlobType").to(Value.protoEnum(BlobType.BLOB_TYPE_SNAPSHOT))
            set("PoolOffset").to(0L)
            set("BlobUri").to("gs://ranks/snapshot")
            set("EncryptedDek").to(Value.protoMessage(EncryptedDek.getDefaultInstance()))
            set("MaxEventDate").to(com.google.cloud.Date.fromYearMonthDay(2026, 1, 5))
            set("BlobChecksum").to(com.google.cloud.ByteArray.copyFrom(ByteArray(32)))
            set("CreateTime").to(Value.COMMIT_TIMESTAMP)
            set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
          }
        )
      )
    return newService(IdGenerator.Default)
      .markRawImpressionUploadModelLineFailed(
        markRawImpressionUploadModelLineFailedRequest {
          dataProviderResourceId = testProvider
          rawImpressionUploadResourceId = "failed"
          rawImpressionUploadModelLineResourceId = ranked.rawImpressionUploadModelLineResourceId
          etag = ranked.etag
          requestId = UUID.randomUUID().toString()
          failureReason =
            RawImpressionUploadModelLineFailureReason
              .RAW_IMPRESSION_UPLOAD_MODEL_LINE_FAILURE_REASON_PROCESSING_FAILURE
        }
      )
  }

  @Test
  fun `atomic claim keeps later computation behind failed snapshot`() = runBlocking {
    failedRanking(withSnapshot = true)
    val next = registeredModel("next", "gs://input/new")
    assertThat(assertFailsWith<StatusRuntimeException> { labeling(next) }.status.code)
      .isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `atomic claim allows later computation when failed ranking has no snapshot`() = runBlocking {
    failedRanking(withSnapshot = false)
    assertThat(labeling(registeredModel("next", "gs://input/new")).state)
      .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING)
  }

  @Test
  fun `direct healing with disjoint output follows unaffected availability under fence`(): Unit =
    runBlocking {
      syncing(labeling(registeredModel("unaffected", "gs://input/unaffected")))
      val replacement = registeredModel("replacement", "gs://input/replacement")
      authorizeHealingUpload("replacement")
      val active = labeling(replacement)
      assertThat(active.state)
        .isEqualTo(
          RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING
        )
      assertThat(syncing(active).state)
        .isEqualTo(
          RawImpressionUploadModelLineState
            .RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_AVAILABILITY_SYNCING
        )
    }

  @Test
  fun `direct healing still rejects overlapping output under fence`(): Unit = runBlocking {
    syncing(labeling(registeredModel("unaffected", "gs://input/shared")))
    val replacement = registeredModel("replacement", "gs://input/shared")
    authorizeHealingUpload("replacement")
    assertThat(assertFailsWith<StatusRuntimeException> { labeling(replacement) }.status.code)
      .isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `memoized healing still waits for unrelated availability owner`(): Unit = runBlocking {
    syncing(labeling(registeredModel("unaffected", "gs://input/unaffected")))
    val replacement = registeredModel("replacement", "gs://input/replacement")
    authorizeHealingUpload("replacement")
    val error =
      assertFailsWith<StatusRuntimeException> {
        newService(IdGenerator.Default)
          .markRawImpressionUploadModelLinePoolAssigning(
            markRawImpressionUploadModelLinePoolAssigningRequest {
              dataProviderResourceId = testProvider
              rawImpressionUploadResourceId = "replacement"
              rawImpressionUploadModelLineResourceId =
                replacement.rawImpressionUploadModelLineResourceId
              etag = replacement.etag
              requestId = UUID.randomUUID().toString()
            }
          )
      }
    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `direct healing exception rejects candidate memoized processing evidence`(): Unit =
    runBlocking {
      syncing(labeling(registeredModel("unaffected", "gs://input/unaffected")))
      val replacement = registeredModel("replacement", "gs://input/replacement")
      addPoolAssignmentEvidence("replacement")
      authorizeHealingUpload("replacement")
      assertThat(assertFailsWith<StatusRuntimeException> { labeling(replacement) }.status.code)
        .isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `direct healing exception rejects availability owner memoized processing evidence`(): Unit =
    runBlocking {
      syncing(labeling(registeredModel("unaffected", "gs://input/unaffected")))
      addPoolAssignmentEvidence("unaffected")
      val replacement = registeredModel("replacement", "gs://input/replacement")
      authorizeHealingUpload("replacement")
      assertThat(assertFailsWith<StatusRuntimeException> { labeling(replacement) }.status.code)
        .isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `direct healing processing failure retries beside disjoint availability`(): Unit =
    runBlocking {
      syncing(labeling(registeredModel("unaffected", "gs://input/unaffected")))
      val failed =
        failProcessing(labeling(registeredModel("replacement", "gs://input/replacement")))
      authorizeHealingUpload("replacement")
      val retried = labeling(failed)
      assertThat(retried.state)
        .isEqualTo(
          RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING
        )
      assertThat(syncing(retried).state)
        .isEqualTo(
          RawImpressionUploadModelLineState
            .RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_AVAILABILITY_SYNCING
        )
    }

  @Test
  fun `direct healing failed retry still rejects overlapping availability output`(): Unit =
    runBlocking {
      val failed = failProcessing(labeling(registeredModel("replacement", "gs://input/shared")))
      syncing(labeling(registeredModel("unaffected", "gs://input/shared")))
      authorizeHealingUpload("replacement")
      assertThat(assertFailsWith<StatusRuntimeException> { labeling(failed) }.status.code)
        .isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `direct healing failed retry still rejects memoized processing evidence`(): Unit =
    runBlocking {
      val failed =
        failProcessing(labeling(registeredModel("replacement", "gs://input/replacement")))
      addPoolAssignmentEvidence("replacement")
      syncing(labeling(registeredModel("unaffected", "gs://input/unaffected")))
      authorizeHealingUpload("replacement")
      assertThat(assertFailsWith<StatusRuntimeException> { labeling(failed) }.status.code)
        .isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `direct healing exception never retries evicted output`(): Unit = runBlocking {
    syncing(labeling(registeredModel("unaffected", "gs://input/unaffected")))
    val pending = syncing(labeling(registeredModel("replacement", "gs://input/replacement")))
    val evicted =
      newService(IdGenerator.Default)
        .markRawImpressionUploadModelLineFailed(
          markRawImpressionUploadModelLineFailedRequest {
            dataProviderResourceId = testProvider
            rawImpressionUploadResourceId = "replacement"
            rawImpressionUploadModelLineResourceId = pending.rawImpressionUploadModelLineResourceId
            etag = pending.etag
            requestId = UUID.randomUUID().toString()
            failureReason =
              RawImpressionUploadModelLineFailureReason
                .RAW_IMPRESSION_UPLOAD_MODEL_LINE_FAILURE_REASON_EVICTED_OUTPUT
            evictionOperationId = UUID.randomUUID().toString()
            recoveryAction =
              RawImpressionUploadModelLineRecoveryAction
                .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY
          }
        )
    authorizeHealingUpload("replacement")
    assertThat(assertFailsWith<StatusRuntimeException> { labeling(evicted) }.status.code)
      .isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `ordinary labeling failure and retry preserve an independent pending availability owner`():
    Unit = runBlocking {
    val prior = syncing(labeling(registeredModel("failure-prior", "gs://input/failure-prior")))
    val active = labeling(registeredModel("failure-current", "gs://input/failure-current"))
    val failed = failProcessing(active)
    assertThat(failed.state)
      .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED)
    assertThat(failed.failureReason)
      .isEqualTo(
        RawImpressionUploadModelLineFailureReason
          .RAW_IMPRESSION_UPLOAD_MODEL_LINE_FAILURE_REASON_PROCESSING_FAILURE
      )
    val retried = labeling(failed)
    assertThat(retried.state)
      .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING)
    assertThat(syncing(retried).state)
      .isEqualTo(
        RawImpressionUploadModelLineState
          .RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_AVAILABILITY_SYNCING
      )
    val unchanged =
      newService(IdGenerator.Default)
        .getRawImpressionUploadModelLine(
          getRawImpressionUploadModelLineRequest {
            dataProviderResourceId = testProvider
            rawImpressionUploadResourceId = prior.rawImpressionUploadResourceId
            rawImpressionUploadModelLineResourceId = prior.rawImpressionUploadModelLineResourceId
          }
        )
    assertThat(unchanged.state)
      .isEqualTo(
        RawImpressionUploadModelLineState
          .RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_AVAILABILITY_SYNCING
      )
  }

  @Test
  fun `snapshot free failed availability neither strands retry nor blocks independent input`():
    Unit = runBlocking {
    val prior = syncing(labeling(registeredModel("failed-handoff", "gs://input/failed-handoff")))
    val failed = failProcessing(prior)
    val next =
      syncing(labeling(registeredModel("independent-handoff", "gs://input/independent-handoff")))
    val retried = labeling(failed)
    assertThat(retried.state)
      .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING)
    assertThat(syncing(retried).state)
      .isEqualTo(
        RawImpressionUploadModelLineState
          .RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_AVAILABILITY_SYNCING
      )
    val unchanged =
      newService(IdGenerator.Default)
        .getRawImpressionUploadModelLine(
          getRawImpressionUploadModelLineRequest {
            dataProviderResourceId = testProvider
            rawImpressionUploadResourceId = next.rawImpressionUploadResourceId
            rawImpressionUploadModelLineResourceId = next.rawImpressionUploadModelLineResourceId
          }
        )
    assertThat(unchanged.state)
      .isEqualTo(
        RawImpressionUploadModelLineState
          .RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_AVAILABILITY_SYNCING
      )
  }

  @Test
  fun `deferred ordinary claim can resume when its fence and correction gates are gone`(): Unit =
    runBlocking {
      val candidate = registeredModel("released-deferred", "gs://input/released-deferred")
      spannerDatabase.databaseClient.write(
        listOf(
          Mutation.newUpdateBuilder("RawImpressionUpload")
            .set("DataProviderResourceId")
            .to(testProvider)
            .set("RawImpressionUploadId")
            .to(uploadIdsByResourceId.getValue("released-deferred"))
            .set("ProcessingDeferred")
            .to(true)
            .build()
        )
      )
      assertThat(labeling(candidate).state)
        .isEqualTo(
          RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING
        )
    }

  @Test
  fun `deferred recovery remains blocked after fence release while operation ownership is unresolved`():
    Unit = runBlocking {
    val candidate = registeredModel("owned-deferred", "gs://input/owned-deferred")
    setParentUploadEvictionDisposition(
      testProvider,
      "owned-deferred",
      "unresolved-operation",
      false,
    )
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("RawImpressionUpload")
          .set("DataProviderResourceId")
          .to(testProvider)
          .set("RawImpressionUploadId")
          .to(uploadIdsByResourceId.getValue("owned-deferred"))
          .set("ProcessingDeferred")
          .to(true)
          .build()
      )
    )
    assertThat(assertFailsWith<StatusRuntimeException> { labeling(candidate) }.status.code)
      .isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(
        newService(IdGenerator.Default)
          .getRawImpressionUploadModelLine(
            getRawImpressionUploadModelLineRequest {
              dataProviderResourceId = testProvider
              rawImpressionUploadResourceId = candidate.rawImpressionUploadResourceId
              rawImpressionUploadModelLineResourceId =
                candidate.rawImpressionUploadModelLineResourceId
            }
          )
          .state
      )
      .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED)
  }

  private suspend fun failProcessing(
    model: RawImpressionUploadModelLine
  ): RawImpressionUploadModelLine =
    newService(IdGenerator.Default)
      .markRawImpressionUploadModelLineFailed(
        markRawImpressionUploadModelLineFailedRequest {
          dataProviderResourceId = testProvider
          rawImpressionUploadResourceId = model.rawImpressionUploadResourceId
          rawImpressionUploadModelLineResourceId = model.rawImpressionUploadModelLineResourceId
          etag = model.etag
          requestId = UUID.randomUUID().toString()
          failureReason =
            RawImpressionUploadModelLineFailureReason
              .RAW_IMPRESSION_UPLOAD_MODEL_LINE_FAILURE_REASON_PROCESSING_FAILURE
        }
      )

  private suspend fun authorizeHealingUpload(upload: String) {
    setParentUploadEvictionDisposition(testProvider, upload, "healing", false)
    setEvictionFence(
      testProvider,
      "healing",
      VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING,
    )
  }

  private suspend fun addPoolAssignmentEvidence(upload: String) {
    spannerDatabase.databaseClient.write(
      listOf(
        insertMutation("PoolAssignmentJob") {
          set("DataProviderResourceId").to(testProvider)
          set("RawImpressionUploadId").to(uploadIdsByResourceId.getValue(upload))
          set("PoolAssignmentJobId").to(1L)
          set("PoolAssignmentJobResourceId").to("pool-evidence")
          set("CmmsModelLine").to(testModel)
          set("ShardIndex").to(0L)
          set("CreateRequestId").to(UUID.randomUUID().toString())
          set("State").to(Value.protoEnum(PoolAssignmentState.POOL_ASSIGNMENT_STATE_SUCCEEDED))
          set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        }
      )
    )
  }

  @Test
  fun `memoized healing admits historical availability using deleted original pool boundary`():
    Unit = runBlocking {
    verifyHistoricalAvailabilityAdmission(
      sourceSnapshot = true,
      newer = false,
      overlap = false,
      expected = true,
    )
  }

  @Test
  fun `memoized healing rejects unapproved availability newer than original pool boundary`(): Unit =
    runBlocking {
      verifyHistoricalAvailabilityAdmission(
        sourceSnapshot = true,
        newer = true,
        overlap = false,
        expected = false,
      )
    }

  @Test
  fun `memoized healing still rejects historical overlapping output`(): Unit = runBlocking {
    verifyHistoricalAvailabilityAdmission(
      sourceSnapshot = true,
      newer = false,
      overlap = true,
      expected = false,
    )
  }

  @Test
  fun `memoized healing without original snapshot requires history before source creation`(): Unit =
    runBlocking {
      verifyHistoricalAvailabilityAdmission(
        sourceSnapshot = false,
        newer = false,
        overlap = false,
        expected = true,
      )
    }

  @Test
  fun `memoized healing without original snapshot rejects unproven later checkpoint`(): Unit =
    runBlocking {
      verifyHistoricalAvailabilityAdmission(
        sourceSnapshot = false,
        newer = true,
        overlap = false,
        expected = false,
      )
    }

  @Test
  fun `historical owner pool absent from original uses first source checkpoint after registration`():
    Unit = runBlocking {
    verifyHistoricalAvailabilityAdmission(
      sourceSnapshot = true,
      newer = false,
      overlap = false,
      expected = true,
      missingSourcePool = true,
    )
  }

  @Test
  fun `owner pool absent from original still rejects snapshot after first source checkpoint`():
    Unit = runBlocking {
    verifyHistoricalAvailabilityAdmission(
      sourceSnapshot = true,
      newer = true,
      overlap = false,
      expected = false,
      missingSourcePool = true,
    )
  }

  private suspend fun verifyHistoricalAvailabilityAdmission(
    sourceSnapshot: Boolean,
    newer: Boolean,
    overlap: Boolean,
    expected: Boolean,
    missingSourcePool: Boolean = false,
  ) {
    val source: RawImpressionUploadModelLine
    if (sourceSnapshot || newer) {
      source = registeredModel("source", "gs://input/source")
      if (sourceSnapshot && newer) addHistoricalCheckpoint("source", deleted = true)
    } else {
      source = rawImpressionUploadModelLine {}
    }
    val history = registeredModel("history", "gs://input/history")
    addHistoricalCheckpoint("history", poolOffset = if (missingSourcePool) 1L else 0L)
    syncing(labeling(history))
    val original =
      if (source.rawImpressionUploadResourceId.isEmpty())
        registeredModel("source", "gs://input/source")
      else source
    if (sourceSnapshot && !newer) addHistoricalCheckpoint("source", deleted = true)
    val replacement =
      registeredModel(
        "replacement",
        if (overlap) "gs://input/history" else "gs://input/replacement",
      )
    authorizeHistoricalRecoveryTarget(original, "replacement")
    suspend fun claim() =
      newService(IdGenerator.Default)
        .markRawImpressionUploadModelLinePoolAssigning(
          markRawImpressionUploadModelLinePoolAssigningRequest {
            dataProviderResourceId = testProvider
            rawImpressionUploadResourceId = "replacement"
            rawImpressionUploadModelLineResourceId =
              replacement.rawImpressionUploadModelLineResourceId
            etag = replacement.etag
            requestId = UUID.randomUUID().toString()
          }
        )
    if (expected)
      assertThat(claim().state)
        .isEqualTo(
          RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING
        )
    else
      assertThat(assertFailsWith<StatusRuntimeException> { claim() }.status.code)
        .isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  private suspend fun authorizeHistoricalRecoveryTarget(
    source: RawImpressionUploadModelLine,
    candidate: String,
  ) {
    authorizeHealingUpload(candidate)
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("RawImpressionUpload")
          .set("DataProviderResourceId")
          .to(testProvider)
          .set("RawImpressionUploadId")
          .to(uploadIdsByResourceId.getValue(candidate))
          .set("ReplacesRawImpressionUploadResourceId")
          .to(source.rawImpressionUploadResourceId)
          .build(),
        insertMutation("UploadHealingOperation") {
          set("DataProviderResourceId").to(testProvider)
          set("UploadHealingOperationId").to("healing")
          set("ReconcileRequestId").to(UUID.randomUUID().toString())
          set("Reason").to("unit historical recovery")
          set("LabeledImpressionsBlobPrefix").to("gs://output/vid")
          set("State")
            .to(
              UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_REPLAYING.number.toLong()
            )
          set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        },
        insertMutation("UploadHealingStep") {
          set("DataProviderResourceId").to(testProvider)
          set("UploadHealingOperationId").to("healing")
          set("UploadHealingStepId").to(1L)
          set("SequenceNumber").to(1L)
          set("SourceRawImpressionUploadResourceId").to(source.rawImpressionUploadResourceId)
          set("RawImpressionUploadModelLineResourceId")
            .to(source.rawImpressionUploadModelLineResourceId)
          set("CmmsModelLine").to(testModel)
          set("Memoized").to(true)
          set("RecoveryTarget").to(true)
          set("RecoveryAction")
            .to(
              Value.protoEnum(
                RawImpressionUploadModelLineRecoveryAction
                  .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY
              )
            )
          set("LabeledOutputManifest").to(com.google.cloud.ByteArray.copyFrom(ByteArray(0)))
          set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        },
      )
    )
  }

  private suspend fun addHistoricalCheckpoint(
    upload: String,
    deleted: Boolean = false,
    poolOffset: Long = 0L,
  ) {
    spannerDatabase.databaseClient.write(
      listOf(
        insertMutation("RankIndexBlob") {
          set("DataProviderResourceId").to(testProvider)
          set("RawImpressionUploadId").to(uploadIdsByResourceId.getValue(upload))
          set("RankIndexBlobId").to(1L)
          set("RankIndexBlobResourceId").to("snapshot-$upload")
          set("CmmsModelLine").to(testModel)
          set("BlobType").to(Value.protoEnum(BlobType.BLOB_TYPE_SNAPSHOT))
          set("PoolOffset").to(poolOffset)
          set("BlobUri").to("gs://ranks/$upload")
          set("EncryptedDek").to(Value.protoMessage(EncryptedDek.getDefaultInstance()))
          set("MaxEventDate").to(com.google.cloud.Date.fromYearMonthDay(2026, 1, 5))
          set("BlobChecksum").to(com.google.cloud.ByteArray.copyFrom(ByteArray(32)))
          set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
          if (deleted) set("DeleteTime").to(Value.COMMIT_TIMESTAMP)
        }
      )
    )
  }

  @Test
  fun `ordinary retry rejects checkpoint basis advanced by completed owner`(): Unit = runBlocking {
    val failed = failedRanking(withSnapshot = true)
    historicalSnapshotOwner(
      RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED
    )
    val error = assertFailsWith<StatusRuntimeException> { retryRanking(failed) }
    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(error.status.description).contains("approved healing")
    assertThat(error.status.description).contains("newer")
  }

  @Test
  fun `ordinary retry rejects checkpoint basis advanced by availability owner`(): Unit =
    runBlocking {
      val failed = failedRanking(withSnapshot = true)
      historicalSnapshotOwner(
        RawImpressionUploadModelLineState
          .RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_AVAILABILITY_SYNCING
      )
      assertThat(assertFailsWith<StatusRuntimeException> { retryRanking(failed) }.status.code)
        .isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `ordinary partial retry preserves checkpoints when no newer basis exists`(): Unit =
    runBlocking {
      val failed = failedRanking(withSnapshot = true)
      assertThat(retryRanking(failed).state)
        .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING)
    }

  @Test
  fun `ordinary partial retry ignores newer snapshots for another pool`(): Unit = runBlocking {
    val failed = failedRanking(withSnapshot = true)
    historicalSnapshotOwner(
      RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED,
      poolOffset = 1L,
    )
    assertThat(retryRanking(failed).state)
      .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING)
  }

  @Test
  fun `ordinary partial retry ignores deleted newer basis`(): Unit = runBlocking {
    val failed = failedRanking(withSnapshot = true)
    historicalSnapshotOwner(
      RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED,
      deleted = true,
    )
    assertThat(retryRanking(failed).state)
      .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING)
  }

  @Test
  fun `ordinary partial retry ignores older usable basis`(): Unit = runBlocking {
    historicalSnapshotOwner(
      RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED
    )
    val failed = failedRanking(withSnapshot = true)
    assertThat(retryRanking(failed).state)
      .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING)
  }

  private suspend fun retryRanking(
    model: RawImpressionUploadModelLine
  ): RawImpressionUploadModelLine =
    newService(IdGenerator.Default)
      .markRawImpressionUploadModelLineRanking(
        markRawImpressionUploadModelLineRankingRequest {
          dataProviderResourceId = testProvider
          rawImpressionUploadResourceId = model.rawImpressionUploadResourceId
          rawImpressionUploadModelLineResourceId = model.rawImpressionUploadModelLineResourceId
          etag = model.etag
          requestId = UUID.randomUUID().toString()
        }
      )

  // Fixture for a history created before failed-checkpoint serialization was introduced. The
  // production API now prevents constructing this history by starting another computation.
  private suspend fun historicalSnapshotOwner(
    state: RawImpressionUploadModelLineState,
    poolOffset: Long = 0L,
    deleted: Boolean = false,
  ) {
    val owner = registeredModel("newer", "gs://input/newer", eventDay = 6)
    setParentUploadState(
      testProvider,
      "newer",
      if (
        state == RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED
      )
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED
      else RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_ACTIVE,
    )
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("RawImpressionUploadModelLine")
          .set("DataProviderResourceId")
          .to(testProvider)
          .set("RawImpressionUploadId")
          .to(uploadIdsByResourceId.getValue("newer"))
          .set("RawImpressionUploadModelLineId")
          .to(readModelId(owner))
          .set("State")
          .to(Value.protoEnum(state))
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build(),
        insertMutation("RankIndexBlob") {
          set("DataProviderResourceId").to(testProvider)
          set("RawImpressionUploadId").to(uploadIdsByResourceId.getValue("newer"))
          set("RankIndexBlobId").to(1L)
          set("RankIndexBlobResourceId").to("newer-snapshot")
          set("CmmsModelLine").to(testModel)
          set("BlobType").to(Value.protoEnum(BlobType.BLOB_TYPE_SNAPSHOT))
          set("PoolOffset").to(poolOffset)
          set("BlobUri").to("gs://ranks/newer")
          set("EncryptedDek").to(Value.protoMessage(EncryptedDek.getDefaultInstance()))
          set("MaxEventDate").to(com.google.cloud.Date.fromYearMonthDay(2026, 1, 6))
          set("BlobChecksum").to(com.google.cloud.ByteArray.copyFrom(ByteArray(32)))
          set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
          if (deleted) set("DeleteTime").to(Value.COMMIT_TIMESTAMP)
        },
      )
    )
  }

  private suspend fun readModelId(model: RawImpressionUploadModelLine): Long =
    spannerDatabase.databaseClient.singleUse().use { txn ->
      txn
        .executeQuery(
          statement(
            "SELECT RawImpressionUploadModelLineId FROM RawImpressionUploadModelLine WHERE DataProviderResourceId = @provider AND RawImpressionUploadId = @upload AND RawImpressionUploadModelLineResourceId = @model"
          ) {
            bind("provider").to(testProvider)
            bind("upload").to(uploadIdsByResourceId.getValue(model.rawImpressionUploadResourceId))
            bind("model").to(model.rawImpressionUploadModelLineResourceId)
          }
        )
        .single()
        .getLong("RawImpressionUploadModelLineId")
    }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
