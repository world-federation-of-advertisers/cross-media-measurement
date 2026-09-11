// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.type.date
import com.google.type.interval
import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.UUID
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.v2alpha.ListModelLinesRequest
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.listModelLinesResponse
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.InternalApiServices
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper
import org.wfanet.measurement.edpaggregator.tools.EvictUploader
import org.wfanet.measurement.edpaggregator.tools.RecoverUploader
import org.wfanet.measurement.edpaggregator.tools.UploadHealingWorkflow
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.batchUndeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRankIndexBlobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadModelLineRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.getImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadModelLineRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineCompletedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLinePoolAssigningRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineRankingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadRegistrationCompleteRequest
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys
import org.wfanet.measurement.edpaggregator.vidlabeling.RawImpressionBlobMetadata
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatchSequencer
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatcher
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class UploadHealingWorkflowIntegrationTest {
  private val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)
  private val internalServer = GrpcTestServerRule {
    InternalApiServices.build(spannerDatabase.databaseClient, EmptyCoroutineContext)
      .toList()
      .forEach { addService(it) }
  }
  private val modelLinesService =
    object : ModelLinesGrpcKt.ModelLinesCoroutineImplBase() {
      override suspend fun listModelLines(request: ListModelLinesRequest) = listModelLinesResponse {
        modelLines += ACTIVE_MODEL_LINE
      }
    }
  private val publicServer = GrpcTestServerRule {
    Services.build(internalServer.channel).toList().forEach { addService(it) }
    addService(modelLinesService)
  }

  @get:Rule
  val ruleChain: TestRule = chainRulesSequentially(spannerDatabase, internalServer, publicServer)

  private lateinit var uploadsStub:
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
  private lateinit var filesStub:
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
  private lateinit var modelLinesStub:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
  private lateinit var rankIndexBlobsStub:
    RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
  private lateinit var impressionMetadataStub:
    ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
  private lateinit var operationsStub:
    UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub
  private lateinit var dispatchSequencer: VidLabelingDispatchSequencer

  private val rawStorage = InMemoryStorageClient()
  private val blobMetadata = mutableMapOf<String, RawImpressionBlobMetadata>()
  private val eventDates = mutableMapOf<String, LocalDate>()
  private val outputBlobUris = mutableSetOf<String>()
  private val recoveryMetadataByDoneBlobUri = mutableMapOf<String, Map<String, String>>()
  private val recoveredSources = mutableListOf<String>()
  private var nextDoneGeneration = 900L
  private var nextDoneCreateTime = BASE_TIME.plusSeconds(100)

  @Before
  fun setUp() {
    val channel = publicServer.channel
    uploadsStub = RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub(channel)
    filesStub =
      RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(channel)
    modelLinesStub =
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
        channel
      )
    rankIndexBlobsStub = RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub(channel)
    impressionMetadataStub =
      ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub(channel)
    operationsStub =
      UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(channel)
    dispatchSequencer = mock()
    dispatchSequencer.stub {
      onBlocking { resolveShardInfo(any()) } doReturn
        VidLabelingDispatchSequencer.ResolvedShardInfo(
          modelBlobPath = "gs://models/model",
          memoizationEnabled = true,
        )
      onBlocking { dispatchNext() } doReturn
        VidLabelingDispatchSequencer.DispatchResult(dispatchedUpload = null, queuedUploads = 0)
    }
  }

  @Test
  fun `healing workflow recovers noncontiguous uploads in chronological order`() = runBlocking {
    val uploads = (1..5).map { createCompletedUpload(it) }
    val d1 = uploads[0]
    val d2 = uploads[1]
    val d3 = uploads[2]
    val d4 = uploads[3]
    val d5 = uploads[4]
    val evictUploader =
      EvictUploader(
        uploadsStub,
        modelLinesStub,
        rankIndexBlobsStub,
        filesStub,
        impressionMetadataStub,
        LABELED_OUTPUT_PREFIX,
        deleteBlob = { outputBlobUris.remove(it) },
      )
    val recoverUploader =
      RecoverUploader(
        uploadsStub,
        modelLinesStub,
        rankIndexBlobsStub,
        rewriteDoneBlob = ::rewriteDoneBlob,
      )
    val workflow =
      UploadHealingWorkflow(
        operationsStub,
        uploadsStub,
        modelLinesStub,
        rankIndexBlobsStub,
        evictUploader,
        recoverUploader,
      )

    val plan = evictUploader.plan(listOf(d2.upload.name, d4.upload.name), Instant.EPOCH)

    assertThat(plan.cascade.map { it.uploadName })
      .containsExactly(d2.upload.name, d3.upload.name, d4.upload.name, d5.upload.name)
      .inOrder()
    val started = workflow.start(plan, "invalid raw impressions", LABELED_OUTPUT_PREFIX)

    assertThat(started.nextAction).contains(d2.upload.name)
    assertThat(started.evictionResult!!.deletedSnapshots).isEqualTo(4)
    assertThat(started.evictionResult!!.deletedImpressionMetadata).isEqualTo(4)
    assertThat(started.evictionResult!!.deletedOutputBlobs).isEqualTo(8)
    for (source in listOf(d2, d3, d4, d5)) {
      val failed = getModelLine(source.modelLine.name)
      assertThat(failed.state).isEqualTo(RawImpressionUploadModelLine.State.FAILED)
      assertThat(failed.failureReason)
        .isEqualTo(RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT)
      assertThat(getMetadata(source.metadata.name).state)
        .isEqualTo(ImpressionMetadata.State.DELETED)
      assertThat(listSnapshots(source.upload.name, showDeleted = true).single().hasDeleteTime())
        .isTrue()
      assertThat(outputBlobUris.intersect(source.outputBlobUris)).isEmpty()
    }

    val d2Replacement = completeReplacement(d2, registerEdpCorrection(d2))
    val afterD2 = workflow.resume(started.operation.name)

    assertThat(afterD2.nextAction).contains(d3.upload.name)
    assertThat(recoveredSources).containsExactly(d3.upload.name)

    val d3Replacement = completeReplacement(d3, registerRecovery(d3))
    val afterD3 = workflow.resume(started.operation.name)

    assertThat(afterD3.nextAction).contains(d4.upload.name)

    val d4Replacement = completeReplacement(d4, registerEdpCorrection(d4))
    val afterD4 = workflow.resume(started.operation.name)

    assertThat(afterD4.nextAction).contains(d5.upload.name)
    assertThat(recoveredSources).containsExactly(d3.upload.name, d5.upload.name).inOrder()

    val d5Replacement = completeReplacement(d5, registerRecovery(d5))
    val completed = workflow.resume(started.operation.name)

    assertThat(completed.operation.state).isEqualTo(UploadHealingOperation.State.COMPLETE)
    assertThat(
        completed.operation.stepsList.associate {
          it.sourceRawImpressionUpload to it.replacementRawImpressionUpload
        }
      )
      .containsExactly(
        d2.upload.name,
        d2Replacement.upload.name,
        d3.upload.name,
        d3Replacement.upload.name,
        d4.upload.name,
        d4Replacement.upload.name,
        d5.upload.name,
        d5Replacement.upload.name,
      )
    for (replacement in listOf(d2Replacement, d3Replacement, d4Replacement, d5Replacement)) {
      assertThat(replacement.upload.state).isEqualTo(RawImpressionUpload.State.COMPLETED)
      assertThat(replacement.modelLine.state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(listSnapshots(replacement.upload.name)).hasSize(1)
    }
    for (source in listOf(d1, d2, d3, d4, d5)) {
      assertThat(getMetadata(source.metadata.name).state).isEqualTo(ImpressionMetadata.State.ACTIVE)
      assertThat(outputBlobUris).containsAtLeastElementsIn(source.outputBlobUris)
    }
    Unit
  }

  private suspend fun createCompletedUpload(index: Int): UploadFixture {
    val doneKey = "day-$index/done"
    val doneBlobUri = "$RAW_INPUT_PREFIX/$doneKey"
    val fileKey = "day-$index/raw-impressions.parquet"
    val fileBlobUri = "$RAW_INPUT_PREFIX/$fileKey"
    val eventDate = BASE_EVENT_DATE.plusDays(index.toLong() - 1L)
    val doneCreateTime = BASE_TIME.plusSeconds(index.toLong())
    rawStorage.writeBlob(doneKey, ByteString.copyFromUtf8("done-$index"))
    rawStorage.writeBlob(fileKey, ByteString.copyFromUtf8("raw-$index"))
    blobMetadata[doneKey] =
      RawImpressionBlobMetadata(
        generation = 1_000L + index,
        sizeBytes = 0L,
        createTime = doneCreateTime,
      )
    blobMetadata[fileKey] =
      RawImpressionBlobMetadata(
        generation = 2_000L + index,
        sizeBytes = 100L,
        createTime = doneCreateTime.minusSeconds(1),
      )
    eventDates[fileKey] = eventDate
    val upload =
      uploadsStub.createRawImpressionUpload(
        createRawImpressionUploadRequest {
          parent = DATA_PROVIDER
          rawImpressionUpload = rawImpressionUpload {
            this.doneBlobUri = doneBlobUri
            doneBlobGeneration = blobMetadata.getValue(doneKey).generation
            doneBlobCreateTime = doneCreateTime.toProtoTime()
          }
          requestId = UUID.randomUUID().toString()
        }
      )
    filesStub.createRawImpressionUploadFile(
      createRawImpressionUploadFileRequest {
        parent = upload.name
        rawImpressionUploadFile = rawImpressionUploadFile {
          blobUri = fileBlobUri
          blobGeneration = blobMetadata.getValue(fileKey).generation
          sizeBytes = 100L
          this.eventDate = date {
            year = eventDate.year
            month = eventDate.monthValue
            day = eventDate.dayOfMonth
          }
        }
        requestId = UUID.randomUUID().toString()
      }
    )
    val modelLine =
      modelLinesStub.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          parent = upload.name
          rawImpressionUploadModelLine = rawImpressionUploadModelLine { cmmsModelLine = MODEL_LINE }
          requestId = UUID.randomUUID().toString()
        }
      )
    uploadsStub.markRawImpressionUploadRegistrationComplete(
      markRawImpressionUploadRegistrationCompleteRequest {
        name = upload.name
        etag = upload.etag
        requestId = UUID.randomUUID().toString()
      }
    )
    val completedModelLine = completeModelLine(modelLine)
    val completedUpload =
      uploadsStub.getRawImpressionUpload(getRawImpressionUploadRequest { name = upload.name })
    createSnapshot(completedUpload.name)
    val outputUri =
      LabeledImpressionsBlobKeys.forInputUri(
        LABELED_OUTPUT_PREFIX,
        fileBlobUri,
        MODEL_LINE,
        eventDate,
      )
    val outputBlobUris = setOf(outputUri, outputUri + METADATA_SUFFIX)
    this.outputBlobUris += outputBlobUris
    val metadata =
      impressionMetadataStub.createImpressionMetadata(
        createImpressionMetadataRequest {
          parent = DATA_PROVIDER
          impressionMetadata = impressionMetadata {
            blobUri = outputUri + METADATA_SUFFIX
            blobTypeUrl = "type.googleapis.com/wfa.measurement.LabeledImpressionsMetadata"
            eventGroupReferenceId = "event-group-$index"
            this.modelLine = MODEL_LINE
            interval = interval {
              startTime = BASE_TIME.plusSeconds(index.toLong()).toProtoTime()
              endTime = BASE_TIME.plusSeconds(index.toLong() + 1L).toProtoTime()
            }
          }
          requestId = UUID.randomUUID().toString()
        }
      )
    return UploadFixture(
      upload = completedUpload,
      modelLine = completedModelLine,
      metadata = metadata,
      doneKey = doneKey,
      outputBlobUris = outputBlobUris,
    )
  }

  private suspend fun registerEdpCorrection(source: UploadFixture): RawImpressionUpload {
    rewriteDoneBlob(source.upload.doneBlobUri, source.upload.doneBlobGeneration, emptyMap())
    return registerCurrentDoneBlob(source)
  }

  private suspend fun registerRecovery(source: UploadFixture): RawImpressionUpload {
    assertThat(recoveryMetadataByDoneBlobUri).containsKey(source.upload.doneBlobUri)
    return registerCurrentDoneBlob(source)
  }

  private suspend fun registerCurrentDoneBlob(source: UploadFixture): RawImpressionUpload {
    val recoveryMetadata = recoveryMetadataByDoneBlobUri[source.upload.doneBlobUri].orEmpty()
    val overrideModelLines =
      recoveryMetadata[WatchedBlobs.OVERRIDE_MODEL_LINES_KEY]
        ?.split(',')
        ?.filter { it.isNotEmpty() }
        .orEmpty()
    val dispatcher =
      VidLabelingDispatcher(
        storageClient = rawStorage,
        rawImpressionUploadStub = uploadsStub,
        rawImpressionUploadFilesStub = filesStub,
        rawImpressionUploadModelLineStub = modelLinesStub,
        rankIndexBlobStub = rankIndexBlobsStub,
        modelLinesStub = ModelLinesGrpcKt.ModelLinesCoroutineStub(publicServer.channel),
        dispatchSequencer = dispatchSequencer,
        dataProviderName = DATA_PROVIDER,
        modelSuiteName = MODEL_SUITE,
        overrideModelLines = overrideModelLines,
        recoverySourceUpload = recoveryMetadata[WatchedBlobs.RECOVERY_SOURCE_UPLOAD_KEY],
        recoveryOperationId = recoveryMetadata[WatchedBlobs.EVICTION_OPERATION_ID_KEY],
        modelLineConfigs =
          mapOf(MODEL_LINE to VidLabelerParams.ModelLineConfig.getDefaultInstance()),
        readEventDate = { generationMatchedBlobUri ->
          val fileKey =
            generationMatchedBlobUri.substringAfter("/.wfa-generation-match/").substringAfter('/')
          eventDates.getValue(fileKey)
        },
        readBlobMetadata = { blobMetadata.getValue(it) },
        rpcThrottlers = VidLabelingRpcThrottlersTestHelper.alwaysReady(),
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
      )
    val generation = blobMetadata.getValue(source.doneKey).generation
    dispatcher.upload(source.upload.doneBlobUri, generation)
    return listRevisions(source.upload.doneBlobUri).single {
      it.doneBlobGeneration == generation && it.name != source.upload.name
    }
  }

  private suspend fun completeReplacement(
    source: UploadFixture,
    replacement: RawImpressionUpload,
  ): ReplacementFixture {
    val createdModelLine =
      modelLinesStub
        .listRawImpressionUploadModelLines(
          listRawImpressionUploadModelLinesRequest { parent = replacement.name }
        )
        .rawImpressionUploadModelLinesList
        .single()
    val completedModelLine = completeModelLine(createdModelLine)
    createSnapshot(replacement.name)
    impressionMetadataStub.batchUndeleteImpressionMetadata(
      batchUndeleteImpressionMetadataRequest {
        parent = DATA_PROVIDER
        names += source.metadata.name
      }
    )
    outputBlobUris += source.outputBlobUris
    val completedUpload =
      uploadsStub.getRawImpressionUpload(getRawImpressionUploadRequest { name = replacement.name })
    return ReplacementFixture(completedUpload, completedModelLine)
  }

  private suspend fun completeModelLine(
    created: RawImpressionUploadModelLine
  ): RawImpressionUploadModelLine {
    val poolAssigning =
      modelLinesStub.markRawImpressionUploadModelLinePoolAssigning(
        markRawImpressionUploadModelLinePoolAssigningRequest {
          name = created.name
          etag = created.etag
          requestId = UUID.randomUUID().toString()
        }
      )
    val ranking =
      modelLinesStub.markRawImpressionUploadModelLineRanking(
        markRawImpressionUploadModelLineRankingRequest {
          name = poolAssigning.name
          etag = poolAssigning.etag
          requestId = UUID.randomUUID().toString()
        }
      )
    val labeling =
      modelLinesStub.markRawImpressionUploadModelLineLabeling(
        markRawImpressionUploadModelLineLabelingRequest {
          name = ranking.name
          etag = ranking.etag
          requestId = UUID.randomUUID().toString()
        }
      )
    return modelLinesStub.markRawImpressionUploadModelLineCompleted(
      markRawImpressionUploadModelLineCompletedRequest {
        name = labeling.name
        etag = labeling.etag
        requestId = UUID.randomUUID().toString()
      }
    )
  }

  private suspend fun createSnapshot(uploadName: String): RankIndexBlob {
    return rankIndexBlobsStub.createRankIndexBlob(
      createRankIndexBlobRequest {
        parent = uploadName
        rankIndexBlob = rankIndexBlob {
          blobType = RankIndexBlob.BlobType.SNAPSHOT
          cmmsModelLine = MODEL_LINE
          poolOffset = 0L
          blobUri = "gs://rank-index/${uploadName.substringAfterLast('/')}"
          blobChecksum = ByteString.copyFromUtf8("checksum")
          encryptedDek = encryptedDek {
            kekUri = "kms://kek"
            ciphertext = ByteString.copyFromUtf8("ciphertext")
          }
          maxEventDate = date {
            year = BASE_EVENT_DATE.year
            month = BASE_EVENT_DATE.monthValue
            day = BASE_EVENT_DATE.dayOfMonth
          }
        }
        requestId = UUID.randomUUID().toString()
      }
    )
  }

  private suspend fun rewriteDoneBlob(
    doneBlobUri: String,
    expectedGeneration: Long,
    metadata: Map<String, String>,
  ): Long {
    val doneKey = doneBlobUri.removePrefix("$RAW_INPUT_PREFIX/")
    val current = blobMetadata.getValue(doneKey)
    check(current.generation == expectedGeneration)
    val newGeneration = nextDoneGeneration--
    rawStorage.writeBlob(doneKey, ByteString.copyFromUtf8("done-$newGeneration"))
    blobMetadata[doneKey] =
      RawImpressionBlobMetadata(
        generation = newGeneration,
        sizeBytes = 0L,
        createTime = nextDoneCreateTime,
      )
    nextDoneCreateTime = nextDoneCreateTime.plusSeconds(1)
    recoveryMetadataByDoneBlobUri[doneBlobUri] = metadata
    metadata[WatchedBlobs.RECOVERY_SOURCE_UPLOAD_KEY]?.let { recoveredSources += it }
    return newGeneration
  }

  private suspend fun listRevisions(doneBlobUri: String): List<RawImpressionUpload> {
    return uploadsStub
      .listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          parent = DATA_PROVIDER
          filter = ListRawImpressionUploadsRequestKt.filter { this.doneBlobUri = doneBlobUri }
        }
      )
      .rawImpressionUploadsList
  }

  private suspend fun getModelLine(name: String): RawImpressionUploadModelLine =
    modelLinesStub.getRawImpressionUploadModelLine(
      getRawImpressionUploadModelLineRequest { this.name = name }
    )

  private suspend fun getMetadata(name: String): ImpressionMetadata =
    impressionMetadataStub.getImpressionMetadata(getImpressionMetadataRequest { this.name = name })

  private suspend fun listSnapshots(
    uploadName: String,
    showDeleted: Boolean = false,
  ): List<RankIndexBlob> =
    rankIndexBlobsStub
      .listRankIndexBlobs(
        listRankIndexBlobsRequest {
          parent = uploadName
          this.showDeleted = showDeleted
          filter =
            ListRankIndexBlobsRequestKt.filter {
              blobType = RankIndexBlob.BlobType.SNAPSHOT
              cmmsModelLine = MODEL_LINE
            }
        }
      )
      .rankIndexBlobsList

  private data class UploadFixture(
    val upload: RawImpressionUpload,
    val modelLine: RawImpressionUploadModelLine,
    val metadata: ImpressionMetadata,
    val doneKey: String,
    val outputBlobUris: Set<String>,
  )

  private data class ReplacementFixture(
    val upload: RawImpressionUpload,
    val modelLine: RawImpressionUploadModelLine,
  )

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private const val DATA_PROVIDER = "dataProviders/dp"
    private const val MODEL_SUITE = "modelProviders/mp/modelSuites/ms"
    private const val MODEL_LINE = "$MODEL_SUITE/modelLines/ml"
    private const val RAW_INPUT_PREFIX = "gs://input"
    private const val LABELED_OUTPUT_PREFIX = "gs://output/vid"
    private const val METADATA_SUFFIX = ".metadata.binpb"
    private val NOW = Instant.parse("2026-01-10T00:00:00Z")
    private val BASE_TIME = Instant.parse("2026-01-01T00:00:00Z")
    private val BASE_EVENT_DATE = LocalDate.parse("2026-01-01")
    private val ACTIVE_MODEL_LINE: ModelLine = modelLine {
      name = MODEL_LINE
      type = ModelLine.Type.PROD
      activeStartTime = NOW.minusSeconds(86_400L).toProtoTime()
      activeEndTime = NOW.plusSeconds(86_400L).toProtoTime()
    }
  }
}
