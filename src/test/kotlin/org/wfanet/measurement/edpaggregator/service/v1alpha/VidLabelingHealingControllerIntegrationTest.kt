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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.type.date
import com.google.type.interval
import java.time.Clock
import java.time.Duration
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
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.ListModelLinesRequest
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.listModelLinesResponse
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.InternalApiServices
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLeaseServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidateServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.acquireDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRankIndexBlobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadModelLineRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadModelLineRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineCompletedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLinePoolAssigningRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineRankingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadRegistrationCompleteRequest
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.releaseDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingStep
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys
import org.wfanet.measurement.edpaggregator.vidlabeling.RawImpressionBlobMetadata
import org.wfanet.measurement.edpaggregator.vidlabeling.RawImpressionUploadManifestClassifier
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatchSequencer
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatcher
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.CorrectionCandidateCleaner
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.CorrectionManifestReader
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.DoneBlobReplayer
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.EvictUploader
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.RecoverUploader
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.VidLabelingHealingController
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt as InternalCorrectionCandidateServiceGrpcKt
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class VidLabelingHealingControllerIntegrationTest {
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
  private val candidatesService =
    mockService<
      RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineImplBase
    >()
  private val operationsService =
    mockService<
      UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase
    >()
  private val controllerServer = GrpcTestServerRule {
    addService(candidatesService)
    addService(operationsService)
  }

  @get:Rule
  val ruleChain: TestRule =
    chainRulesSequentially(spannerDatabase, internalServer, publicServer, controllerServer)

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
  private lateinit var dataAvailabilitySyncLeasesStub:
    DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineStub
  private lateinit var correctionDetectionStub:
    InternalCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub
  private lateinit var dispatchSequencer: VidLabelingDispatchSequencer

  private val rawStorage = InMemoryStorageClient()
  private val blobMetadata = mutableMapOf<String, RawImpressionBlobMetadata>()
  private val eventDates = mutableMapOf<String, LocalDate>()
  private val recoveryMetadataByDoneBlobUri = mutableMapOf<String, Map<String, String>>()
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
    dataAvailabilitySyncLeasesStub =
      DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineStub(channel)
    correctionDetectionStub =
      InternalCorrectionCandidateServiceGrpcKt
        .RawImpressionUploadCorrectionCandidateServiceCoroutineStub(internalServer.channel)
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
  fun `controller recovery generation is accepted by dispatcher`() = runBlocking {
    val d1 = createCompletedUpload(1)
    val d2 = createCompletedUpload(2)
    val evictUploader =
      EvictUploader(
        uploadsStub,
        modelLinesStub,
        rankIndexBlobsStub,
        filesStub,
        impressionMetadataStub,
        LABELED_OUTPUT_PREFIX,
        getBlobGeneration = { null },
        deleteBlob = { _, _ -> true },
      )
    val plan = evictUploader.plan(listOf(d1.upload.name), Instant.EPOCH)
    evictUploader.evict(plan, "invalid raw impressions")
    completeReplacement(d1, registerEdpCorrection(d1))
    val failedModelLine = getModelLine(d2.modelLine.name)
    assertThat(failedModelLine.recoveryAction)
      .isEqualTo(RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY)

    var operation = uploadHealingOperation {
      name = "$DATA_PROVIDER/uploadHealingOperations/${plan.evictionOperationId}"
      state = UploadHealingOperation.State.RECOVERING
      etag = "recovering-etag"
      steps += uploadHealingStep {
        name = "${this@uploadHealingOperation.name}/uploadHealingSteps/1"
        sourceRawImpressionUpload = d2.upload.name
        rawImpressionUploadModelLine = failedModelLine.name
        cmmsModelLine = MODEL_LINE
        memoized = true
        recoveryAction = failedModelLine.recoveryAction
        recoveryPredecessorRawImpressionUpload =
          failedModelLine.recoveryPredecessorRawImpressionUpload
        recoveryTarget = true
        state = UploadHealingStep.State.WAITING_FOR_REPLACEMENT
        etag = "step-etag"
      }
    }
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val states =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in states) uploadHealingOperations += operation
      }
    }
    whenever(operationsService.getUploadHealingOperation(any())).thenAnswer { operation }
    whenever(operationsService.advanceUploadHealingStep(any())).thenAnswer { invocation ->
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
        >(
          0
        )
      val updated =
        operation.stepsList.single().copy {
          state = UploadHealingStep.State.RECOVERY_STARTED
          recoveryDoneBlobGeneration = request.recoveryDoneBlobGeneration
          etag = "started-step-etag"
        }
      operation =
        operation.copy {
          steps[0] = updated
          etag = "started-operation-etag"
        }
      updated
    }
    val recoverUploader =
      RecoverUploader(uploadsStub, modelLinesStub, rankIndexBlobsStub, ::rewriteDoneBlob)
    var replayed: DoneBlobReplayer.Request? = null
    val controller =
      VidLabelingHealingController(
        listOf(
          VidLabelingHealingController.DataProviderConfig(
            DATA_PROVIDER,
            LABELED_OUTPUT_PREFIX,
            Duration.ofDays(30),
          )
        ),
        RawImpressionUploadCorrectionCandidateServiceGrpcKt
          .RawImpressionUploadCorrectionCandidateServiceCoroutineStub(controllerServer.channel),
        CorrectionCandidateCleaner { _ -> },
        UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(
          controllerServer.channel
        ),
        uploadsStub,
        filesStub,
        modelLinesStub,
        rankIndexBlobsStub,
        plannerFactory = { error("unexpected planning") },
        evictionExecutorFactory = { evictUploader },
        manifestReader = manifestReader(d2),
        doneBlobReplayerFactory = {
          DoneBlobReplayer { request ->
            replayed = request
            registerCurrentDoneBlob(d2)
            Unit
          }
        },
        recoveryExecutorFactory = { recoverUploader },
      )

    val result = controller.run()

    assertThat(result.failedDataProviders).isEqualTo(0)
    val recoveryGeneration = operation.stepsList.single().recoveryDoneBlobGeneration
    assertThat(recoveryGeneration).isNotEqualTo(d2.upload.doneBlobGeneration)
    val replay = checkNotNull(replayed)
    assertThat(replay.doneBlobUri).isEqualTo(d2.upload.doneBlobUri)
    assertThat(replay.doneBlobGeneration).isEqualTo(recoveryGeneration)
    assertThat(replay.sourceRawImpressionUpload).isEqualTo(d2.upload.name)
    assertThat(replay.cmmsModelLines).containsExactly(MODEL_LINE)
    assertThat(replay.uploadHealingOperation).isEqualTo(operation.name)
    val replacement =
      listRevisions(d2.upload.doneBlobUri).single { it.doneBlobGeneration == recoveryGeneration }
    assertThat(replacement.replacesRawImpressionUpload).isEqualTo(d2.upload.name)
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
    blobMetadata[doneKey] = RawImpressionBlobMetadata(1_000L + index, 0L, doneCreateTime)
    blobMetadata[fileKey] =
      RawImpressionBlobMetadata(2_000L + index, 100L, doneCreateTime.minusSeconds(1))
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
          this.eventDate = eventDate.toProtoDate()
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
    val completedModelLine = completeModelLine(modelLine, upload.name)
    val completedUpload =
      uploadsStub.getRawImpressionUpload(getRawImpressionUploadRequest { name = upload.name })
    val outputUri =
      LabeledImpressionsBlobKeys.forInputUri(
        LABELED_OUTPUT_PREFIX,
        fileBlobUri,
        MODEL_LINE,
        eventDate,
      )
    val synchronizationAttemptId = UUID.randomUUID().toString()
    val lease =
      dataAvailabilitySyncLeasesStub.acquireDataAvailabilitySyncLease(
        acquireDataAvailabilitySyncLeaseRequest {
          name = "$DATA_PROVIDER/dataAvailabilitySyncLeases/$synchronizationAttemptId"
          requestId = UUID.randomUUID().toString()
        }
      )
    impressionMetadataStub.createImpressionMetadata(
      createImpressionMetadataRequest {
        parent = DATA_PROVIDER
        dataAvailabilitySyncLease = lease.name
        impressionMetadata = impressionMetadata {
          blobUri = outputUri + METADATA_SUFFIX
          blobTypeUrl = "type.googleapis.com/wfa.measurement.LabeledImpressionsMetadata"
          eventGroupReferenceId = "event-group-$index"
          this.modelLine = MODEL_LINE
          interval = interval {
            startTime = doneCreateTime.toProtoTime()
            endTime = doneCreateTime.plusSeconds(1).toProtoTime()
          }
        }
        requestId = UUID.randomUUID().toString()
      }
    )
    dataAvailabilitySyncLeasesStub.releaseDataAvailabilitySyncLease(
      releaseDataAvailabilitySyncLeaseRequest {
        name = lease.name
        etag = lease.etag
        requestId = UUID.randomUUID().toString()
      }
    )
    return UploadFixture(completedUpload, completedModelLine, doneKey, fileKey, eventDate)
  }

  private suspend fun registerEdpCorrection(source: UploadFixture): RawImpressionUpload {
    rewriteDoneBlob(source.upload.doneBlobUri, source.upload.doneBlobGeneration, emptyMap())
    return registerCurrentDoneBlob(source)
  }

  private suspend fun registerCurrentDoneBlob(source: UploadFixture): RawImpressionUpload {
    val recoveryMetadata = recoveryMetadataByDoneBlobUri[source.upload.doneBlobUri].orEmpty()
    val dispatcher =
      VidLabelingDispatcher(
        storageClient = rawStorage,
        rawImpressionUploadStub = uploadsStub,
        rawImpressionUploadFilesStub = filesStub,
        correctionDetectionStub = correctionDetectionStub,
        rawImpressionUploadModelLineStub = modelLinesStub,
        rankIndexBlobStub = rankIndexBlobsStub,
        modelLinesStub = ModelLinesGrpcKt.ModelLinesCoroutineStub(publicServer.channel),
        dispatchSequencer = dispatchSequencer,
        dataProviderName = DATA_PROVIDER,
        modelSuiteName = MODEL_SUITE,
        overrideModelLines =
          recoveryMetadata[WatchedBlobs.OVERRIDE_MODEL_LINES_KEY]
            ?.split(',')
            ?.filter(String::isNotEmpty)
            .orEmpty(),
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

  private suspend fun completeReplacement(source: UploadFixture, replacement: RawImpressionUpload) {
    val modelLine =
      modelLinesStub
        .listRawImpressionUploadModelLines(
          listRawImpressionUploadModelLinesRequest { parent = replacement.name }
        )
        .rawImpressionUploadModelLinesList
        .single()
    completeModelLine(modelLine, replacement.name)
  }

  private suspend fun completeModelLine(
    created: RawImpressionUploadModelLine,
    uploadName: String,
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
    createSnapshot(uploadName)
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

  private suspend fun createSnapshot(uploadName: String): RankIndexBlob =
    rankIndexBlobsStub.createRankIndexBlob(
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
          maxEventDate = BASE_EVENT_DATE.toProtoDate()
        }
        requestId = UUID.randomUUID().toString()
      }
    )

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
    blobMetadata[doneKey] = RawImpressionBlobMetadata(newGeneration, 0L, nextDoneCreateTime)
    nextDoneCreateTime = nextDoneCreateTime.plusSeconds(1)
    recoveryMetadataByDoneBlobUri[doneBlobUri] = metadata
    return newGeneration
  }

  private fun manifestReader(source: UploadFixture): CorrectionManifestReader =
    CorrectionManifestReader { doneBlobUri, doneBlobGeneration ->
      check(doneBlobUri == source.upload.doneBlobUri)
      check(blobMetadata.getValue(source.doneKey).generation == doneBlobGeneration)
      listOf(
        RawImpressionUploadManifestClassifier.File(
          "$RAW_INPUT_PREFIX/${source.rawFileKey}",
          blobMetadata.getValue(source.rawFileKey).generation,
          source.eventDate.toProtoDate(),
        )
      )
    }

  private suspend fun listRevisions(doneBlobUri: String): List<RawImpressionUpload> =
    uploadsStub
      .listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          parent = DATA_PROVIDER
          filter = ListRawImpressionUploadsRequestKt.filter { this.doneBlobUri = doneBlobUri }
        }
      )
      .rawImpressionUploadsList

  private suspend fun getModelLine(name: String): RawImpressionUploadModelLine =
    modelLinesStub.getRawImpressionUploadModelLine(
      getRawImpressionUploadModelLineRequest { this.name = name }
    )

  private fun LocalDate.toProtoDate() = date {
    year = this@toProtoDate.year
    month = this@toProtoDate.monthValue
    day = this@toProtoDate.dayOfMonth
  }

  private data class UploadFixture(
    val upload: RawImpressionUpload,
    val modelLine: RawImpressionUploadModelLine,
    val doneKey: String,
    val rawFileKey: String,
    val eventDate: LocalDate,
  )

  companion object {
    private val spannerEmulator = SpannerEmulatorRule()

    @get:ClassRule @JvmStatic val classRule: TestRule = spannerEmulator

    private const val DATA_PROVIDER = "dataProviders/dp"
    private const val MODEL_SUITE = "modelProviders/mp/modelSuites/ms"
    private const val MODEL_LINE = "$MODEL_SUITE/modelLines/ml"
    private const val RAW_INPUT_PREFIX = "gs://raw-input"
    private const val LABELED_OUTPUT_PREFIX = "gs://labeled-output"
    private const val METADATA_SUFFIX = ".metadata.binpb"
    private val BASE_TIME = Instant.parse("2026-01-01T00:00:00Z")
    private val NOW = BASE_TIME.plusSeconds(10_000)
    private val BASE_EVENT_DATE = LocalDate.of(2026, 1, 1)
    private val ACTIVE_MODEL_LINE: ModelLine = modelLine {
      name = MODEL_LINE
      activeStartTime = BASE_TIME.minusSeconds(100).toProtoTime()
      activeEndTime = NOW.plusSeconds(100).toProtoTime()
    }
  }
}
