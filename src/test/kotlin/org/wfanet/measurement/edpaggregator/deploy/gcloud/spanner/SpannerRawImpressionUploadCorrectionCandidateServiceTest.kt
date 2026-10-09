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

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.timestamp
import com.google.type.Date
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getVidLabelingEvictionFence
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.ActivateQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.AdvanceRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadCorrectionCandidatesRequestKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.VidLabelingEvictionFenceState
import org.wfanet.measurement.internal.edpaggregator.activateQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.advanceRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.createQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.createRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.getRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.internal.edpaggregator.purgeExpiredRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUpload
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.internal.edpaggregator.registerDetectedRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.resolveDetectedRawImpressionUploadCorrectionCandidateRequest

@RunWith(JUnit4::class)
class SpannerRawImpressionUploadCorrectionCandidateServiceTest {
  private val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  private val service by lazy {
    SpannerRawImpressionUploadCorrectionCandidateService(spannerDatabase.databaseClient)
  }
  private val mixedManifestComparison =
    manifestComparison(
      RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_MIXED,
      UPLOAD_ID,
    )
  private val priorDigest = manifestDigest(mixedManifestComparison.priorManifestList)
  private val currentDigest = manifestDigest(mixedManifestComparison.currentManifestList)
  private val grpcServer = GrpcTestServerRule {
    InternalApiServices.build(spannerDatabase.databaseClient, EmptyCoroutineContext)
      .toList()
      .forEach { addService(it) }
  }

  @get:Rule val ruleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcServer)

  private val grpcStub by lazy {
    RawImpressionUploadCorrectionCandidateServiceGrpcKt
      .RawImpressionUploadCorrectionCandidateServiceCoroutineStub(grpcServer.channel)
  }

  @Test
  fun `registered gRPC service creates quarantined upload and fence idempotently`() = runBlocking {
    val request = createQuarantinedRawImpressionUploadRequest {
      dataProviderResourceId = DATA_PROVIDER_ID
      rawImpressionUpload = rawImpressionUpload {
        doneBlobUri = SHARED_DONE_BLOB_URI
        doneBlobGeneration = 2L
        doneBlobCreateTime = timestamp { seconds = 2L }
      }
      rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
      requestId = CREATE_REQUEST_ID
    }

    val first = grpcStub.createQuarantinedRawImpressionUpload(request)
    val replay = grpcStub.createQuarantinedRawImpressionUpload(request)

    assertThat(replay).isEqualTo(first)
    assertThat(first.processingDeferred).isTrue()
    assertThat(first.correctionCandidateId).isEqualTo(CANDIDATE_ID)
    val fence =
      spannerDatabase.databaseClient.singleUse().use {
        it.getVidLabelingEvictionFence(DATA_PROVIDER_ID)
      }
    assertThat(fence!!.evictionOperationId).isEqualTo(CANDIDATE_ID)
    assertThat(fence.state)
      .isEqualTo(VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING)
  }

  @Test
  fun `activation authorizes approved candidate and reuses persisted upload`() = runBlocking {
    val quarantined = createAuthorizedQuarantine()
    val request = activationRequest(quarantined.rawImpressionUploadResourceId)

    val activated = grpcStub.activateQuarantinedRawImpressionUpload(request)
    val replay = grpcStub.activateQuarantinedRawImpressionUpload(request)

    assertThat(replay).isEqualTo(activated)
    assertThat(activated.rawImpressionUploadResourceId)
      .isEqualTo(quarantined.rawImpressionUploadResourceId)
    assertThat(activated.state)
      .isEqualTo(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED)
    assertThat(activated.registrationComplete).isFalse()
    assertThat(activated.processingDeferred).isFalse()
    assertThat(activated.evictionOperationId).isEqualTo(OPERATION_ID)
  }

  @Test
  fun `activation rejects no-replacement decision`() = runBlocking {
    val quarantined = createAuthorizedQuarantine()
    setCandidateHealing(
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT
    )

    val error =
      assertFailsWith<StatusException> {
        grpcStub.activateQuarantinedRawImpressionUpload(
          activationRequest(quarantined.rawImpressionUploadResourceId)
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `activation rejects wrong source dependency`() = runBlocking {
    val quarantined = createAuthorizedQuarantine()

    val error =
      assertFailsWith<StatusException> {
        grpcStub.activateQuarantinedRawImpressionUpload(
          activationRequest(quarantined.rawImpressionUploadResourceId, sourceUploadId = UPLOAD_ID_2)
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `activation rejects operation without the fence`() = runBlocking {
    val quarantined = createAuthorizedQuarantine()
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("VidLabelingEvictionFence")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("EvictionOperationId")
          .to(CANDIDATE_ID)
          .build()
      )
    )

    val error =
      assertFailsWith<StatusException> {
        grpcStub.activateQuarantinedRawImpressionUpload(
          activationRequest(quarantined.rawImpressionUploadResourceId)
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `register atomically finalizes quarantine and is idempotent`() = runBlocking {
    insertRawUpload(
      1L,
      UPLOAD_ID,
      RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED,
      registrationComplete = false,
    )
    insertEvictionFence(CANDIDATE_ID)

    val first = service.registerDetectedRawImpressionUploadCorrectionCandidate(registerRequest())
    val second = service.registerDetectedRawImpressionUploadCorrectionCandidate(registerRequest())

    assertThat(first.newlyCreated).isTrue()
    assertThat(second.newlyCreated).isFalse()
    val upload =
      spannerDatabase.databaseClient.singleUse().use {
        it.getRawImpressionUploadByResourceId(DATA_PROVIDER_ID, UPLOAD_ID).rawImpressionUpload
      }
    assertThat(upload.registrationComplete).isTrue()
    assertThat(upload.state)
      .isEqualTo(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED)
  }

  @Test
  fun `resolve supersedes pending candidate and releases deferred uploads`() = runBlocking {
    insertRawUpload(
      1L,
      UPLOAD_ID,
      RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
    )
    insertRawUpload(
      2L,
      UPLOAD_ID_2,
      RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED,
      processingDeferred = true,
    )
    service.createRawImpressionUploadCorrectionCandidate(createRequest())
    insertEvictionFence(CANDIDATE_ID)
    val request = resolveDetectedRawImpressionUploadCorrectionCandidateRequest {
      dataProviderResourceId = DATA_PROVIDER_ID
      rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
      requestId = java.util.UUID.randomUUID().toString()
    }

    val first = service.resolveDetectedRawImpressionUploadCorrectionCandidate(request)
    val second = service.resolveDetectedRawImpressionUploadCorrectionCandidate(request)

    assertThat(first.newlyResolved).isTrue()
    assertThat(second.newlyResolved).isFalse()
    assertThat(getCandidate(CANDIDATE_ID).state)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED)
    val fence =
      spannerDatabase.databaseClient.singleUse().use {
        it.getVidLabelingEvictionFence(DATA_PROVIDER_ID)
      }
    assertThat(fence).isNull()
    val deferredUpload =
      spannerDatabase.databaseClient.singleUse().use {
        it.getRawImpressionUploadByResourceId(DATA_PROVIDER_ID, UPLOAD_ID_2).rawImpressionUpload
      }
    assertThat(deferredUpload.processingDeferred).isFalse()

    val successorUploadId = "resolved-successor"
    insertRawUpload(
      3L,
      successorUploadId,
      RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED,
      doneBlobUri = "gs://bucket/$UPLOAD_ID/done",
      registrationComplete = false,
    )
    insertEvictionFence(CANDIDATE_IDS[1])
    val successor =
      service.registerDetectedRawImpressionUploadCorrectionCandidate(
        registerRequest(
          CANDIDATE_IDS[1],
          successorUploadId,
          CREATE_REQUEST_IDS[1],
          supersededCandidateId = CANDIDATE_ID,
        )
      )
    assertThat(successor.newlyCreated).isTrue()
  }

  @Test
  fun `create persists candidate`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )

      val created = service.createRawImpressionUploadCorrectionCandidate(createRequest())
      val fetched =
        service.getRawImpressionUploadCorrectionCandidate(
          getRawImpressionUploadCorrectionCandidateRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
          }
        )

      assertThat(created).isEqualTo(fetched)
      assertThat(created.dataProviderResourceId).isEqualTo(DATA_PROVIDER_ID)
      assertThat(created.rawImpressionUploadCorrectionCandidateId).isEqualTo(CANDIDATE_ID)
      assertThat(created.rawImpressionUploadResourceId).isEqualTo(UPLOAD_ID)
      assertThat(created.classification)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_MIXED)
      assertThat(created.priorManifestDigest).isEqualTo(priorDigest)
      assertThat(created.currentManifestDigest).isEqualTo(currentDigest)
      assertThat(created.manifestComparison).isEqualTo(mixedManifestComparison)
      assertThat(created.state)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_PENDING)
      assertThat(created.decision)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.Decision.DECISION_UNSPECIFIED)
      assertThat(created.hasCreateTime()).isTrue()
      assertThat(created.hasUpdateTime()).isTrue()
      assertThat(created.etag).isNotEmpty()
    }

  @Test
  fun `create is idempotent`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      val request = createRequest()

      val first = service.createRawImpressionUploadCorrectionCandidate(request)
      val second = service.createRawImpressionUploadCorrectionCandidate(request)

      assertThat(second).isEqualTo(first)
    }

  @Test
  fun `create rejects reused request ID with different candidate`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      insertRawUpload(
        2L,
        UPLOAD_ID_2,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      service.createRawImpressionUploadCorrectionCandidate(createRequest())

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadCorrectionCandidate(
            createRequest(CANDIDATE_ID_2, UPLOAD_ID_2).copy { requestId = CREATE_REQUEST_ID }
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }

  @Test
  fun `create rejects upload that is not quarantined`() =
    runBlocking<Unit> {
      insertRawUpload(1L, UPLOAD_ID, RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED)

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadCorrectionCandidate(createRequest())
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `create rejects quarantined upload before manifest registration completes`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
        registrationComplete = false,
      )

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadCorrectionCandidate(createRequest())
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `create rejects quarantined upload without done creation time`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
        includeDoneBlobCreateTime = false,
      )

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadCorrectionCandidate(createRequest())
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `create rejects invalid manifest digest`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      val request =
        createRequest().copy {
          rawImpressionUploadCorrectionCandidate =
            rawImpressionUploadCorrectionCandidate.copy {
              priorManifestDigest = ByteString.copyFromUtf8("not-sha256")
            }
        }

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadCorrectionCandidate(request)
        }

      assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `create rejects a manifest comparison that does not match its digest`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      val request =
        createRequest().copy {
          rawImpressionUploadCorrectionCandidate =
            rawImpressionUploadCorrectionCandidate.copy {
              manifestComparison =
                mixedManifestComparison
                  .toBuilder()
                  .setCurrentManifest(
                    0,
                    mixedManifestComparison.currentManifestList[0]
                      .toBuilder()
                      .setBlobGeneration(3L)
                      .build(),
                  )
                  .build()
            }
        }

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadCorrectionCandidate(request)
        }

      assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `create persists an event-date change as one mixed difference`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      val priorDate = date {
        year = 2026
        month = 1
        day = 1
      }
      val currentDate = date {
        year = 2026
        month = 1
        day = 2
      }
      val comparison =
        RawImpressionUploadCorrectionCandidateKt.manifestComparison {
          val prior = manifestEntry("gs://raw/a", 1L, "prior", priorDate)
          val current = manifestEntry("gs://raw/a", 1L, UPLOAD_ID, currentDate)
          priorManifest += prior
          currentManifest += current
          differences +=
            RawImpressionUploadCorrectionCandidateKt.manifestDifference {
              type =
                RawImpressionUploadCorrectionCandidate.ManifestDifference.Type
                  .TYPE_EVENT_DATE_CHANGED
              this.prior = prior
              this.current = current
            }
        }
      val request =
        createRequest().copy {
          rawImpressionUploadCorrectionCandidate =
            rawImpressionUploadCorrectionCandidate.copy {
              classification =
                RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_MIXED
              priorManifestDigest = manifestDigest(comparison.priorManifestList)
              currentManifestDigest = manifestDigest(comparison.currentManifestList)
              manifestComparison = comparison
            }
        }

      val created = service.createRawImpressionUploadCorrectionCandidate(request)

      assertThat(created.manifestComparison.differencesList.single().type)
        .isEqualTo(
          RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.TYPE_EVENT_DATE_CHANGED
        )
    }

  @Test
  fun `create persists prior legacy manifest with unknown generation`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )

      val created =
        service.createRawImpressionUploadCorrectionCandidate(
          createRequest(
            classification =
              RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_EDITED,
            priorGeneration = 0L,
          )
        )

      assertThat(created.manifestComparison.priorManifestList.single().blobGeneration).isEqualTo(0L)
    }

  @Test
  fun `create rejects second candidate for same upload`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      service.createRawImpressionUploadCorrectionCandidate(createRequest())

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadCorrectionCandidate(
            createRequest(CANDIDATE_ID_2, UPLOAD_ID)
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }

  @Test
  fun `list paginates in creation order`() =
    runBlocking<Unit> {
      createCandidates(3)

      val first =
        service.listRawImpressionUploadCorrectionCandidates(
          listRawImpressionUploadCorrectionCandidatesRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            pageSize = 2
          }
        )
      val second =
        service.listRawImpressionUploadCorrectionCandidates(
          listRawImpressionUploadCorrectionCandidatesRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            pageSize = 2
            pageToken = first.nextPageToken
          }
        )

      assertThat(
          first.rawImpressionUploadCorrectionCandidatesList.map {
            it.rawImpressionUploadCorrectionCandidateId
          }
        )
        .containsExactly(CANDIDATE_IDS[0], CANDIDATE_IDS[1])
        .inOrder()
      assertThat(first.hasNextPageToken()).isTrue()
      assertThat(
          second.rawImpressionUploadCorrectionCandidatesList.map {
            it.rawImpressionUploadCorrectionCandidateId
          }
        )
        .containsExactly(CANDIDATE_IDS[2])
      assertThat(second.hasNextPageToken()).isFalse()
    }

  @Test
  fun `list filters by state`() =
    runBlocking<Unit> {
      createCandidates(2)
      val pending = getCandidate(CANDIDATE_IDS[0])
      service.advanceRawImpressionUploadCorrectionCandidate(
        advanceRequest(
          CANDIDATE_IDS[0],
          pending.etag,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REQUIRE_MANUAL_INTERVENTION,
        )
      )

      val response =
        service.listRawImpressionUploadCorrectionCandidates(
          listRawImpressionUploadCorrectionCandidatesRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            filter =
              ListRawImpressionUploadCorrectionCandidatesRequestKt.filter {
                stateIn +=
                  RawImpressionUploadCorrectionCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED
              }
          }
        )

      assertThat(
          response.rawImpressionUploadCorrectionCandidatesList.map {
            it.rawImpressionUploadCorrectionCandidateId
          }
        )
        .containsExactly(CANDIDATE_IDS[0])
    }

  @Test
  fun `list filters by classification`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_IDS[0],
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      insertRawUpload(
        2L,
        UPLOAD_IDS[1],
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      service.createRawImpressionUploadCorrectionCandidate(
        createRequest(
          CANDIDATE_IDS[0],
          UPLOAD_IDS[0],
          CREATE_REQUEST_IDS[0],
          RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_EDITED,
        )
      )
      service.createRawImpressionUploadCorrectionCandidate(
        createRequest(
          CANDIDATE_IDS[1],
          UPLOAD_IDS[1],
          CREATE_REQUEST_IDS[1],
          RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_REMOVED,
        )
      )

      val response =
        service.listRawImpressionUploadCorrectionCandidates(
          listRawImpressionUploadCorrectionCandidatesRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            filter =
              ListRawImpressionUploadCorrectionCandidatesRequestKt.filter {
                classificationIn +=
                  RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_EDITED
              }
          }
        )

      assertThat(
          response.rawImpressionUploadCorrectionCandidatesList.map {
            it.rawImpressionUploadCorrectionCandidateId
          }
        )
        .containsExactly(CANDIDATE_IDS[0])
    }

  @Test
  fun `list rejects negative page size`() =
    runBlocking<Unit> {
      val error =
        assertFailsWith<StatusRuntimeException> {
          service.listRawImpressionUploadCorrectionCandidates(
            listRawImpressionUploadCorrectionCandidatesRequest {
              dataProviderResourceId = DATA_PROVIDER_ID
              pageSize = -1
            }
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `correct lifecycle completes`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      insertHealingOperation()
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())

      val assigned =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            pending.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
            operationId = OPERATION_ID,
          )
        )
      val approved =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            assigned.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.APPROVE_APPLY_CANDIDATE,
          )
        )
      completeHealingOperation()
      val complete =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            approved.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.COMPLETE,
          )
        )

      assertThat(assigned.uploadHealingOperationId).isEqualTo(OPERATION_ID)
      assertThat(approved.decision)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE)
      assertThat(complete.state)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_COMPLETE)
      assertThat(complete.manifestComparison).isEqualTo(mixedManifestComparison)
    }

  @Test
  fun `no replacement lifecycle completes without replay state`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      insertHealingOperation()
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())
      val assigned =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            pending.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
            operationId = OPERATION_ID,
          )
        )
      val approved =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            assigned.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action
              .APPROVE_REMOVE_WITHOUT_REPLACEMENT,
          )
        )
      completeHealingOperation()
      val complete =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            approved.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.COMPLETE,
          )
        )

      assertThat(complete.state)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_COMPLETE)
      assertThat(complete.decision)
        .isEqualTo(
          RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT
        )
    }

  @Test
  fun `approved decision is immutable`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      insertHealingOperation()
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())
      val assigned =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            pending.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
            operationId = OPERATION_ID,
          )
        )
      val approved =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            assigned.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.APPROVE_APPLY_CANDIDATE,
          )
        )

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            advanceRequest(
              CANDIDATE_ID,
              approved.etag,
              AdvanceRawImpressionUploadCorrectionCandidateRequest.Action
                .APPROVE_REMOVE_WITHOUT_REPLACEMENT,
            )
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `advance is idempotent`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      insertHealingOperation()
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())
      val request =
        advanceRequest(
          CANDIDATE_ID,
          pending.etag,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
          operationId = OPERATION_ID,
        )

      val first = service.advanceRawImpressionUploadCorrectionCandidate(request)
      val second = service.advanceRawImpressionUploadCorrectionCandidate(request)
      val approved =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            first.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.APPROVE_APPLY_CANDIDATE,
          )
        )
      val delayedRetry = service.advanceRawImpressionUploadCorrectionCandidate(request)

      assertThat(second).isEqualTo(first)
      assertThat(delayedRetry).isEqualTo(approved)
    }

  @Test
  fun `advance rejects request ID reused for another action`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      insertHealingOperation()
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())
      val assignRequest =
        advanceRequest(
          CANDIDATE_ID,
          pending.etag,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
          operationId = OPERATION_ID,
        )
      val assigned = service.advanceRawImpressionUploadCorrectionCandidate(assignRequest)

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            assignRequest.copy {
              etag = assigned.etag
              action =
                AdvanceRawImpressionUploadCorrectionCandidateRequest.Action
                  .REQUIRE_MANUAL_INTERVENTION
              uploadHealingOperationId = ""
            }
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }

  @Test
  fun `advance rejects request ID reused for another candidate`() =
    runBlocking<Unit> {
      createCandidates(2)
      insertHealingOperation()
      val first = getCandidate(CANDIDATE_IDS[0])
      val second = getCandidate(CANDIDATE_IDS[1])
      service.advanceRawImpressionUploadCorrectionCandidate(
        advanceRequest(
          CANDIDATE_IDS[0],
          first.etag,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
          operationId = OPERATION_ID,
        )
      )

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            advanceRequest(
              CANDIDATE_IDS[1],
              second.etag,
              AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
              operationId = OPERATION_ID,
            )
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }

  @Test
  fun `assign plan rejects missing healing operation`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            advanceRequest(
              CANDIDATE_ID,
              pending.etag,
              AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
              operationId = OPERATION_ID,
            )
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `assign plan rejects operation without candidate membership`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      insertHealingOperation(candidateIds = emptyList())
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            advanceRequest(
              CANDIDATE_ID,
              pending.etag,
              AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
              operationId = OPERATION_ID,
            )
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `complete rejects active healing operation`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      insertHealingOperation()
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())
      val assigned =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            pending.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
            operationId = OPERATION_ID,
          )
        )
      val approved =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            assigned.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.APPROVE_APPLY_CANDIDATE,
          )
        )
      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            advanceRequest(
              CANDIDATE_ID,
              approved.etag,
              AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.COMPLETE,
            )
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `advance rejects stale etag`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      service.createRawImpressionUploadCorrectionCandidate(createRequest())

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            advanceRequest(
              CANDIDATE_ID,
              "stale",
              AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
              operationId = OPERATION_ID,
            )
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.ABORTED)
    }

  @Test
  fun `advance rejects invalid lifecycle transition`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            advanceRequest(
              CANDIDATE_ID,
              pending.etag,
              AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.COMPLETE,
            )
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `supersede records newer candidate`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_IDS[0],
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
        doneBlobUri = SHARED_DONE_BLOB_URI,
      )
      insertRawUpload(
        2L,
        UPLOAD_IDS[1],
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED,
        doneBlobUri = SHARED_DONE_BLOB_URI,
        registrationComplete = false,
      )
      service.createRawImpressionUploadCorrectionCandidate(
        createRequest(CANDIDATE_IDS[0], UPLOAD_IDS[0], CREATE_REQUEST_IDS[0])
      )
      insertEvictionFence(CANDIDATE_IDS[0])

      val registration =
        service.registerDetectedRawImpressionUploadCorrectionCandidate(
          registerRequest(
            CANDIDATE_IDS[1],
            UPLOAD_IDS[1],
            CREATE_REQUEST_IDS[1],
            supersededCandidateId = CANDIDATE_IDS[0],
          )
        )
      val superseded = getCandidate(CANDIDATE_IDS[0])

      assertThat(registration.newlyCreated).isTrue()
      assertThat(superseded.state)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED)
      assertThat(superseded.supersedingRawImpressionUploadCorrectionCandidateId)
        .isEqualTo(CANDIDATE_IDS[1])
      val fence =
        spannerDatabase.databaseClient.singleUse().use {
          it.getVidLabelingEvictionFence(DATA_PROVIDER_ID)
        }
      assertThat(fence?.evictionOperationId).isEqualTo(CANDIDATE_IDS[1])
    }

  @Test
  fun `supersede rejects candidate for another done object`() =
    runBlocking<Unit> {
      createCandidates(2)
      val current = getCandidate(CANDIDATE_IDS[0])

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            advanceRequest(
              CANDIDATE_IDS[0],
              current.etag,
              AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.SUPERSEDE,
              supersedingCandidateId = CANDIDATE_IDS[1],
            )
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `supersede active candidate preserves the plan fence`() = runBlocking {
    insertRawUpload(
      1L,
      UPLOAD_IDS[0],
      RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      doneBlobUri = SHARED_DONE_BLOB_URI,
    )
    insertRawUpload(
      2L,
      UPLOAD_IDS[1],
      RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      doneBlobUri = SHARED_DONE_BLOB_URI,
    )
    service.createRawImpressionUploadCorrectionCandidate(
      createRequest(CANDIDATE_IDS[0], UPLOAD_IDS[0], CREATE_REQUEST_IDS[0])
    )
    service.createRawImpressionUploadCorrectionCandidate(
      createRequest(CANDIDATE_IDS[1], UPLOAD_IDS[1], CREATE_REQUEST_IDS[1])
    )
    insertHealingOperation()
    setCandidateState(
      CANDIDATE_IDS[0],
      RawImpressionUploadCorrectionCandidate.State.STATE_ASSIGNED,
      OPERATION_ID,
    )
    insertEvictionFence(OPERATION_ID)
    val current = getCandidate(CANDIDATE_IDS[0])

    val superseded =
      service.advanceRawImpressionUploadCorrectionCandidate(
        advanceRequest(
          CANDIDATE_IDS[0],
          current.etag,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.SUPERSEDE,
          supersedingCandidateId = CANDIDATE_IDS[1],
        )
      )

    assertThat(superseded.state)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED)
    val fence =
      spannerDatabase.databaseClient.singleUse().use {
        it.getVidLabelingEvictionFence(DATA_PROVIDER_ID)
      }
    assertThat(fence?.evictionOperationId).isEqualTo(OPERATION_ID)
  }

  @Test
  fun `terminal candidate rejects further transition`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())
      val manual =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            pending.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REQUIRE_MANUAL_INTERVENTION,
          )
        )

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            advanceRequest(
              CANDIDATE_ID,
              manual.etag,
              AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
              operationId = OPERATION_ID,
            )
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `registered gRPC service purges expired terminal candidate`() = runBlocking {
    val candidate = createCandidateForCleanup(0, TERMINAL_STATES.first(), expireSeconds = 1L)

    val response =
      grpcStub.purgeExpiredRawImpressionUploadCorrectionCandidates(
        purgeExpiredRawImpressionUploadCorrectionCandidatesRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          pageSize = 10
        }
      )

    assertThat(response.purgedCount).isEqualTo(1)
    val error = assertFailsWith<StatusRuntimeException> { getCandidate(candidate.candidateId) }
    assertThat(error.status.code).isEqualTo(Status.Code.NOT_FOUND)
    val upload =
      spannerDatabase.databaseClient.singleUse().use {
        it.getRawImpressionUploadByResourceId(DATA_PROVIDER_ID, candidate.uploadId)
      }
    assertThat(upload.rawImpressionUpload.correctionCandidateId).isEmpty()
  }

  @Test
  fun `purge removes every expired terminal state`() = runBlocking {
    val candidates =
      TERMINAL_STATES.mapIndexed { index, state ->
        createCandidateForCleanup(index, state, expireSeconds = 100L)
      }

    val response =
      cleanupService()
        .purgeExpiredRawImpressionUploadCorrectionCandidates(
          purgeExpiredRawImpressionUploadCorrectionCandidatesRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            pageSize = 10
          }
        )

    assertThat(response.purgedCount).isEqualTo(TERMINAL_STATES.size)
    for (candidate in candidates) {
      val error = assertFailsWith<StatusRuntimeException> { getCandidate(candidate.candidateId) }
      assertThat(error.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }
  }

  @Test
  fun `purge releases a terminal candidate fence and deferred uploads`() = runBlocking {
    val candidate =
      createCandidateForCleanup(
        0,
        RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED,
        expireSeconds = 100L,
      )
    val deferredUploadId = "deferred-upload"
    insertRawUpload(
      internalId = 200L,
      resourceId = deferredUploadId,
      state = RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED,
      processingDeferred = true,
    )
    insertEvictionFence(candidate.candidateId)

    val response =
      cleanupService()
        .purgeExpiredRawImpressionUploadCorrectionCandidates(
          purgeExpiredRawImpressionUploadCorrectionCandidatesRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            pageSize = 10
          }
        )

    assertThat(response.purgedCount).isEqualTo(1)
    val fence =
      spannerDatabase.databaseClient.singleUse().use {
        it.getVidLabelingEvictionFence(DATA_PROVIDER_ID)
      }
    assertThat(fence).isNull()
    val deferredUpload =
      spannerDatabase.databaseClient.singleUse().use {
        it.getRawImpressionUploadByResourceId(DATA_PROVIDER_ID, deferredUploadId)
      }
    assertThat(deferredUpload.rawImpressionUpload.processingDeferred).isFalse()
  }

  @Test
  fun `purge retains unresolved manual and unexpired candidates`() = runBlocking {
    val retainedStates =
      listOf(
        RawImpressionUploadCorrectionCandidate.State.STATE_PENDING,
        RawImpressionUploadCorrectionCandidate.State.STATE_ASSIGNED,
        RawImpressionUploadCorrectionCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED,
      )
    val candidates =
      retainedStates.mapIndexed { index, state ->
        createCandidateForCleanup(index, state, expireSeconds = 100L)
      } +
        createCandidateForCleanup(
          retainedStates.size,
          RawImpressionUploadCorrectionCandidate.State.STATE_COMPLETE,
          expireSeconds = 300L,
        )

    val response =
      cleanupService()
        .purgeExpiredRawImpressionUploadCorrectionCandidates(
          purgeExpiredRawImpressionUploadCorrectionCandidatesRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            pageSize = 10
          }
        )

    assertThat(response.purgedCount).isEqualTo(0)
    for (candidate in candidates) {
      assertThat(getCandidate(candidate.candidateId).state).isEqualTo(candidate.state)
    }
  }

  @Test
  fun `purge waits for an associated healing operation to complete`() = runBlocking {
    val candidate =
      createCandidateForCleanup(
        0,
        RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED,
        expireSeconds = 100L,
        operationId = OPERATION_ID,
      )
    insertHealingOperation(listOf(candidate.candidateId))
    val cleanupService = cleanupService()
    val request = purgeExpiredRawImpressionUploadCorrectionCandidatesRequest {
      dataProviderResourceId = DATA_PROVIDER_ID
      pageSize = 10
    }

    assertThat(
        cleanupService.purgeExpiredRawImpressionUploadCorrectionCandidates(request).purgedCount
      )
      .isEqualTo(0)
    assertThat(getCandidate(candidate.candidateId).state).isEqualTo(candidate.state)

    completeHealingOperation()

    assertThat(
        cleanupService.purgeExpiredRawImpressionUploadCorrectionCandidates(request).purgedCount
      )
      .isEqualTo(1)
  }

  @Test
  fun `purge is paginated and retry safe`() = runBlocking {
    repeat(3) { index ->
      createCandidateForCleanup(
        index,
        RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED,
        expireSeconds = 100L,
      )
    }
    val cleanupService = cleanupService()
    val request = purgeExpiredRawImpressionUploadCorrectionCandidatesRequest {
      dataProviderResourceId = DATA_PROVIDER_ID
      pageSize = 2
    }

    assertThat(
        cleanupService.purgeExpiredRawImpressionUploadCorrectionCandidates(request).purgedCount
      )
      .isEqualTo(2)
    assertThat(
        cleanupService.purgeExpiredRawImpressionUploadCorrectionCandidates(request).purgedCount
      )
      .isEqualTo(1)
    assertThat(
        cleanupService.purgeExpiredRawImpressionUploadCorrectionCandidates(request).purgedCount
      )
      .isEqualTo(0)
  }

  @Test
  fun `purge rejects an invalid page size`() = runBlocking {
    val error =
      assertFailsWith<StatusRuntimeException> {
        cleanupService()
          .purgeExpiredRawImpressionUploadCorrectionCandidates(
            purgeExpiredRawImpressionUploadCorrectionCandidatesRequest {
              dataProviderResourceId = DATA_PROVIDER_ID
              pageSize = 1001
            }
          )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  private data class CleanupCandidate(
    val candidateId: String,
    val uploadId: String,
    val state: RawImpressionUploadCorrectionCandidate.State,
  )

  private fun cleanupService() =
    SpannerRawImpressionUploadCorrectionCandidateService(
      spannerDatabase.databaseClient,
      clock = Clock.fixed(Instant.ofEpochSecond(200L), ZoneOffset.UTC),
    )

  private suspend fun createCandidateForCleanup(
    index: Int,
    state: RawImpressionUploadCorrectionCandidate.State,
    expireSeconds: Long,
    operationId: String = "",
  ): CleanupCandidate {
    val suffix = (index + 1L).toString().padStart(12, '0')
    val candidateId = "10000000-0000-4000-8000-$suffix"
    val requestId = "20000000-0000-4000-8000-$suffix"
    val uploadId = "cleanup-upload-${index + 1}"
    insertRawUpload(
      internalId = index + 100L,
      resourceId = uploadId,
      state = RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      correctionCandidateId = candidateId,
    )
    service.createRawImpressionUploadCorrectionCandidate(
      createRequest(
        candidateId = candidateId,
        uploadId = uploadId,
        requestId = requestId,
        expireTime = timestamp { seconds = expireSeconds },
      )
    )
    val mutation =
      Mutation.newUpdateBuilder("RawImpressionUploadCorrectionCandidate")
        .set("DataProviderResourceId")
        .to(DATA_PROVIDER_ID)
        .set("RawImpressionUploadCorrectionCandidateId")
        .to(candidateId)
        .set("State")
        .to(Value.protoEnum(state))
        .set("UpdateTime")
        .to(Value.COMMIT_TIMESTAMP)
    if (operationId.isNotEmpty()) {
      mutation.set("UploadHealingOperationId").to(operationId)
    }
    spannerDatabase.databaseClient.write(listOf(mutation.build()))
    return CleanupCandidate(candidateId, uploadId, state)
  }

  private suspend fun createCandidates(count: Int) {
    for (index in 0 until count) {
      insertRawUpload(
        index.toLong() + 1L,
        UPLOAD_IDS[index],
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      service.createRawImpressionUploadCorrectionCandidate(
        createRequest(CANDIDATE_IDS[index], UPLOAD_IDS[index], CREATE_REQUEST_IDS[index])
      )
    }
  }

  private suspend fun getCandidate(candidateId: String): RawImpressionUploadCorrectionCandidate =
    service.getRawImpressionUploadCorrectionCandidate(
      getRawImpressionUploadCorrectionCandidateRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        rawImpressionUploadCorrectionCandidateId = candidateId
      }
    )

  private suspend fun setCandidateHealing(
    decision: RawImpressionUploadCorrectionCandidate.Decision
  ) {
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("RawImpressionUploadCorrectionCandidate")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("RawImpressionUploadCorrectionCandidateId")
          .to(CANDIDATE_ID)
          .set("State")
          .to(Value.protoEnum(RawImpressionUploadCorrectionCandidate.State.STATE_ASSIGNED))
          .set("Decision")
          .to(Value.protoEnum(decision))
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    )
  }

  private suspend fun createAuthorizedQuarantine():
    org.wfanet.measurement.internal.edpaggregator.RawImpressionUpload {
    insertRawUpload(
      1L,
      UPLOAD_ID,
      RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED,
      doneBlobUri = SHARED_DONE_BLOB_URI,
    )
    val quarantined =
      grpcStub.createQuarantinedRawImpressionUpload(
        createQuarantinedRawImpressionUploadRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          rawImpressionUpload = rawImpressionUpload {
            doneBlobUri = SHARED_DONE_BLOB_URI
            doneBlobGeneration = 2L
            doneBlobCreateTime = timestamp { seconds = 2L }
          }
          rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
          requestId = CREATE_REQUEST_ID
        }
      )
    val comparison =
      manifestComparison(
        RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_EDITED,
        quarantined.rawImpressionUploadResourceId,
      )
    grpcStub.registerDetectedRawImpressionUploadCorrectionCandidate(
      registerDetectedRawImpressionUploadCorrectionCandidateRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
        rawImpressionUploadCorrectionCandidate = rawImpressionUploadCorrectionCandidate {
          rawImpressionUploadResourceId = quarantined.rawImpressionUploadResourceId
          classification =
            RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_EDITED
          priorManifestDigest = manifestDigest(comparison.priorManifestList)
          currentManifestDigest = manifestDigest(comparison.currentManifestList)
          manifestComparison = comparison
          expireTime = EXPIRY_TIME
        }
        requestId = REGISTER_REQUEST_ID
      }
    )
    insertHealingOperation(listOf(CANDIDATE_ID))
    authorizeCandidateReplay()
    return quarantined
  }

  private fun activationRequest(
    uploadId: String,
    sourceUploadId: String = UPLOAD_ID,
  ): ActivateQuarantinedRawImpressionUploadRequest = activateQuarantinedRawImpressionUploadRequest {
    dataProviderResourceId = DATA_PROVIDER_ID
    rawImpressionUploadResourceId = uploadId
    sourceRawImpressionUploadResourceId = sourceUploadId
    evictionOperationId = OPERATION_ID
    requestId = ACTIVATE_REQUEST_ID
  }

  private suspend fun authorizeCandidateReplay() {
    setCandidateState(
      CANDIDATE_ID,
      RawImpressionUploadCorrectionCandidate.State.STATE_ASSIGNED,
      OPERATION_ID,
    )
    setCandidateHealing(RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE)
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("UploadHealingOperation")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("UploadHealingOperationId")
          .to(OPERATION_ID)
          .set("State")
          .to(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_REPLAYING.number.toLong())
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build(),
        Mutation.newUpdateBuilder("VidLabelingEvictionFence")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("EvictionOperationId")
          .to(OPERATION_ID)
          .set("State")
          .to(
            Value.protoEnum(
              VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
            )
          )
          .build(),
        Mutation.newInsertBuilder("UploadHealingStep")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("UploadHealingOperationId")
          .to(OPERATION_ID)
          .set("UploadHealingStepId")
          .to(1L)
          .set("SequenceNumber")
          .to(0L)
          .set("SourceRawImpressionUploadResourceId")
          .to(UPLOAD_ID_2)
          .set("RawImpressionUploadModelLineResourceId")
          .to("earlier-model-line")
          .set("CmmsModelLine")
          .to("modelProviders/mp/modelSuites/ms/modelLines/ml")
          .set("Memoized")
          .to(false)
          .set("RecoveryAction")
          .to(
            Value.protoEnum(
              RawImpressionUploadModelLineRecoveryAction
                .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_EDP_CORRECTION
            )
          )
          .set("RecoveryTarget")
          .to(false)
          .set("RawImpressionUploadCorrectionCandidateId")
          .to(CANDIDATE_ID)
          .set("CompleteTime")
          .to(Value.COMMIT_TIMESTAMP)
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build(),
        Mutation.newInsertBuilder("UploadHealingStep")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("UploadHealingOperationId")
          .to(OPERATION_ID)
          .set("UploadHealingStepId")
          .to(2L)
          .set("SequenceNumber")
          .to(1L)
          .set("SourceRawImpressionUploadResourceId")
          .to(UPLOAD_ID)
          .set("RawImpressionUploadModelLineResourceId")
          .to("target-model-line")
          .set("CmmsModelLine")
          .to("modelProviders/mp/modelSuites/ms/modelLines/ml")
          .set("Memoized")
          .to(false)
          .set("RecoveryAction")
          .to(
            Value.protoEnum(
              RawImpressionUploadModelLineRecoveryAction
                .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_EDP_CORRECTION
            )
          )
          .set("RecoveryTarget")
          .to(true)
          .set("RawImpressionUploadCorrectionCandidateId")
          .to(CANDIDATE_ID)
          .set("EvictionCompleteTime")
          .to(Value.COMMIT_TIMESTAMP)
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build(),
      )
    )
  }

  private suspend fun setCandidateState(
    candidateId: String,
    state: RawImpressionUploadCorrectionCandidate.State,
    operationId: String,
  ) {
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("RawImpressionUploadCorrectionCandidate")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("RawImpressionUploadCorrectionCandidateId")
          .to(candidateId)
          .set("State")
          .to(Value.protoEnum(state))
          .set("UploadHealingOperationId")
          .to(operationId)
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    )
  }

  private fun createRequest(
    candidateId: String = CANDIDATE_ID,
    uploadId: String = UPLOAD_ID,
    requestId: String = CREATE_REQUEST_ID,
    classification: RawImpressionUploadCorrectionCandidate.Classification =
      RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_MIXED,
    priorGeneration: Long = 1L,
    expireTime: com.google.protobuf.Timestamp = EXPIRY_TIME,
  ) = createRawImpressionUploadCorrectionCandidateRequest {
    val comparison = manifestComparison(classification, uploadId, priorGeneration)
    dataProviderResourceId = DATA_PROVIDER_ID
    rawImpressionUploadCorrectionCandidateId = candidateId
    rawImpressionUploadCorrectionCandidate = rawImpressionUploadCorrectionCandidate {
      rawImpressionUploadResourceId = uploadId
      this.classification = classification
      priorManifestDigest = manifestDigest(comparison.priorManifestList)
      currentManifestDigest = manifestDigest(comparison.currentManifestList)
      manifestComparison = comparison
      this.expireTime = expireTime
    }
    this.requestId = requestId
  }

  private fun registerRequest(
    candidateId: String = CANDIDATE_ID,
    uploadId: String = UPLOAD_ID,
    requestId: String = CREATE_REQUEST_ID,
    supersededCandidateId: String = "",
  ) = registerDetectedRawImpressionUploadCorrectionCandidateRequest {
    val comparison =
      manifestComparison(
        RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_MIXED,
        uploadId,
      )
    dataProviderResourceId = DATA_PROVIDER_ID
    rawImpressionUploadCorrectionCandidateId = candidateId
    rawImpressionUploadCorrectionCandidate = rawImpressionUploadCorrectionCandidate {
      rawImpressionUploadResourceId = uploadId
      classification = RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_MIXED
      priorManifestDigest = manifestDigest(comparison.priorManifestList)
      currentManifestDigest = manifestDigest(comparison.currentManifestList)
      manifestComparison = comparison
      expireTime = EXPIRY_TIME
    }
    supersededRawImpressionUploadCorrectionCandidateId = supersededCandidateId
    this.requestId = requestId
  }

  private fun advanceRequest(
    candidateId: String,
    etag: String,
    action: AdvanceRawImpressionUploadCorrectionCandidateRequest.Action,
    operationId: String = "",
    supersedingCandidateId: String = "",
  ) = advanceRawImpressionUploadCorrectionCandidateRequest {
    dataProviderResourceId = DATA_PROVIDER_ID
    rawImpressionUploadCorrectionCandidateId = candidateId
    this.etag = etag
    this.action = action
    uploadHealingOperationId = operationId
    supersedingRawImpressionUploadCorrectionCandidateId = supersedingCandidateId
    requestId = REQUEST_IDS.getValue(action)
  }

  private fun manifestComparison(
    classification: RawImpressionUploadCorrectionCandidate.Classification,
    currentOwner: String,
    priorGeneration: Long = 1L,
  ): RawImpressionUploadCorrectionCandidate.ManifestComparison =
    RawImpressionUploadCorrectionCandidateKt.manifestComparison {
      val priorA = manifestEntry("gs://raw/a", priorGeneration, "prior-a")
      priorManifest += priorA
      when (classification) {
        RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_EDITED -> {
          val currentA = manifestEntry("gs://raw/a", 2L, currentOwner)
          currentManifest += currentA
          differences +=
            RawImpressionUploadCorrectionCandidateKt.manifestDifference {
              type = RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.TYPE_EDITED
              prior = priorA
              current = currentA
            }
        }
        RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_REMOVED -> {
          differences +=
            RawImpressionUploadCorrectionCandidateKt.manifestDifference {
              type = RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.TYPE_REMOVED
              prior = priorA
            }
        }
        RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_MIXED -> {
          val priorB = manifestEntry("gs://raw/b", 1L, "prior-b")
          val currentA = manifestEntry("gs://raw/a", 2L, currentOwner)
          val currentC = manifestEntry("gs://raw/c", 1L, currentOwner)
          priorManifest += priorB
          currentManifest += listOf(currentA, currentC)
          differences +=
            listOf(
              RawImpressionUploadCorrectionCandidateKt.manifestDifference {
                type = RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.TYPE_EDITED
                prior = priorA
                current = currentA
              },
              RawImpressionUploadCorrectionCandidateKt.manifestDifference {
                type = RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.TYPE_REMOVED
                prior = priorB
              },
              RawImpressionUploadCorrectionCandidateKt.manifestDifference {
                type = RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.TYPE_ADDED
                current = currentC
              },
            )
        }
        RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_UNSPECIFIED,
        RawImpressionUploadCorrectionCandidate.Classification.UNRECOGNIZED ->
          error("Invalid test classification")
      }
    }

  private fun manifestEntry(
    uri: String,
    generation: Long,
    owner: String,
    eventDate: Date = Date.getDefaultInstance(),
  ) =
    RawImpressionUploadCorrectionCandidateKt.manifestEntry {
      blobUri = uri
      blobGeneration = generation
      this.eventDate = eventDate
      outputSourceRawImpressionUploadResourceId = owner
    }

  private fun manifestDigest(
    manifest: List<RawImpressionUploadCorrectionCandidate.ManifestEntry>
  ): ByteString {
    val digest = MessageDigest.getInstance("SHA-256")
    for (entry in manifest.sortedBy { it.blobUri }) {
      val uriBytes = entry.blobUri.toByteArray(StandardCharsets.UTF_8)
      digest.update(ByteBuffer.allocate(Int.SIZE_BYTES).putInt(uriBytes.size).array())
      digest.update(uriBytes)
      digest.update(ByteBuffer.allocate(Long.SIZE_BYTES).putLong(entry.blobGeneration).array())
      digest.update(
        ByteBuffer.allocate(Int.SIZE_BYTES * 3)
          .putInt(entry.eventDate.year)
          .putInt(entry.eventDate.month)
          .putInt(entry.eventDate.day)
          .array()
      )
    }
    return digest.digest().toByteString()
  }

  private suspend fun insertRawUpload(
    internalId: Long,
    resourceId: String,
    state: RawImpressionUploadState,
    doneBlobUri: String = "gs://bucket/$resourceId/done",
    registrationComplete: Boolean = true,
    includeDoneBlobCreateTime: Boolean = true,
    processingDeferred: Boolean = false,
    correctionCandidateId: String = "",
  ) {
    val mutation =
      Mutation.newInsertBuilder("RawImpressionUpload")
        .set("DataProviderResourceId")
        .to(DATA_PROVIDER_ID)
        .set("RawImpressionUploadId")
        .to(internalId)
        .set("RawImpressionUploadResourceId")
        .to(resourceId)
        .set("DoneBlobUri")
        .to(doneBlobUri)
        .set("DoneBlobGeneration")
        .to(internalId)
        .set("RegistrationComplete")
        .to(registrationComplete)
        .set("ProcessingDeferred")
        .to(processingDeferred)
        .set("State")
        .to(Value.protoEnum(state))
        .set("CreateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .set("UpdateTime")
        .to(Value.COMMIT_TIMESTAMP)
    if (includeDoneBlobCreateTime) {
      mutation.set("DoneBlobCreateTime").to(Timestamp.ofTimeSecondsAndNanos(internalId, 0))
    }
    if (correctionCandidateId.isNotEmpty()) {
      mutation.set("CorrectionCandidateId").to(correctionCandidateId)
    }
    spannerDatabase.databaseClient.write(listOf(mutation.build()))
  }

  private suspend fun insertEvictionFence(ownerId: String) {
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newInsertBuilder("VidLabelingEvictionFence")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("EvictionOperationId")
          .to(ownerId)
          .set("Etag")
          .to("fence-etag")
          .set("State")
          .to(
            Value.protoEnum(
              VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING
            )
          )
          .set("CreateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    )
  }

  private suspend fun insertHealingOperation(candidateIds: List<String> = CANDIDATE_IDS) {
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newInsertBuilder("UploadHealingOperation")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("UploadHealingOperationId")
          .to(OPERATION_ID)
          .set("ReconcileRequestId")
          .to(HEALING_CREATE_REQUEST_ID)
          .set("State")
          .to(
            UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED.number
              .toLong()
          )
          .set("Reason")
          .to("correction")
          .set("RawImpressionUploadCorrectionCandidateIds")
          .toStringArray(candidateIds)
          .set("MutationRequestIds")
          .toStringArray(emptyList())
          .set("MutationRequestFingerprints")
          .toBytesArray(emptyList())
          .set("CreateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    )
  }

  private suspend fun completeHealingOperation() {
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("UploadHealingOperation")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("UploadHealingOperationId")
          .to(OPERATION_ID)
          .set("State")
          .to(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_COMPLETE.number.toLong())
          .set("CompleteTime")
          .to(Value.COMMIT_TIMESTAMP)
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    )
  }

  companion object {
    @JvmField @ClassRule val spannerEmulator = SpannerEmulatorRule()

    private const val DATA_PROVIDER_ID = "data-provider"
    private const val UPLOAD_ID = "upload-1"
    private const val UPLOAD_ID_2 = "upload-2"
    private const val CANDIDATE_ID = "11111111-1111-4111-8111-111111111111"
    private const val CANDIDATE_ID_2 = "22222222-2222-4222-8222-222222222222"
    private const val CREATE_REQUEST_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    private const val REGISTER_REQUEST_ID = "ffffffff-ffff-4fff-8fff-ffffffffffff"
    private const val ACTIVATE_REQUEST_ID = "99999999-9999-4999-8999-999999999999"
    private const val OPERATION_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    private const val HEALING_CREATE_REQUEST_ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
    private const val SHARED_DONE_BLOB_URI = "gs://bucket/shared/done"
    private val UPLOAD_IDS = listOf(UPLOAD_ID, UPLOAD_ID_2, "upload-3")
    private val CANDIDATE_IDS =
      listOf(CANDIDATE_ID, CANDIDATE_ID_2, "33333333-3333-4333-8333-333333333333")
    private val CREATE_REQUEST_IDS =
      listOf(
        CREATE_REQUEST_ID,
        "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
      )
    private val REQUEST_IDS =
      mapOf(
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN to
          "00000000-0000-4000-8000-000000000001",
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.APPROVE_APPLY_CANDIDATE to
          "00000000-0000-4000-8000-000000000002",
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action
          .APPROVE_REMOVE_WITHOUT_REPLACEMENT to "00000000-0000-4000-8000-000000000003",
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.COMPLETE to
          "00000000-0000-4000-8000-000000000005",
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.SUPERSEDE to
          "00000000-0000-4000-8000-000000000007",
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REQUIRE_MANUAL_INTERVENTION to
          "00000000-0000-4000-8000-000000000008",
      )
    private val TERMINAL_STATES =
      listOf(
        RawImpressionUploadCorrectionCandidate.State.STATE_COMPLETE,
        RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED,
      )
    private val EXPIRY_TIME = timestamp { seconds = 4_102_444_800L }
  }
}
