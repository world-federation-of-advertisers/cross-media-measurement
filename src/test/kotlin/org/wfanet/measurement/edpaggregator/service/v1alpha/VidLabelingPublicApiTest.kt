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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat as assertThatProto
import com.google.protobuf.ByteString
import com.google.type.date
import io.grpc.ManagedChannel
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.netty.GrpcSslContexts
import io.grpc.netty.NettyChannelBuilder
import io.netty.handler.ssl.ClientAuth
import java.nio.file.Paths
import java.time.Instant
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
import org.wfanet.measurement.edpaggregator.v1alpha.FinalizeUploadHealingOperationRequestKt as PublicFinalizeKt
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledOutputManifestKt as PublicManifestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidateServiceGrpcKt as PublicCandidates
import org.wfanet.measurement.edpaggregator.v1alpha.ReconcileUploadHealingOperationRequestKt as PublicPlanKt
import org.wfanet.measurement.edpaggregator.v1alpha.RegisterRawImpressionUploadCorrectionCandidateRequestKt as PublicComparisonKt
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt as PublicOperations
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.activateRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.advanceUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.advanceUploadHealingStepRequest
import org.wfanet.measurement.edpaggregator.v1alpha.copy as publicCopy
import org.wfanet.measurement.edpaggregator.v1alpha.finalizeUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.labeledOutputManifest as publicManifest
import org.wfanet.measurement.edpaggregator.v1alpha.purgeRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.quarantineRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.reconcileUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.registerRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.edpaggregator.v1alpha.resolveRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.ActivateQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingStepRequest as InternalAdvanceStep
import org.wfanet.measurement.internal.edpaggregator.CreateQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.FinalizeUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.FinalizeUploadHealingOperationRequestKt as InternalFinalizeKt
import org.wfanet.measurement.internal.edpaggregator.LabeledOutputManifestKt as InternalManifestKt
import org.wfanet.measurement.internal.edpaggregator.PurgeExpiredRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate as InternalCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateKt as InternalComparisonKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt as InternalCandidates
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.ReconcileUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.RegisterDetectedRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.ResolveDetectedRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation as InternalOperation
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperationServiceGrpcKt as InternalOperations
import org.wfanet.measurement.internal.edpaggregator.UploadHealingStep as InternalStep
import org.wfanet.measurement.internal.edpaggregator.activateQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.advanceUploadHealingOperationRequest as internalAdvance
import org.wfanet.measurement.internal.edpaggregator.advanceUploadHealingStepRequest as internalAdvanceStep
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.createQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.finalizeUploadHealingOperationRequest as internalFinalize
import org.wfanet.measurement.internal.edpaggregator.labeledOutputManifest as internalManifest
import org.wfanet.measurement.internal.edpaggregator.purgeExpiredRawImpressionUploadCorrectionCandidatesResponse
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUpload
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.internal.edpaggregator.reconcileUploadHealingOperationRequest as internalReconcile
import org.wfanet.measurement.internal.edpaggregator.registerDetectedRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.registerDetectedRawImpressionUploadCorrectionCandidateResponse
import org.wfanet.measurement.internal.edpaggregator.resolveDetectedRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.resolveDetectedRawImpressionUploadCorrectionCandidateResponse
import org.wfanet.measurement.internal.edpaggregator.uploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.uploadHealingStep

class VidLabelingPublicApiTest {
  private val candidates =
    mockService<InternalCandidates.RawImpressionUploadCorrectionCandidateServiceCoroutineImplBase>()
  private val operations =
    mockService<InternalOperations.UploadHealingOperationServiceCoroutineImplBase>()
  @get:Rule
  val internalServer = GrpcTestServerRule {
    addService(candidates)
    addService(operations)
  }
  private lateinit var server: CommonServer
  private lateinit var channel: ManagedChannel
  private val candidateStub
    get() = PublicCandidates.RawImpressionUploadCorrectionCandidateServiceCoroutineStub(channel)

  private val operationStub
    get() = PublicOperations.UploadHealingOperationServiceCoroutineStub(channel)

  @Before
  fun start() =
    runBlocking<Unit> {
      whenever(candidates.createQuarantinedRawImpressionUpload(any())).thenReturn(UPLOAD)
      whenever(candidates.activateQuarantinedRawImpressionUpload(any())).thenReturn(UPLOAD)
      whenever(candidates.registerDetectedRawImpressionUploadCorrectionCandidate(any()))
        .thenReturn(
          registerDetectedRawImpressionUploadCorrectionCandidateResponse {
            rawImpressionUploadCorrectionCandidate = CANDIDATE
            newlyCreated = true
          }
        )
      whenever(candidates.resolveDetectedRawImpressionUploadCorrectionCandidate(any()))
        .thenReturn(
          resolveDetectedRawImpressionUploadCorrectionCandidateResponse { newlyResolved = true }
        )
      whenever(candidates.getRawImpressionUploadCorrectionCandidate(any())).thenReturn(CANDIDATE)
      whenever(candidates.purgeExpiredRawImpressionUploadCorrectionCandidates(any()))
        .thenReturn(purgeExpiredRawImpressionUploadCorrectionCandidatesResponse { purgedCount = 7 })
      whenever(operations.reconcileUploadHealingOperation(any())).thenReturn(OPERATION)
      whenever(operations.advanceUploadHealingOperation(any())).thenReturn(OPERATION)
      whenever(operations.finalizeUploadHealingOperation(any())).thenReturn(OPERATION)
      whenever(operations.advanceUploadHealingStep(any())).thenReturn(STEP)
      whenever(operations.getUploadHealingOperation(any())).thenReturn(OPERATION)
      server =
        CommonServer.fromParameters(
            nameForLogging = "VidLabelingPublicApiTest",
            verboseGrpcLogging = false,
            certs =
              SigningCerts.fromPemFiles(
                fixture("edp_aggregator_tls.pem"),
                fixture("edp_aggregator_tls.key"),
                fixture("edp_aggregator_trusted_certs.pem"),
              ),
            clientAuth = ClientAuth.REQUIRE,
            services =
              Services.build(internalServer.channel, EmptyCoroutineContext).toList().map {
                it.bindService()
              },
          )
          .start()
      channel =
        buildMutualTlsChannel(
          "localhost:${server.port}",
          SigningCerts.fromPemFiles(
            fixture("edpa_tee_app_tls.pem"),
            fixture("edpa_tee_app_tls.key"),
            fixture("edp_aggregator_root.pem"),
          ),
          "localhost",
        )
    }

  @After
  fun stop() {
    if (::channel.isInitialized) channel.shutdownNow()
    if (::server.isInitialized) server.shutdown()
  }

  @Test
  fun `quarantine forwards exact identity and returns public upload lifecycle`() =
    runBlocking<Unit> {
      val response = candidateStub.quarantineRawImpressionUpload(quarantine()).rawImpressionUpload
      val requests = argumentCaptor<CreateQuarantinedRawImpressionUploadRequest>()
      verifyBlocking(candidates) { createQuarantinedRawImpressionUpload(requests.capture()) }
      assertThatProto(requests.firstValue)
        .isEqualTo(
          createQuarantinedRawImpressionUploadRequest {
            dataProviderResourceId = "dp"
            rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
            requestId = REQUEST_ID
            rawImpressionUpload = rawImpressionUpload {
              doneBlobUri = DONE
              doneBlobGeneration = 20L
              doneBlobCreateTime = NOW.toProtoTime()
            }
          }
        )
      assertThat(response.name).isEqualTo(UPLOAD_NAME)
      assertThat(response.state).isEqualTo(RawImpressionUpload.State.CORRECTION_REQUIRED)
      assertThat(response.doneBlobUri).isEqualTo(DONE)
      assertThat(response.doneBlobGeneration).isEqualTo(20L)
      assertThatProto(response.doneBlobCreateTime).isEqualTo(NOW.toProtoTime())
      assertThat(response.etag).isEqualTo("upload-etag")
      assertThat(response.registrationComplete).isTrue()
      assertThat(response.processingDeferred).isTrue()
      assertThat(response.replacesRawImpressionUpload).isEqualTo("$PARENT/rawImpressionUploads/old")
      assertThat(response.uploadHealingOperation).isEqualTo(OPERATION_NAME)
      assertThatProto(response.createTime).isEqualTo(NOW.toProtoTime())
      assertThatProto(response.updateTime).isEqualTo(NOW.plusSeconds(1).toProtoTime())
    }

  @Test
  fun `activation preserves full source and operation identity`() =
    runBlocking<Unit> {
      candidateStub.activateRawImpressionUpload(
        activateRawImpressionUploadRequest {
          name = UPLOAD_NAME
          sourceRawImpressionUpload = "$PARENT/rawImpressionUploads/old"
          uploadHealingOperation = OPERATION_NAME
          requestId = REQUEST_ID
        }
      )
      val requests = argumentCaptor<ActivateQuarantinedRawImpressionUploadRequest>()
      verifyBlocking(candidates) { activateQuarantinedRawImpressionUpload(requests.capture()) }
      assertThatProto(requests.firstValue)
        .isEqualTo(
          activateQuarantinedRawImpressionUploadRequest {
            dataProviderResourceId = "dp"
            rawImpressionUploadResourceId = "upload"
            sourceRawImpressionUploadResourceId = "old"
            evictionOperationId = OPERATION_ID
            requestId = REQUEST_ID
          }
        )
    }

  @Test
  fun `registration preserves complete manifests differences and retry flags`() =
    runBlocking<Unit> {
      whenever(candidates.registerDetectedRawImpressionUploadCorrectionCandidate(any()))
        .thenReturn(
          registerDetectedRawImpressionUploadCorrectionCandidateResponse {
            rawImpressionUploadCorrectionCandidate = CANDIDATE
            newlyCreated = true
          },
          registerDetectedRawImpressionUploadCorrectionCandidateResponse {
            rawImpressionUploadCorrectionCandidate = CANDIDATE
            newlyCreated = false
          },
        )
      val first = candidateStub.registerRawImpressionUploadCorrectionCandidate(registration())
      val retry = candidateStub.registerRawImpressionUploadCorrectionCandidate(registration())
      val requests = argumentCaptor<RegisterDetectedRawImpressionUploadCorrectionCandidateRequest>()
      verifyBlocking(candidates, times(2)) {
        registerDetectedRawImpressionUploadCorrectionCandidate(requests.capture())
      }
      val expected = registerDetectedRawImpressionUploadCorrectionCandidateRequest {
        dataProviderResourceId = "dp"
        rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
        rawImpressionUploadCorrectionCandidate =
          CANDIDATE.copy {
            clearDataProviderResourceId()
            clearRawImpressionUploadCorrectionCandidateId()
            clearState()
            clearDecision()
            clearUploadHealingOperationId()
            clearSupersedingRawImpressionUploadCorrectionCandidateId()
            clearCreateTime()
            clearUpdateTime()
            clearEtag()
          }
        supersededRawImpressionUploadCorrectionCandidateId = OTHER_CANDIDATE_ID
        requestId = REQUEST_ID
      }
      for (request in requests.allValues) assertThatProto(request).isEqualTo(expected)
      assertThat(first.newlyCreated).isTrue()
      assertThat(retry.newlyCreated).isFalse()
      val resource = first.rawImpressionUploadCorrectionCandidate
      assertThat(resource.name).isEqualTo(CANDIDATE_NAME)
      assertThat(resource.rawImpressionUpload).isEqualTo(UPLOAD_NAME)
      assertThat(resource.uploadHealingOperation).isEqualTo(OPERATION_NAME)
      assertThat(resource.supersedingRawImpressionUploadCorrectionCandidate)
        .isEqualTo("$PARENT/rawImpressionUploadCorrectionCandidates/$OTHER_CANDIDATE_ID")
      assertThat(resource.state).isEqualTo(RawImpressionUploadCorrectionCandidate.State.ASSIGNED)
      assertThat(resource.decision)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE)
      assertThat(resource.manifestDifferencesCount).isEqualTo(3)
      assertThat(resource.manifestDifferencesList.first().priorOutputSourceRawImpressionUpload)
        .isEqualTo("$PARENT/rawImpressionUploads/old")
      assertThatProto(resource.manifestDifferencesList.first().currentEventDate).isEqualTo(DATE_2)
      assertThat(resource.etag).isEqualTo("candidate-etag")
      assertThatProto(resource.expireTime).isEqualTo(NOW.plusSeconds(86400).toProtoTime())
    }

  @Test
  fun `resolution preserves exact candidate and idempotent result`() =
    runBlocking<Unit> {
      whenever(candidates.resolveDetectedRawImpressionUploadCorrectionCandidate(any()))
        .thenReturn(
          resolveDetectedRawImpressionUploadCorrectionCandidateResponse { newlyResolved = true },
          resolveDetectedRawImpressionUploadCorrectionCandidateResponse { newlyResolved = false },
        )
      val request = resolveRawImpressionUploadCorrectionCandidateRequest {
        name = CANDIDATE_NAME
        requestId = REQUEST_ID
      }
      assertThat(candidateStub.resolveRawImpressionUploadCorrectionCandidate(request).newlyResolved)
        .isTrue()
      assertThat(candidateStub.resolveRawImpressionUploadCorrectionCandidate(request).newlyResolved)
        .isFalse()
      val requests = argumentCaptor<ResolveDetectedRawImpressionUploadCorrectionCandidateRequest>()
      verifyBlocking(candidates, times(2)) {
        resolveDetectedRawImpressionUploadCorrectionCandidate(requests.capture())
      }
      for (forwarded in requests.allValues) assertThatProto(forwarded)
        .isEqualTo(
          resolveDetectedRawImpressionUploadCorrectionCandidateRequest {
            dataProviderResourceId = "dp"
            rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
            requestId = REQUEST_ID
          }
        )
    }

  @Test
  fun `purge forwards bounded defaults and caps large pages`() =
    runBlocking<Unit> {
      for (size in listOf(0, 1001)) {
        assertThat(
            candidateStub
              .purgeRawImpressionUploadCorrectionCandidates(
                purgeRawImpressionUploadCorrectionCandidatesRequest {
                  parent = PARENT
                  pageSize = size
                }
              )
              .purgeCount
          )
          .isEqualTo(7)
      }
      val requests = argumentCaptor<PurgeExpiredRawImpressionUploadCorrectionCandidatesRequest>()
      verifyBlocking(candidates, times(2)) {
        purgeExpiredRawImpressionUploadCorrectionCandidates(requests.capture())
      }
      assertThat(requests.allValues.map { it.pageSize }).containsExactly(100, 1000).inOrder()
      assertThat(requests.allValues.map { it.dataProviderResourceId }).containsExactly("dp", "dp")
    }

  @Test
  fun `reconcile converts complete controller plan into internal resource IDs`() =
    runBlocking<Unit> {
      val result = operationStub.reconcileUploadHealingOperation(reconcile()).uploadHealingOperation
      val requests = argumentCaptor<ReconcileUploadHealingOperationRequest>()
      verifyBlocking(operations) { reconcileUploadHealingOperation(requests.capture()) }
      assertThatProto(requests.firstValue)
        .isEqualTo(
          internalReconcile {
            dataProviderResourceId = "dp"
            uploadHealingOperationId = OPERATION_ID
            etag = "draft-etag"
            requestId = REQUEST_ID
            uploadHealingOperation =
              OPERATION.copy {
                clearDataProviderResourceId()
                clearUploadHealingOperationId()
                clearCreateTime()
                clearUpdateTime()
                clearEtag()
                state = InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
                steps.clear()
                steps +=
                  STEP.copy {
                    clearState()
                    clearReplacementRawImpressionUploadResourceId()
                    clearEvictionCompleteTime()
                    clearRecoveryStartTime()
                    clearCompleteTime()
                    clearUpdateTime()
                    clearEtag()
                    clearRecoveryDoneBlobGeneration()
                    labeledOutputManifest = INTERNAL_PROVISIONAL_OUTPUT
                  }
              }
          }
        )
      assertPublicOperation(result)
    }

  @Test
  fun `advance operation preserves state and concurrency fields`() =
    runBlocking<Unit> {
      val result =
        operationStub.advanceUploadHealingOperation(
          advanceUploadHealingOperationRequest {
            name = OPERATION_NAME
            state = UploadHealingOperation.State.EVICTING
            etag = "operation-etag"
            requestId = REQUEST_ID
          }
        )
      val requests = argumentCaptor<AdvanceUploadHealingOperationRequest>()
      verifyBlocking(operations) { advanceUploadHealingOperation(requests.capture()) }
      assertThatProto(requests.firstValue)
        .isEqualTo(
          internalAdvance {
            dataProviderResourceId = "dp"
            uploadHealingOperationId = OPERATION_ID
            state = InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING
            etag = "operation-etag"
            requestId = REQUEST_ID
          }
        )
      assertPublicOperation(result)
    }

  @Test
  fun `finalization forwards exact generations including absent markers`() =
    runBlocking<Unit> {
      val result = operationStub.finalizeUploadHealingOperation(finalization())
      val requests = argumentCaptor<FinalizeUploadHealingOperationRequest>()
      verifyBlocking(operations) { finalizeUploadHealingOperation(requests.capture()) }
      assertThatProto(requests.firstValue)
        .isEqualTo(
          internalFinalize {
            dataProviderResourceId = "dp"
            uploadHealingOperationId = OPERATION_ID
            stepManifests +=
              InternalFinalizeKt.stepManifest {
                uploadHealingStepId = 1L
                labeledOutputManifest = INTERNAL_OUTPUT
              }
            etag = "operation-etag"
            requestId = REQUEST_ID
          }
        )
      assertPublicOperation(result)
    }

  @Test
  fun `all three step actions preserve replacement generation and returned lifecycle`() =
    runBlocking {
      for (action in
        listOf(
          AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION,
          AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY,
          AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT,
        )) {
        val result =
          operationStub.advanceUploadHealingStep(
            advanceUploadHealingStepRequest {
              name = STEP_NAME
              this.action = action
              if (action == AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT)
                replacementRawImpressionUpload = "$PARENT/rawImpressionUploads/replacement"
              if (action == AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY)
                recoveryDoneBlobGeneration = 73L
              etag = "step-etag"
              requestId = REQUEST_ID
            }
          )
        assertThat(result.name).isEqualTo(STEP_NAME)
        assertThat(result.state).isEqualTo(UploadHealingStep.State.RECOVERY_STARTED)
        assertThat(result.replacementRawImpressionUpload)
          .isEqualTo("$PARENT/rawImpressionUploads/replacement")
        assertThat(result.recoveryDoneBlobGeneration).isEqualTo(73L)
        assertThatProto(result.evictionCompleteTime).isEqualTo(NOW.toProtoTime())
        assertThatProto(result.recoveryStartTime).isEqualTo(NOW.plusSeconds(1).toProtoTime())
        assertThatProto(result.completeTime).isEqualTo(NOW.plusSeconds(2).toProtoTime())
        assertThat(result.etag).isEqualTo("step-etag")
      }
      val requests = argumentCaptor<InternalAdvanceStep>()
      verifyBlocking(operations, times(3)) { advanceUploadHealingStep(requests.capture()) }
      for ((index, request) in requests.allValues.withIndex()) {
        assertThatProto(request)
          .isEqualTo(
            internalAdvanceStep {
              dataProviderResourceId = "dp"
              uploadHealingOperationId = OPERATION_ID
              uploadHealingStepId = 1L
              action = InternalAdvanceStep.Action.forNumber(index + 1)
              replacementRawImpressionUploadResourceId = if (index == 2) "replacement" else ""
              recoveryDoneBlobGeneration = if (index == 1) 73L else 0L
              etag = "step-etag"
              requestId = REQUEST_ID
            }
          )
      }
    }

  @Test
  fun `existing operator reads remain registered on the same endpoint`() =
    runBlocking<Unit> {
      assertThat(
          candidateStub
            .getRawImpressionUploadCorrectionCandidate(
              getRawImpressionUploadCorrectionCandidateRequest { name = CANDIDATE_NAME }
            )
            .name
        )
        .isEqualTo(CANDIDATE_NAME)
      assertPublicOperation(
        operationStub.getUploadHealingOperation(
          getUploadHealingOperationRequest { name = OPERATION_NAME }
        )
      )
    }

  @Test
  fun `public mutation endpoint requires a client TLS certificate`() =
    runBlocking<Unit> {
      val noClientCertificate =
        NettyChannelBuilder.forAddress("localhost", server.port)
          .sslContext(
            GrpcSslContexts.forClient().trustManager(fixture("edp_aggregator_root.pem")).build()
          )
          .build()
      try {
        val failure =
          assertFailsWith<StatusException> {
            PublicCandidates.RawImpressionUploadCorrectionCandidateServiceCoroutineStub(
                noClientCertificate
              )
              .quarantineRawImpressionUpload(quarantine())
          }
        assertThat(failure.status.code).isEqualTo(Status.Code.UNAVAILABLE)
        verifyBlocking(candidates, never()) { createQuarantinedRawImpressionUpload(any()) }
      } finally {
        noClientCertificate.shutdownNow()
      }
    }

  @Test
  fun `quarantine rejects invalid identity and missing done metadata before forwarding`() =
    runBlocking<Unit> {
      for (request in
        listOf(
          quarantine().publicCopy { parent = "bad" },
          quarantine().publicCopy { rawImpressionUploadCorrectionCandidateId = "not-a-uuid" },
          quarantine().publicCopy { requestId = "11111111-1111-1111-8111-111111111111" },
          quarantine().publicCopy { doneBlobGeneration = 0L },
          quarantine().publicCopy { clearDoneBlobCreateTime() },
          quarantine().publicCopy { doneBlobUri = "" },
        )) {
        assertInvalid { candidateStub.quarantineRawImpressionUpload(request) }
      }
      verifyBlocking(candidates, never()) { createQuarantinedRawImpressionUpload(any()) }
    }

  @Test
  fun `activation rejects another provider with equal resource ID`() =
    runBlocking<Unit> {
      assertInvalid {
        candidateStub.activateRawImpressionUpload(
          activateRawImpressionUploadRequest {
            name = UPLOAD_NAME
            sourceRawImpressionUpload = "dataProviders/other/rawImpressionUploads/old"
            uploadHealingOperation = OPERATION_NAME
            requestId = REQUEST_ID
          }
        )
      }
      assertInvalid {
        candidateStub.activateRawImpressionUpload(
          activateRawImpressionUploadRequest {
            name = UPLOAD_NAME
            sourceRawImpressionUpload = "$PARENT/rawImpressionUploads/old"
            uploadHealingOperation = "dataProviders/other/uploadHealingOperations/$OPERATION_ID"
            requestId = REQUEST_ID
          }
        )
      }
      verifyBlocking(candidates, never()) { activateQuarantinedRawImpressionUpload(any()) }
    }

  @Test
  fun `registration rejects owners uploads and superseded candidates from another provider`() =
    runBlocking<Unit> {
      val base = registration()
      assertInvalid {
        candidateStub.registerRawImpressionUploadCorrectionCandidate(
          base.publicCopy {
            rawImpressionUpload = "dataProviders/other/rawImpressionUploads/upload"
          }
        )
      }
      assertInvalid {
        candidateStub.registerRawImpressionUploadCorrectionCandidate(
          base.publicCopy {
            supersededRawImpressionUploadCorrectionCandidate =
              "dataProviders/other/rawImpressionUploadCorrectionCandidates/$OTHER_CANDIDATE_ID"
          }
        )
      }
      assertInvalid {
        candidateStub.registerRawImpressionUploadCorrectionCandidate(
          base.publicCopy {
            manifestComparison =
              base.manifestComparison.publicCopy {
                priorManifest.clear()
                priorManifest +=
                  base.manifestComparison.priorManifestList.first().publicCopy {
                    outputSourceRawImpressionUpload = "dataProviders/other/rawImpressionUploads/old"
                  }
              }
          }
        )
      }
      verifyBlocking(candidates, never()) {
        registerDetectedRawImpressionUploadCorrectionCandidate(any())
      }
    }

  @Test
  fun `resolution and purge reject malformed requests`() =
    runBlocking<Unit> {
      assertInvalid {
        candidateStub.resolveRawImpressionUploadCorrectionCandidate(
          resolveRawImpressionUploadCorrectionCandidateRequest {
            name = "bad"
            requestId = REQUEST_ID
          }
        )
      }
      assertInvalid {
        candidateStub.resolveRawImpressionUploadCorrectionCandidate(
          resolveRawImpressionUploadCorrectionCandidateRequest { name = CANDIDATE_NAME }
        )
      }
      assertInvalid {
        candidateStub.purgeRawImpressionUploadCorrectionCandidates(
          purgeRawImpressionUploadCorrectionCandidatesRequest {
            parent = PARENT
            pageSize = -1
          }
        )
      }
      verifyBlocking(candidates, never()) {
        resolveDetectedRawImpressionUploadCorrectionCandidate(any())
      }
      verifyBlocking(candidates, never()) {
        purgeExpiredRawImpressionUploadCorrectionCandidates(any())
      }
    }

  @Test
  fun `reconcile rejects cross-provider plan references`() =
    runBlocking<Unit> {
      val base = reconcile()
      assertInvalid {
        operationStub.reconcileUploadHealingOperation(
          base.publicCopy {
            plan =
              base.plan.publicCopy {
                badRawImpressionUploads.clear()
                badRawImpressionUploads += "dataProviders/other/rawImpressionUploads/old"
              }
          }
        )
      }
      assertInvalid {
        operationStub.reconcileUploadHealingOperation(
          base.publicCopy {
            plan =
              base.plan.publicCopy {
                steps.clear()
                steps +=
                  base.plan.stepsList.single().publicCopy {
                    recoveryPredecessorRawImpressionUpload =
                      "dataProviders/other/rawImpressionUploads/previous"
                  }
              }
          }
        )
      }
      assertInvalid {
        operationStub.reconcileUploadHealingOperation(
          base.publicCopy {
            plan =
              base.plan.publicCopy {
                steps.clear()
                steps +=
                  base.plan.stepsList.single().publicCopy {
                    rawImpressionUploadModelLine =
                      "$PARENT/rawImpressionUploads/wrong/rawImpressionUploadModelLines/row"
                  }
              }
          }
        )
      }
      verifyBlocking(operations, never()) { reconcileUploadHealingOperation(any()) }
    }

  @Test
  fun `advance rejects missing concurrency identity and unspecified state`() =
    runBlocking<Unit> {
      assertInvalid {
        operationStub.advanceUploadHealingOperation(
          advanceUploadHealingOperationRequest {
            name = OPERATION_NAME
            requestId = REQUEST_ID
            state = UploadHealingOperation.State.EVICTING
          }
        )
      }
      assertInvalid {
        operationStub.advanceUploadHealingOperation(
          advanceUploadHealingOperationRequest {
            name = OPERATION_NAME
            requestId = REQUEST_ID
            etag = "etag"
          }
        )
      }
      verifyBlocking(operations, never()) { advanceUploadHealingOperation(any()) }
    }

  @Test
  fun `finalize rejects mismatched and duplicate step identities`() =
    runBlocking<Unit> {
      val base = finalization()
      assertInvalid {
        operationStub.finalizeUploadHealingOperation(
          base.publicCopy {
            stepManifests.clear()
            stepManifests +=
              base.stepManifestsList.single().publicCopy {
                uploadHealingStep =
                  "$PARENT/uploadHealingOperations/$OTHER_CANDIDATE_ID/uploadHealingSteps/1"
              }
          }
        )
      }
      assertInvalid {
        operationStub.finalizeUploadHealingOperation(
          base.publicCopy { stepManifests += base.stepManifestsList.single() }
        )
      }
      verifyBlocking(operations, never()) { finalizeUploadHealingOperation(any()) }
    }

  @Test
  fun `step advancement rejects invalid action ID and replacement provider`() =
    runBlocking<Unit> {
      assertInvalid {
        operationStub.advanceUploadHealingStep(
          advanceUploadHealingStepRequest {
            name = STEP_NAME
            etag = "etag"
            requestId = REQUEST_ID
          }
        )
      }
      assertInvalid {
        operationStub.advanceUploadHealingStep(
          advanceUploadHealingStepRequest {
            name = "$OPERATION_NAME/uploadHealingSteps/not-a-number"
            action = AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION
            etag = "etag"
            requestId = REQUEST_ID
          }
        )
      }
      assertInvalid {
        operationStub.advanceUploadHealingStep(
          advanceUploadHealingStepRequest {
            name = STEP_NAME
            action = AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT
            replacementRawImpressionUpload = "dataProviders/other/rawImpressionUploads/replacement"
            etag = "etag"
            requestId = REQUEST_ID
          }
        )
      }
      verifyBlocking(operations, never()) { advanceUploadHealingStep(any()) }
    }

  @Test
  fun `internal transient failures remain retryable through both public services`() =
    runBlocking<Unit> {
      for (code in
        listOf(
          Status.Code.UNAVAILABLE,
          Status.Code.DEADLINE_EXCEEDED,
          Status.Code.RESOURCE_EXHAUSTED,
          Status.Code.ABORTED,
          Status.Code.FAILED_PRECONDITION,
        )) {
        whenever(candidates.createQuarantinedRawImpressionUpload(any())).thenAnswer {
          throw Status.fromCode(code).asException()
        }
        whenever(operations.advanceUploadHealingOperation(any())).thenAnswer {
          throw Status.fromCode(code).asException()
        }
        assertThat(
            assertFailsWith<StatusException> {
                candidateStub.quarantineRawImpressionUpload(quarantine())
              }
              .status
              .code
          )
          .isEqualTo(code)
        assertThat(
            assertFailsWith<StatusException> {
                operationStub.advanceUploadHealingOperation(
                  advanceUploadHealingOperationRequest {
                    name = OPERATION_NAME
                    state = UploadHealingOperation.State.EVICTING
                    etag = "etag"
                    requestId = REQUEST_ID
                  }
                )
              }
              .status
              .code
          )
          .isEqualTo(code)
      }
    }

  @Test
  fun `registration requires an owner for every manifest entry`() =
    runBlocking<Unit> {
      val base = registration()
      val missingOwner =
        base.publicCopy {
          manifestComparison =
            base.manifestComparison.publicCopy {
              priorManifest.clear()
              priorManifest +=
                base.manifestComparison.priorManifestList.first().publicCopy {
                  clearOutputSourceRawImpressionUpload()
                }
            }
        }
      assertInvalid { candidateStub.registerRawImpressionUploadCorrectionCandidate(missingOwner) }
      verifyBlocking(candidates, never()) {
        registerDetectedRawImpressionUploadCorrectionCandidate(any())
      }
    }

  @Test
  fun `registration preserves unknown legacy prior and positive current generations`() =
    runBlocking<Unit> {
      val base = registration()
      candidateStub.registerRawImpressionUploadCorrectionCandidate(
        base.publicCopy {
          manifestComparison =
            base.manifestComparison.publicCopy {
              priorManifest.clear()
              priorManifest +=
                base.manifestComparison.priorManifestList.mapIndexed { index, entry ->
                  if (index == 0) entry.publicCopy { blobGeneration = 0L } else entry
                }
              differences.clear()
              differences +=
                base.manifestComparison.differencesList.map { difference ->
                  if (difference.hasPrior() && difference.prior.blobUri == "gs://raw/edited") {
                    difference.publicCopy {
                      prior = difference.prior.publicCopy { blobGeneration = 0L }
                    }
                  } else difference
                }
            }
        }
      )
      val requests = argumentCaptor<RegisterDetectedRawImpressionUploadCorrectionCandidateRequest>()
      verifyBlocking(candidates) {
        registerDetectedRawImpressionUploadCorrectionCandidate(requests.capture())
      }
      val comparison = requests.firstValue.rawImpressionUploadCorrectionCandidate.manifestComparison
      assertThat(comparison.priorManifestList.first().blobGeneration).isEqualTo(0L)
      assertThat(comparison.differencesList.first().prior.blobGeneration).isEqualTo(0L)
      assertThat(comparison.currentManifestList.first().blobGeneration).isEqualTo(20L)
    }

  @Test
  fun `checkpoint fields must match the selected action`() =
    runBlocking<Unit> {
      for (request in
        listOf(
          advanceUploadHealingStepRequest {
            name = STEP_NAME
            etag = "etag"
            requestId = REQUEST_ID
            action = AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION
            recoveryDoneBlobGeneration = 1L
          },
          advanceUploadHealingStepRequest {
            name = STEP_NAME
            etag = "etag"
            requestId = REQUEST_ID
            action = AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION
            replacementRawImpressionUpload = UPLOAD_NAME
          },
          advanceUploadHealingStepRequest {
            name = STEP_NAME
            etag = "etag"
            requestId = REQUEST_ID
            action = AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY
          },
          advanceUploadHealingStepRequest {
            name = STEP_NAME
            etag = "etag"
            requestId = REQUEST_ID
            action = AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY
            recoveryDoneBlobGeneration = 1L
            replacementRawImpressionUpload = UPLOAD_NAME
          },
          advanceUploadHealingStepRequest {
            name = STEP_NAME
            etag = "etag"
            requestId = REQUEST_ID
            action = AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT
          },
          advanceUploadHealingStepRequest {
            name = STEP_NAME
            etag = "etag"
            requestId = REQUEST_ID
            action = AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT
            replacementRawImpressionUpload = UPLOAD_NAME
            recoveryDoneBlobGeneration = 1L
          },
        )) assertInvalid { operationStub.advanceUploadHealingStep(request) }
      verifyBlocking(operations, never()) { advanceUploadHealingStep(any()) }
    }

  @Test
  fun `reconciliation validates required fields and provisional manifests`() =
    runBlocking<Unit> {
      val base = reconcile()
      for (plan in
        listOf(
          base.plan.publicCopy { reason = "" },
          base.plan.publicCopy { labeledImpressionsBlobPrefix = "" },
          base.plan.publicCopy { badRawImpressionUploads.clear() },
          base.plan.publicCopy { clearCutoffTime() },
          base.plan.publicCopy { rawImpressionUploadCorrectionCandidates.clear() },
          base.plan.publicCopy { planFormatVersion = 0 },
          base.plan.publicCopy {
            outputManifestState =
              UploadHealingOperation.OutputManifestState.OUTPUT_MANIFEST_STATE_FINALIZED
          },
          base.plan.publicCopy {
            steps.clear()
            steps +=
              base.plan.stepsList.single().publicCopy { labeledOutputManifest = PUBLIC_OUTPUT }
          },
        )) assertInvalid {
        operationStub.reconcileUploadHealingOperation(base.publicCopy { this.plan = plan })
      }
      verifyBlocking(operations, never()) { reconcileUploadHealingOperation(any()) }
    }

  @Test
  fun `finalization rejects malformed manifests and mutation identity`() =
    runBlocking<Unit> {
      val base = finalization()
      assertInvalid {
        operationStub.finalizeUploadHealingOperation(
          base.publicCopy { name = "$PARENT/uploadHealingOperations/not-a-uuid" }
        )
      }
      assertInvalid { operationStub.finalizeUploadHealingOperation(base.publicCopy { etag = " " }) }
      for (manifest in
        listOf(
          publicManifest {},
          publicManifest {
            blobs +=
              PublicManifestKt.blobVersion {
                blobUri = ""
                generation = 1L
              }
          },
          publicManifest {
            blobs +=
              PublicManifestKt.blobVersion {
                blobUri = "gs://labeled/file"
                generation = 0L
              }
          },
        )) assertInvalid {
        operationStub.finalizeUploadHealingOperation(
          base.publicCopy {
            stepManifests.clear()
            stepManifests +=
              base.stepManifestsList.single().publicCopy { labeledOutputManifest = manifest }
          }
        )
      }
      verifyBlocking(operations, never()) { finalizeUploadHealingOperation(any()) }
    }

  private suspend fun assertInvalid(block: suspend () -> Any) {
    assertThat(assertFailsWith<StatusException> { block() }.status.code)
      .isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  private fun assertPublicOperation(result: UploadHealingOperation) {
    assertThat(result.name).isEqualTo(OPERATION_NAME)
    assertThat(result.state).isEqualTo(UploadHealingOperation.State.EVICTING)
    assertThat(result.etag).isEqualTo("operation-etag")
    val step = result.stepsList.single()
    assertThat(step.name).isEqualTo(STEP_NAME)
    assertThat(step.sourceRawImpressionUpload).isEqualTo("$PARENT/rawImpressionUploads/old")
    assertThat(step.rawImpressionUploadModelLine)
      .isEqualTo("$PARENT/rawImpressionUploads/old/rawImpressionUploadModelLines/row")
    assertThat(step.rawImpressionUploadCorrectionCandidate).isEqualTo(CANDIDATE_NAME)
    assertThatProto(step.labeledOutputManifest).isEqualTo(PUBLIC_OUTPUT)
    assertThat(step.labeledOutputManifest.blobsList[1].hasGeneration()).isFalse()
  }

  private fun quarantine() = quarantineRawImpressionUploadRequest {
    parent = PARENT
    rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
    doneBlobUri = DONE
    doneBlobGeneration = 20L
    doneBlobCreateTime = NOW.toProtoTime()
    requestId = REQUEST_ID
  }

  private fun registration() = registerRawImpressionUploadCorrectionCandidateRequest {
    parent = PARENT
    rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
    rawImpressionUpload = UPLOAD_NAME
    classification = RawImpressionUploadCorrectionCandidate.Classification.MIXED
    priorManifestDigest = CANDIDATE.priorManifestDigest
    currentManifestDigest = CANDIDATE.currentManifestDigest
    expireTime = CANDIDATE.expireTime
    requestId = REQUEST_ID
    supersededRawImpressionUploadCorrectionCandidate =
      "$PARENT/rawImpressionUploadCorrectionCandidates/$OTHER_CANDIDATE_ID"
    fun entry(value: InternalCandidate.ManifestEntry) =
      PublicComparisonKt.manifestEntry {
        blobUri = value.blobUri
        blobGeneration = value.blobGeneration
        eventDate = value.eventDate
        outputSourceRawImpressionUpload =
          "$PARENT/rawImpressionUploads/${value.outputSourceRawImpressionUploadResourceId}"
      }
    manifestComparison =
      PublicComparisonKt.manifestComparison {
        priorManifest += COMPARISON.priorManifestList.map { entry(it) }
        currentManifest += COMPARISON.currentManifestList.map { entry(it) }
        differences +=
          COMPARISON.differencesList.map { difference ->
            PublicComparisonKt.manifestDifference {
              type =
                RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.forNumber(
                  difference.type.number
                )
              if (difference.hasPrior()) prior = entry(difference.prior)
              if (difference.hasCurrent()) current = entry(difference.current)
            }
          }
      }
  }

  private fun reconcile() = reconcileUploadHealingOperationRequest {
    parent = PARENT
    uploadHealingOperationId = OPERATION_ID
    etag = "draft-etag"
    requestId = REQUEST_ID
    plan =
      PublicPlanKt.plan {
        reason = OPERATION.reason
        labeledImpressionsBlobPrefix = OPERATION.labeledImpressionsBlobPrefix
        badRawImpressionUploads += "$PARENT/rawImpressionUploads/old"
        cutoffTime = OPERATION.cutoffTime
        rawImpressionUploadCorrectionCandidates += CANDIDATE_NAME
        planFormatVersion = 2
        outputManifestState =
          UploadHealingOperation.OutputManifestState.OUTPUT_MANIFEST_STATE_PROVISIONAL
        steps +=
          PublicPlanKt.step {
            sequenceNumber = 1L
            sourceRawImpressionUpload = "$PARENT/rawImpressionUploads/old"
            rawImpressionUploadModelLine =
              "$PARENT/rawImpressionUploads/old/rawImpressionUploadModelLines/row"
            cmmsModelLine = STEP.cmmsModelLine
            memoized = true
            recoveryAction =
              org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
                .RecoveryAction
                .RECOVERY_ACTION_EDP_CORRECTION
            recoveryPredecessorRawImpressionUpload = "$PARENT/rawImpressionUploads/previous"
            recoveryTarget = true
            labeledOutputManifest = PUBLIC_PROVISIONAL_OUTPUT
            rawImpressionUploadCorrectionCandidate = CANDIDATE_NAME
          }
      }
  }

  private fun finalization() = finalizeUploadHealingOperationRequest {
    name = OPERATION_NAME
    etag = "operation-etag"
    requestId = REQUEST_ID
    stepManifests +=
      PublicFinalizeKt.stepManifest {
        uploadHealingStep = STEP_NAME
        labeledOutputManifest = PUBLIC_OUTPUT
      }
  }

  companion object {
    private const val PARENT = "dataProviders/dp"
    private const val CANDIDATE_ID = "11111111-1111-4111-8111-111111111111"
    private const val OPERATION_ID = "22222222-2222-4222-8222-222222222222"
    private const val REQUEST_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    private const val OTHER_CANDIDATE_ID = "33333333-3333-4333-8333-333333333333"
    private const val CANDIDATE_NAME =
      "$PARENT/rawImpressionUploadCorrectionCandidates/$CANDIDATE_ID"
    private const val OPERATION_NAME = "$PARENT/uploadHealingOperations/$OPERATION_ID"
    private const val STEP_NAME = "$OPERATION_NAME/uploadHealingSteps/1"
    private const val UPLOAD_NAME = "$PARENT/rawImpressionUploads/upload"
    private const val DONE = "gs://raw/date/done"
    private val NOW = Instant.parse("2030-01-01T00:00:00Z")
    private val DATE_1 = date {
      year = 2026
      month = 1
      day = 1
    }
    private val DATE_2 = date {
      year = 2026
      month = 1
      day = 2
    }

    private fun fixture(name: String) =
      Paths.get(
          System.getenv("TEST_SRCDIR"),
          System.getenv("TEST_WORKSPACE"),
          "src/main/k8s/testing/secretfiles",
          name,
        )
        .toFile()

    private val INTERNAL_OUTPUT = internalManifest {
      blobs +=
        InternalManifestKt.blobVersion {
          blobUri = "gs://labeled/existing"
          generation = 51L
        }
      blobs += InternalManifestKt.blobVersion { blobUri = "gs://labeled/absent" }
    }
    private val PUBLIC_OUTPUT = publicManifest {
      blobs +=
        PublicManifestKt.blobVersion {
          blobUri = "gs://labeled/existing"
          generation = 51L
        }
      blobs += PublicManifestKt.blobVersion { blobUri = "gs://labeled/absent" }
    }
    private val PUBLIC_PROVISIONAL_OUTPUT =
      PUBLIC_OUTPUT.publicCopy {
        blobs.clear()
        blobs += PUBLIC_OUTPUT.blobsList.map { it.publicCopy { clearGeneration() } }
      }
    private val INTERNAL_PROVISIONAL_OUTPUT =
      INTERNAL_OUTPUT.copy {
        blobs.clear()
        blobs += INTERNAL_OUTPUT.blobsList.map { it.copy { clearGeneration() } }
      }
    private val UPLOAD = rawImpressionUpload {
      dataProviderResourceId = "dp"
      rawImpressionUploadResourceId = "upload"
      state = RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED
      doneBlobUri = DONE
      doneBlobGeneration = 20L
      doneBlobCreateTime = NOW.toProtoTime()
      registrationComplete = true
      processingDeferred = true
      replacesRawImpressionUploadResourceId = "old"
      evictionOperationId = OPERATION_ID
      correctionCandidateId = CANDIDATE_ID
      createTime = NOW.toProtoTime()
      updateTime = NOW.plusSeconds(1).toProtoTime()
      etag = "upload-etag"
    }
    private val COMPARISON =
      InternalComparisonKt.manifestComparison {
        val prior =
          InternalComparisonKt.manifestEntry {
            blobUri = "gs://raw/edited"
            blobGeneration = 10L
            eventDate = DATE_1
            outputSourceRawImpressionUploadResourceId = "old"
          }
        val current =
          prior.copy {
            blobGeneration = 20L
            eventDate = DATE_2
            outputSourceRawImpressionUploadResourceId = "upload"
          }
        val removed =
          prior.copy {
            blobUri = "gs://raw/removed"
            blobGeneration = 11L
          }
        val added =
          current.copy {
            blobUri = "gs://raw/added"
            blobGeneration = 30L
          }
        val unchanged =
          prior.copy {
            blobUri = "gs://raw/unchanged"
            blobGeneration = 5L
          }
        priorManifest += listOf(prior, removed, unchanged)
        currentManifest +=
          listOf(
            current,
            added,
            unchanged.copy { outputSourceRawImpressionUploadResourceId = "upload" },
          )
        differences +=
          InternalComparisonKt.manifestDifference {
            type = InternalCandidate.ManifestDifference.Type.TYPE_EVENT_DATE_CHANGED
            this.prior = prior
            this.current = current
          }
        differences +=
          InternalComparisonKt.manifestDifference {
            type = InternalCandidate.ManifestDifference.Type.TYPE_REMOVED
            this.prior = removed
          }
        differences +=
          InternalComparisonKt.manifestDifference {
            type = InternalCandidate.ManifestDifference.Type.TYPE_ADDED
            this.current = added
          }
      }
    private val CANDIDATE = rawImpressionUploadCorrectionCandidate {
      dataProviderResourceId = "dp"
      rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
      rawImpressionUploadResourceId = "upload"
      classification = InternalCandidate.Classification.CLASSIFICATION_MIXED
      priorManifestDigest = ByteString.copyFrom(ByteArray(32) { 1 })
      currentManifestDigest = ByteString.copyFrom(ByteArray(32) { 2 })
      manifestComparison = COMPARISON
      state = InternalCandidate.State.STATE_ASSIGNED
      decision = InternalCandidate.Decision.DECISION_APPLY_CANDIDATE
      uploadHealingOperationId = OPERATION_ID
      supersedingRawImpressionUploadCorrectionCandidateId = OTHER_CANDIDATE_ID
      expireTime = NOW.plusSeconds(86400).toProtoTime()
      createTime = NOW.toProtoTime()
      updateTime = NOW.toProtoTime()
      etag = "candidate-etag"
    }
    private val STEP = uploadHealingStep {
      uploadHealingStepId = 1L
      sequenceNumber = 1L
      sourceRawImpressionUploadResourceId = "old"
      rawImpressionUploadModelLineResourceId = "row"
      cmmsModelLine = "modelProviders/mp/modelSuites/ms/modelLines/line"
      memoized = true
      recoveryAction =
        RawImpressionUploadModelLineRecoveryAction
          .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_EDP_CORRECTION
      recoveryPredecessorRawImpressionUploadResourceId = "previous"
      recoveryTarget = true
      rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
      labeledOutputManifest = INTERNAL_OUTPUT
      state = InternalStep.State.UPLOAD_HEALING_STEP_STATE_RECOVERY_STARTED
      replacementRawImpressionUploadResourceId = "replacement"
      evictionCompleteTime = NOW.toProtoTime()
      recoveryStartTime = NOW.plusSeconds(1).toProtoTime()
      completeTime = NOW.plusSeconds(2).toProtoTime()
      updateTime = NOW.plusSeconds(3).toProtoTime()
      recoveryDoneBlobGeneration = 73L
      etag = "step-etag"
    }
    private val OPERATION = uploadHealingOperation {
      dataProviderResourceId = "dp"
      uploadHealingOperationId = OPERATION_ID
      state = InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING
      reason = "correct raw revision"
      labeledImpressionsBlobPrefix = "gs://labeled/"
      badRawImpressionUploadResourceIds += "old"
      cutoffTime = NOW.minusSeconds(86400).toProtoTime()
      steps += STEP
      rawImpressionUploadCorrectionCandidateIds += CANDIDATE_ID
      planFormatVersion = 2
      outputManifestState =
        InternalOperation.OutputManifestState.UPLOAD_HEALING_OUTPUT_MANIFEST_STATE_PROVISIONAL
      createTime = NOW.toProtoTime()
      updateTime = NOW.toProtoTime()
      etag = "operation-etag"
    }
  }
}
