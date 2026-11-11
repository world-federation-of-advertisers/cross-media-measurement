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

import com.google.api.client.testing.http.MockHttpTransport
import com.google.api.client.testing.http.MockLowLevelHttpResponse
import com.google.auth.oauth2.TokenVerifier
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat as assertThatProto
import com.google.devtools.build.runfiles.Runfiles
import com.google.gson.JsonObject
import com.google.type.date
import io.grpc.ManagedChannel
import io.grpc.Metadata
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.stub.MetadataUtils
import io.netty.handler.ssl.ClientAuth
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Paths
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.Signature
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.Base64
import java.util.concurrent.CopyOnWriteArrayList
import java.util.logging.Handler
import java.util.logging.LogRecord
import java.util.logging.Logger
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.RuleChain
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.never
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.withInterceptor
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.edpaggregator.vidLabelingMachineApiConfig
import org.wfanet.measurement.edpaggregator.deploy.common.server.RedactingLoggingServerInterceptor
import org.wfanet.measurement.edpaggregator.service.VidLabelingMachineApiConversions
import org.wfanet.measurement.edpaggregator.service.VidLabelingMachineAuthorizer
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledOutputManifestKt as PublicManifestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidateServiceGrpcKt as PublicCandidates
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine as PublicModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation as PublicOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt as PublicOperations
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingCorrectionDetectionServiceGrpcKt as Detection
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingHealingControllerServiceGrpcKt as Controller
import org.wfanet.measurement.edpaggregator.v1alpha.activateRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.copy as publicCopy
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.edpaggregator.v1alpha.labeledOutputManifest as publicManifest
import org.wfanet.measurement.edpaggregator.v1alpha.purgeRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.quarantineRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation as publicOperation
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingStep as publicStep
import org.wfanet.measurement.edpaggregator.vidlabeling.CorrectionDetectionClient
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.AdvanceUploadHealingOperationCommand
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.AdvanceUploadHealingStepCommand
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.FinalizeUploadHealingOperationCommand
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.ReconcileUploadHealingOperationCommand
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.SystemApiCorrectionCandidateCleaner
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.SystemApiHealingOperationStore
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.UploadHealingStepAction
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.UploadHealingStepManifest
import org.wfanet.measurement.internal.edpaggregator.ActivateQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingStepRequest as InternalAdvanceStep
import org.wfanet.measurement.internal.edpaggregator.CreateQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.FinalizeUploadHealingOperationRequest as InternalFinalize
import org.wfanet.measurement.internal.edpaggregator.FinalizeUploadHealingOperationRequestKt as InternalFinalizeKt
import org.wfanet.measurement.internal.edpaggregator.LabeledOutputManifestKt as InternalManifestKt
import org.wfanet.measurement.internal.edpaggregator.PurgeExpiredRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate as InternalCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateKt as InternalCandidateKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt as InternalCandidates
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction as InternalRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.ReconcileUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.RegisterDetectedRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.ResolveDetectedRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation as InternalOperation
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperationServiceGrpcKt as InternalOperations
import org.wfanet.measurement.internal.edpaggregator.UploadHealingStep as InternalStep
import org.wfanet.measurement.internal.edpaggregator.activateQuarantinedRawImpressionUploadRequest
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
import org.wfanet.measurement.internal.edpaggregator.uploadHealingStep as internalStep

@RunWith(JUnit4::class)
class VidLabelingMachineApiServicesTest {
  private val candidates:
    InternalCandidates.RawImpressionUploadCorrectionCandidateServiceCoroutineImplBase =
    mockService()
  private val operations: InternalOperations.UploadHealingOperationServiceCoroutineImplBase =
    mockService()
  private val internalServer = GrpcTestServerRule {
    addService(candidates)
    addService(operations)
  }
  private val insecureServer = GrpcTestServerRule {
    addService(
      object : Detection.VidLabelingCorrectionDetectionServiceCoroutineImplBase() {
          override suspend fun quarantineRawImpressionUpload(
            request:
              org.wfanet.measurement.edpaggregator.v1alpha.QuarantineRawImpressionUploadRequest
          ) = org.wfanet.measurement.edpaggregator.v1alpha.quarantineRawImpressionUploadResponse {}
        }
        .withInterceptor(VidLabelingMachineAuthorizer.Interceptor)
    )
  }
  @get:Rule val rules = RuleChain.outerRule(internalServer).around(insecureServer)
  private lateinit var server: CommonServer
  private lateinit var channel: ManagedChannel
  private val records = CopyOnWriteArrayList<String>()
  private val loggingHandler =
    object : Handler() {
      override fun publish(record: LogRecord) {
        records += record.message
      }

      override fun flush() {}

      override fun close() {}
    }

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
        .thenReturn(purgeExpiredRawImpressionUploadCorrectionCandidatesResponse { purgedCount = 0 })
      val config = vidLabelingMachineApiConfig {
        audience = AUDIENCE
        dispatcherServiceAccountSubjects += DISPATCHER
        healingControllerServiceAccountSubjects += HEALER
      }
      val verifier =
        TokenVerifier.newBuilder()
          .setAudience(AUDIENCE)
          .setIssuer(VidLabelingMachineAuthorizer.GOOGLE_ID_TOKEN_ISSUER)
          .setPublicKey(KEY_PAIR.public)
          .setClock { CLOCK.millis() }
          .build()
      LOGGER.addHandler(loggingHandler)
      startPublicServer(VidLabelingMachineAuthorizer(config, CLOCK, verifier))
    }

  private fun startPublicServer(authorizer: VidLabelingMachineAuthorizer) {
    val machineServices =
      VidLabelingMachineApiServices.build(
        internalServer.channel,
        authorizer,
        VidLabelingRpcThrottlersTestHelper.alwaysReady(),
      )
    val publicServices =
      Services.build(internalServer.channel, EmptyCoroutineContext).toList().map {
        it.bindService()
      }
    server =
      CommonServer.fromParameters(
          verboseGrpcLogging = false,
          certs =
            SigningCerts.fromPemFiles(
              fixture("edp_aggregator_tls.pem"),
              fixture("edp_aggregator_tls.key"),
              fixture("edp_aggregator_trusted_certs.pem"),
            ),
          clientAuth = ClientAuth.REQUIRE,
          nameForLogging = "MachineGatewayTest",
          services =
            (machineServices + publicServices).map {
              it.withInterceptor(RedactingLoggingServerInterceptor)
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
    LOGGER.removeHandler(loggingHandler)
  }

  private fun detection(
    token: String? = token(DISPATCHER)
  ): Detection.VidLabelingCorrectionDetectionServiceCoroutineStub {
    val stub = Detection.VidLabelingCorrectionDetectionServiceCoroutineStub(channel)
    return if (token == null) stub
    else stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers(token)))
  }

  private fun controller(token: String = token(HEALER)) =
    Controller.VidLabelingHealingControllerServiceCoroutineStub(channel)
      .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers(token)))

  private fun quarantine() = quarantineRawImpressionUploadRequest {
    parent = PARENT
    rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
    doneBlobUri = "gs://raw/date/done"
    doneBlobGeneration = 20L
    doneBlobCreateTime = NOW.toProtoTime()
    requestId = REQUEST_ID
  }

  @Test
  fun `dispatcher gateway preserves quarantine request and response`() = runBlocking {
    val request = createQuarantinedRawImpressionUploadRequest {
      dataProviderResourceId = "edp123"
      rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
      rawImpressionUpload = rawImpressionUpload {
        doneBlobUri = "gs://raw/date/done"
        doneBlobGeneration = 20L
        doneBlobCreateTime = NOW.toProtoTime()
      }
      requestId = REQUEST_ID
    }
    val result =
      CorrectionDetectionClient(detection()).createQuarantinedRawImpressionUpload(request)
    val captor = argumentCaptor<CreateQuarantinedRawImpressionUploadRequest>()
    verifyBlocking(candidates) { createQuarantinedRawImpressionUpload(captor.capture()) }
    assertThatProto(captor.firstValue).isEqualTo(request)
    assertThatProto(result).isEqualTo(UPLOAD)
  }

  @Test
  fun `dispatcher gateway preserves complete registration manifests`() = runBlocking {
    val request = registerDetectedRawImpressionUploadCorrectionCandidateRequest {
      dataProviderResourceId = "edp123"
      rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
      rawImpressionUploadCorrectionCandidate =
        CANDIDATE.copy {
          clearDataProviderResourceId()
          clearRawImpressionUploadCorrectionCandidateId()
          clearState()
          clearDecision()
          clearCreateTime()
          clearUpdateTime()
          clearEtag()
        }
      requestId = REQUEST_ID
      supersededRawImpressionUploadCorrectionCandidateId = "44444444-4444-4444-8444-444444444444"
    }
    val persisted =
      CANDIDATE.copy {
        state = InternalCandidate.State.STATE_SUPERSEDED
        decision = InternalCandidate.Decision.DECISION_APPLY_CANDIDATE
        uploadHealingOperationId = OPERATION_ID
        supersedingRawImpressionUploadCorrectionCandidateId = "33333333-3333-4333-8333-333333333333"
      }
    whenever(candidates.registerDetectedRawImpressionUploadCorrectionCandidate(any()))
      .thenReturn(
        registerDetectedRawImpressionUploadCorrectionCandidateResponse {
          rawImpressionUploadCorrectionCandidate = persisted
          newlyCreated = true
        },
        registerDetectedRawImpressionUploadCorrectionCandidateResponse {
          rawImpressionUploadCorrectionCandidate = persisted
          newlyCreated = false
        },
      )
    val result =
      CorrectionDetectionClient(detection())
        .registerDetectedRawImpressionUploadCorrectionCandidate(request)
    val retry =
      CorrectionDetectionClient(detection())
        .registerDetectedRawImpressionUploadCorrectionCandidate(request)
    val captor = argumentCaptor<RegisterDetectedRawImpressionUploadCorrectionCandidateRequest>()
    verifyBlocking(candidates, org.mockito.kotlin.times(2)) {
      registerDetectedRawImpressionUploadCorrectionCandidate(captor.capture())
    }
    assertThatProto(captor.firstValue).isEqualTo(request)
    assertThatProto(result.rawImpressionUploadCorrectionCandidate).isEqualTo(persisted)
    assertThat(result.newlyCreated).isTrue()
    assertThatProto(captor.secondValue).isEqualTo(request)
    assertThatProto(retry.rawImpressionUploadCorrectionCandidate).isEqualTo(persisted)
    assertThat(retry.newlyCreated).isFalse()
  }

  @Test
  fun `unlisted Google identity cannot quarantine even with accepted TLS credentials`() =
    runBlocking<Unit> {
      val error =
        assertFailsWith<StatusException> {
          detection(token("999999999999999999999")).quarantineRawImpressionUpload(quarantine())
        }
      assertThat(error.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
      verifyBlocking(candidates, never()) { createQuarantinedRawImpressionUpload(any()) }
    }

  @Test
  fun `TLS credentials alone cannot call a machine operation`() =
    runBlocking<Unit> {
      val error =
        assertFailsWith<StatusException> {
          detection(null).quarantineRawImpressionUpload(quarantine())
        }
      assertThat(error.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
      verifyBlocking(candidates, never()) { createQuarantinedRawImpressionUpload(any()) }
    }

  @Test
  fun `controller identity cannot call dispatcher operations`() =
    runBlocking<Unit> {
      val error =
        assertFailsWith<StatusException> {
          detection(token(HEALER)).quarantineRawImpressionUpload(quarantine())
        }
      assertThat(error.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
      verifyBlocking(candidates, never()) { createQuarantinedRawImpressionUpload(any()) }
    }

  @Test
  fun `dispatcher identity cannot call controller operations`() =
    runBlocking<Unit> {
      val error =
        assertFailsWith<StatusException> {
          controller(token(DISPATCHER))
            .purgeRawImpressionUploadCorrectionCandidates(
              purgeRawImpressionUploadCorrectionCandidatesRequest { parent = PARENT }
            )
        }
      assertThat(error.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
      verifyBlocking(candidates, never()) {
        purgeExpiredRawImpressionUploadCorrectionCandidates(any())
      }
    }

  @Test
  fun `machine operations reject invalid signature audience issuer expiration and subject`() =
    runBlocking<Unit> {
      val invalidTokens =
        listOf(
          token(DISPATCHER, keyPair = OTHER_KEY_PAIR),
          token(DISPATCHER, audience = "https://another-environment.example"),
          token(DISPATCHER, issuer = "https://untrusted.example"),
          token(DISPATCHER, expiration = NOW.epochSecond),
          token(DISPATCHER, expiration = null),
          token(null),
          "not-a-jwt",
        )
      for (invalid in invalidTokens) {
        val error =
          assertFailsWith<StatusException> {
            detection(invalid).quarantineRawImpressionUpload(quarantine())
          }
        assertThat(error.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
      }
      verifyBlocking(candidates, never()) { createQuarantinedRawImpressionUpload(any()) }
    }

  @Test
  fun `activation rejects another DataProvider with the same upload ID`() =
    runBlocking<Unit> {
      val error =
        assertFailsWith<StatusException> {
          detection()
            .activateRawImpressionUpload(
              activateRawImpressionUploadRequest {
                name = "$PARENT/rawImpressionUploads/upload-1"
                sourceRawImpressionUpload = "dataProviders/other/rawImpressionUploads/upload-1"
                uploadHealingOperation = "$PARENT/uploadHealingOperations/$OPERATION_ID"
                requestId = REQUEST_ID
              }
            )
        }
      assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      verifyBlocking(candidates, never()) { activateQuarantinedRawImpressionUpload(any()) }
    }

  @Test
  fun `registration rejects a manifest owner from another DataProvider`() =
    runBlocking<Unit> {
      val request =
        VidLabelingMachineApiConversions.registrationToPublic(
            registerDetectedRawImpressionUploadCorrectionCandidateRequest {
              dataProviderResourceId = "edp123"
              rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
              rawImpressionUploadCorrectionCandidate = CANDIDATE
              requestId = REQUEST_ID
            }
          )
          .publicCopy {
            val old = manifestComparison
            manifestComparison =
              org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadManifestComparison {
                priorManifest +=
                  old.priorManifestList.map {
                    it.publicCopy {
                      outputSourceRawImpressionUpload =
                        "dataProviders/other/rawImpressionUploads/owner"
                    }
                  }
                currentManifest += old.currentManifestList
                differences += old.differencesList
              }
          }
      val error =
        assertFailsWith<StatusException> {
          detection().registerRawImpressionUploadCorrectionCandidate(request)
        }
      assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      verifyBlocking(candidates, never()) {
        registerDetectedRawImpressionUploadCorrectionCandidate(any())
      }
    }

  @Test
  fun `ordinary candidate Get remains available without a machine token`() = runBlocking {
    val result =
      PublicCandidates.RawImpressionUploadCorrectionCandidateServiceCoroutineStub(channel)
        .getRawImpressionUploadCorrectionCandidate(
          getRawImpressionUploadCorrectionCandidateRequest {
            name = "$PARENT/rawImpressionUploadCorrectionCandidates/$CANDIDATE_ID"
          }
        )
    assertThat(result.name)
      .isEqualTo("$PARENT/rawImpressionUploadCorrectionCandidates/$CANDIDATE_ID")
  }

  @Test
  fun `verbose RPC logging does not expose the bearer token`() = runBlocking {
    val bearer = token(DISPATCHER)
    detection(bearer).quarantineRawImpressionUpload(quarantine())
    assertThat(records).isNotEmpty()
    assertThat(records.joinToString("\n")).doesNotContain(bearer)
    assertThat(records.joinToString("\n")).doesNotContain("Bearer ")
  }

  @Test
  fun `transient internal failures remain retryable through the gateway`() =
    runBlocking<Unit> {
      whenever(candidates.createQuarantinedRawImpressionUpload(any())).thenAnswer {
        throw StatusException(Status.UNAVAILABLE)
      }
      val error =
        assertFailsWith<StatusException> { detection().quarantineRawImpressionUpload(quarantine()) }
      assertThat(error.status.code).isEqualTo(Status.Code.UNAVAILABLE)
    }

  @Test
  fun `controller gateway preserves draft identity and advances through the internal API`() =
    runBlocking {
      val persisted = uploadHealingOperation {
        dataProviderResourceId = "edp123"
        uploadHealingOperationId = OPERATION_ID
        state = InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
        reason = "Planning needs attention"
        labeledImpressionsBlobPrefix = "gs://labeled/"
        badRawImpressionUploadResourceIds += "owner"
        rawImpressionUploadCorrectionCandidateIds += CANDIDATE_ID
        cutoffTime = NOW.toProtoTime()
        createTime = NOW.toProtoTime()
        updateTime = NOW.toProtoTime()
        etag = "operation-etag"
        planFormatVersion = 2
        outputManifestState =
          InternalOperation.OutputManifestState.forNumber(
            PublicOperation.OutputManifestState.OUTPUT_MANIFEST_STATE_PROVISIONAL.number
          )
      }
      whenever(operations.reconcileUploadHealingOperation(any())).thenReturn(persisted)
      whenever(operations.getUploadHealingOperation(any())).thenReturn(persisted)
      val store =
        SystemApiHealingOperationStore(
          PublicOperations.UploadHealingOperationServiceCoroutineStub(channel),
          controller(),
          VidLabelingRpcThrottlersTestHelper.alwaysReady(),
        )
      val draft = publicOperation {
        reason = "Planning needs attention"
        labeledImpressionsBlobPrefix = "gs://labeled/"
        badRawImpressionUploads += "$PARENT/rawImpressionUploads/owner"
        rawImpressionUploadCorrectionCandidates +=
          "$PARENT/rawImpressionUploadCorrectionCandidates/$CANDIDATE_ID"
        cutoffTime = NOW.toProtoTime()
        planFormatVersion = 2
        outputManifestState = PublicOperation.OutputManifestState.OUTPUT_MANIFEST_STATE_PROVISIONAL
      }
      val result =
        store.reconcileUploadHealingOperation(
          ReconcileUploadHealingOperationCommand(
            PARENT,
            draft,
            OPERATION_ID,
            requestId = REQUEST_ID,
          )
        )
      val captor = argumentCaptor<ReconcileUploadHealingOperationRequest>()
      verifyBlocking(operations) { reconcileUploadHealingOperation(captor.capture()) }
      assertThat(captor.firstValue.dataProviderResourceId).isEqualTo("edp123")
      assertThat(captor.firstValue.uploadHealingOperationId).isEqualTo(OPERATION_ID)
      assertThat(captor.firstValue.requestId).isEqualTo(REQUEST_ID)
      assertThat(captor.firstValue.uploadHealingOperation.badRawImpressionUploadResourceIdsList)
        .containsExactly("owner")
      assertThat(
          captor.firstValue.uploadHealingOperation.rawImpressionUploadCorrectionCandidateIdsList
        )
        .containsExactly(CANDIDATE_ID)
      assertThat(result.name).isEqualTo("$PARENT/uploadHealingOperations/$OPERATION_ID")
      assertThat(result.state).isEqualTo(PublicOperation.State.NEEDS_ATTENTION)

      val evicting =
        persisted.copy { state = InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING }
      whenever(operations.advanceUploadHealingOperation(any())).thenReturn(evicting)
      whenever(operations.getUploadHealingOperation(any())).thenReturn(evicting)
      val advanced =
        store.advanceUploadHealingOperation(
          AdvanceUploadHealingOperationCommand(
            result.name,
            "operation-etag",
            PublicOperation.State.EVICTING,
            REQUEST_ID,
          )
        )
      val advance = argumentCaptor<AdvanceUploadHealingOperationRequest>()
      verifyBlocking(operations) { advanceUploadHealingOperation(advance.capture()) }
      assertThat(advance.firstValue.state)
        .isEqualTo(InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING)
      assertThat(advance.firstValue.etag).isEqualTo("operation-etag")
      assertThat(advanced.state).isEqualTo(PublicOperation.State.EVICTING)
    }

  @Test
  fun `controller cleanup preserves pagination through the system API`() = runBlocking {
    whenever(candidates.purgeExpiredRawImpressionUploadCorrectionCandidates(any()))
      .thenReturn(
        purgeExpiredRawImpressionUploadCorrectionCandidatesResponse { purgedCount = 100 },
        purgeExpiredRawImpressionUploadCorrectionCandidatesResponse { purgedCount = 1 },
      )
    SystemApiCorrectionCandidateCleaner(
        controller(),
        VidLabelingRpcThrottlersTestHelper.alwaysReady(),
      )
      .purge(PARENT)
    val captor = argumentCaptor<PurgeExpiredRawImpressionUploadCorrectionCandidatesRequest>()
    verifyBlocking(candidates, org.mockito.kotlin.times(2)) {
      purgeExpiredRawImpressionUploadCorrectionCandidates(captor.capture())
    }
    for (request in captor.allValues) {
      assertThat(request.dataProviderResourceId).isEqualTo("edp123")
      assertThat(request.pageSize).isEqualTo(100)
    }
  }

  @Test
  fun `machine transport rejects an ID token without mutual TLS`() =
    runBlocking<Unit> {
      val stub =
        Detection.VidLabelingCorrectionDetectionServiceCoroutineStub(insecureServer.channel)
          .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers(token(DISPATCHER))))
      val error =
        assertFailsWith<StatusException> { stub.quarantineRawImpressionUpload(quarantine()) }
      assertThat(error.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    }

  @Test
  fun `Google verification key lookup failure remains retryable`() =
    runBlocking<Unit> {
      channel.shutdownNow()
      server.shutdown()
      val config = vidLabelingMachineApiConfig {
        audience = AUDIENCE
        dispatcherServiceAccountSubjects += DISPATCHER
        healingControllerServiceAccountSubjects += HEALER
      }
      val response =
        MockLowLevelHttpResponse()
          .setStatusCode(503)
          .setContentType("application/json")
          .setContent("{}")
      val verifier =
        TokenVerifier.newBuilder()
          .setAudience(AUDIENCE)
          .setIssuer(VidLabelingMachineAuthorizer.GOOGLE_ID_TOKEN_ISSUER)
          .setClock { CLOCK.millis() }
          .setCertificatesLocation("https://keys.example.test")
          .setHttpTransportFactory {
            MockHttpTransport.Builder().setLowLevelHttpResponse(response).build()
          }
          .build()
      startPublicServer(VidLabelingMachineAuthorizer(config, CLOCK, verifier))
      val error =
        assertFailsWith<StatusException> { detection().quarantineRawImpressionUpload(quarantine()) }
      assertThat(error.status.code).isEqualTo(Status.Code.UNAVAILABLE)
      verifyBlocking(candidates, never()) { createQuarantinedRawImpressionUpload(any()) }
    }

  @Test
  fun `dispatcher activation preserves request and all upload lifecycle fields`() = runBlocking {
    val activated =
      UPLOAD.copy {
        state = RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED
        registrationComplete = true
        processingDeferred = false
        replacesRawImpressionUploadResourceId = "owner"
        evictionOperationId = OPERATION_ID
      }
    whenever(candidates.activateQuarantinedRawImpressionUpload(any())).thenReturn(activated)
    val request = activateQuarantinedRawImpressionUploadRequest {
      dataProviderResourceId = "edp123"
      rawImpressionUploadResourceId = "upload-1"
      sourceRawImpressionUploadResourceId = "owner"
      evictionOperationId = OPERATION_ID
      requestId = REQUEST_ID
    }

    val result =
      CorrectionDetectionClient(detection()).activateQuarantinedRawImpressionUpload(request)

    val captor = argumentCaptor<ActivateQuarantinedRawImpressionUploadRequest>()
    verifyBlocking(candidates) { activateQuarantinedRawImpressionUpload(captor.capture()) }
    assertThatProto(captor.firstValue).isEqualTo(request)
    assertThatProto(result).isEqualTo(activated)
  }

  @Test
  fun `dispatcher resolution preserves candidate identity request ID and retry result`() =
    runBlocking {
      whenever(candidates.resolveDetectedRawImpressionUploadCorrectionCandidate(any()))
        .thenReturn(
          resolveDetectedRawImpressionUploadCorrectionCandidateResponse { newlyResolved = true },
          resolveDetectedRawImpressionUploadCorrectionCandidateResponse { newlyResolved = false },
        )
      val request = resolveDetectedRawImpressionUploadCorrectionCandidateRequest {
        dataProviderResourceId = "edp123"
        rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
        requestId = REQUEST_ID
      }

      val result =
        CorrectionDetectionClient(detection())
          .resolveDetectedRawImpressionUploadCorrectionCandidate(request)

      val retry =
        CorrectionDetectionClient(detection())
          .resolveDetectedRawImpressionUploadCorrectionCandidate(request)
      val captor = argumentCaptor<ResolveDetectedRawImpressionUploadCorrectionCandidateRequest>()
      verifyBlocking(candidates, org.mockito.kotlin.times(2)) {
        resolveDetectedRawImpressionUploadCorrectionCandidate(captor.capture())
      }
      assertThatProto(captor.firstValue).isEqualTo(request)
      assertThat(result.newlyResolved).isTrue()
      assertThatProto(captor.secondValue).isEqualTo(request)
      assertThat(retry.newlyResolved).isFalse()
    }

  @Test
  fun `controller finalization preserves exact manifests and generation presence through gateway`() =
    runBlocking {
      val persisted = operationWithStep()
      whenever(operations.finalizeUploadHealingOperation(any())).thenReturn(persisted)
      whenever(operations.getUploadHealingOperation(any())).thenReturn(persisted)
      val command =
        FinalizeUploadHealingOperationCommand(
          "$PARENT/uploadHealingOperations/$OPERATION_ID",
          listOf(
            UploadHealingStepManifest(
              "$PARENT/uploadHealingOperations/$OPERATION_ID/uploadHealingSteps/1",
              OUTPUT_MANIFEST,
            )
          ),
          "operation-etag",
          REQUEST_ID,
        )

      val result = machineStore().finalizeUploadHealingOperation(command)

      val expected = internalFinalize {
        dataProviderResourceId = "edp123"
        uploadHealingOperationId = OPERATION_ID
        stepManifests +=
          InternalFinalizeKt.stepManifest {
            uploadHealingStepId = 1L
            labeledOutputManifest = INTERNAL_OUTPUT_MANIFEST
          }
        etag = command.etag
        requestId = REQUEST_ID
      }
      val captor = argumentCaptor<InternalFinalize>()
      verifyBlocking(operations) { finalizeUploadHealingOperation(captor.capture()) }
      assertThatProto(captor.firstValue).isEqualTo(expected)
      assertThat(result.name).isEqualTo(command.name)
      assertThat(result.etag).isEqualTo("operation-etag")
      assertThatProto(result.stepsList.single().labeledOutputManifest).isEqualTo(OUTPUT_MANIFEST)
      assertThat(result.stepsList.single().labeledOutputManifest.blobsList[1].hasGeneration())
        .isFalse()
    }

  @Test
  fun `controller confirms eviction through gateway without changing step identity`() =
    runBlocking {
      assertStepCheckpoint(
        UploadHealingStepAction.CONFIRM_EVICTION,
        InternalAdvanceStep.Action.CONFIRM_EVICTION,
      )
    }

  @Test
  fun `controller records exact recovery generation through gateway`() = runBlocking {
    assertStepCheckpoint(
      UploadHealingStepAction.RECORD_RECOVERY,
      InternalAdvanceStep.Action.RECORD_RECOVERY,
    )
  }

  @Test
  fun `controller confirms full replacement upload identity through gateway`() = runBlocking {
    assertStepCheckpoint(
      UploadHealingStepAction.CONFIRM_REPLACEMENT,
      InternalAdvanceStep.Action.CONFIRM_REPLACEMENT,
    )
  }

  @Test
  fun `controller reconciliation preserves full stepped draft through gateway`() = runBlocking {
    val persisted = operationWithStep()
    whenever(operations.reconcileUploadHealingOperation(any())).thenReturn(persisted)
    whenever(operations.getUploadHealingOperation(any())).thenReturn(persisted)
    val draft = publicOperation {
      name = "$PARENT/uploadHealingOperations/$OPERATION_ID"
      reason = "Edited raw manifest"
      labeledImpressionsBlobPrefix = "gs://labeled/"
      badRawImpressionUploads += "$PARENT/rawImpressionUploads/owner"
      rawImpressionUploadCorrectionCandidates +=
        "$PARENT/rawImpressionUploadCorrectionCandidates/$CANDIDATE_ID"
      cutoffTime = NOW.toProtoTime()
      planFormatVersion = 2
      outputManifestState = PublicOperation.OutputManifestState.OUTPUT_MANIFEST_STATE_PROVISIONAL
      steps += publicStep {
        name = "$PARENT/uploadHealingOperations/$OPERATION_ID/uploadHealingSteps/1"
        sequenceNumber = 1L
        sourceRawImpressionUpload = "$PARENT/rawImpressionUploads/owner"
        rawImpressionUploadModelLine =
          "$PARENT/rawImpressionUploads/owner/rawImpressionUploadModelLines/line1"
        cmmsModelLine = "modelProviders/mp/modelSuites/ms/modelLines/ml"
        memoized = true
        recoveryAction = PublicModelLine.RecoveryAction.forNumber(1)
        recoveryPredecessorRawImpressionUpload = "$PARENT/rawImpressionUploads/previous"
        recoveryTarget = true
        rawImpressionUploadCorrectionCandidate =
          "$PARENT/rawImpressionUploadCorrectionCandidates/$CANDIDATE_ID"
        labeledOutputManifest = OUTPUT_MANIFEST
      }
    }

    val result =
      machineStore()
        .reconcileUploadHealingOperation(
          ReconcileUploadHealingOperationCommand(
            PARENT,
            draft,
            OPERATION_ID,
            "draft-etag",
            REQUEST_ID,
          )
        )

    val expected = internalReconcile {
      dataProviderResourceId = "edp123"
      uploadHealingOperationId = OPERATION_ID
      etag = "draft-etag"
      requestId = REQUEST_ID
      uploadHealingOperation = uploadHealingOperation {
        state = InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
        reason = "Edited raw manifest"
        labeledImpressionsBlobPrefix = "gs://labeled/"
        badRawImpressionUploadResourceIds += "owner"
        rawImpressionUploadCorrectionCandidateIds += CANDIDATE_ID
        cutoffTime = NOW.toProtoTime()
        planFormatVersion = 2
        outputManifestState =
          InternalOperation.OutputManifestState.forNumber(draft.outputManifestState.number)
        steps += internalStep {
          uploadHealingStepId = 1L
          sequenceNumber = 1L
          sourceRawImpressionUploadResourceId = "owner"
          rawImpressionUploadModelLineResourceId = "line1"
          cmmsModelLine = "modelProviders/mp/modelSuites/ms/modelLines/ml"
          memoized = true
          recoveryAction = InternalRecoveryAction.forNumber(1)
          recoveryPredecessorRawImpressionUploadResourceId = "previous"
          recoveryTarget = true
          rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
          labeledOutputManifest = INTERNAL_OUTPUT_MANIFEST
        }
      }
    }
    val captor = argumentCaptor<ReconcileUploadHealingOperationRequest>()
    verifyBlocking(operations) { reconcileUploadHealingOperation(captor.capture()) }
    assertThatProto(captor.firstValue).isEqualTo(expected)
    assertThat(result.name).isEqualTo(draft.name)
    assertThatProto(result.stepsList.single().labeledOutputManifest).isEqualTo(OUTPUT_MANIFEST)
  }

  private suspend fun assertStepCheckpoint(
    action: UploadHealingStepAction,
    internalAction: InternalAdvanceStep.Action,
  ) {
    val persisted = operationWithStep()
    whenever(operations.advanceUploadHealingStep(any())).thenReturn(persisted.stepsList.single())
    whenever(operations.getUploadHealingOperation(any())).thenReturn(persisted)
    val command =
      AdvanceUploadHealingStepCommand(
        "$PARENT/uploadHealingOperations/$OPERATION_ID/uploadHealingSteps/1",
        "step-etag",
        action,
        replacementRawImpressionUpload =
          if (action == UploadHealingStepAction.CONFIRM_REPLACEMENT)
            "$PARENT/rawImpressionUploads/replacement"
          else "",
        recoveryDoneBlobGeneration =
          if (action == UploadHealingStepAction.RECORD_RECOVERY) 73L else 0L,
        requestId = REQUEST_ID,
      )

    val result = machineStore().advanceUploadHealingStep(command)

    val expected = internalAdvanceStep {
      dataProviderResourceId = "edp123"
      uploadHealingOperationId = OPERATION_ID
      uploadHealingStepId = 1L
      this.action = internalAction
      etag = "step-etag"
      requestId = REQUEST_ID
      replacementRawImpressionUploadResourceId =
        if (action == UploadHealingStepAction.CONFIRM_REPLACEMENT) "replacement" else ""
      recoveryDoneBlobGeneration = command.recoveryDoneBlobGeneration
    }
    val captor = argumentCaptor<InternalAdvanceStep>()
    verifyBlocking(operations) { advanceUploadHealingStep(captor.capture()) }
    assertThatProto(captor.firstValue).isEqualTo(expected)
    assertThat(result.name).isEqualTo(command.name)
    assertThat(result.sourceRawImpressionUpload).isEqualTo("$PARENT/rawImpressionUploads/owner")
    assertThat(result.replacementRawImpressionUpload)
      .isEqualTo("$PARENT/rawImpressionUploads/replacement")
    assertThat(result.rawImpressionUploadCorrectionCandidate)
      .isEqualTo("$PARENT/rawImpressionUploadCorrectionCandidates/$CANDIDATE_ID")
    assertThat(result.recoveryDoneBlobGeneration).isEqualTo(73L)
    assertThat(result.etag).isEqualTo("step-result-etag")
    assertThat(result.state.number)
      .isEqualTo(InternalStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE.number)
    assertThat(result.sequenceNumber).isEqualTo(1L)
    assertThat(result.rawImpressionUploadModelLine)
      .isEqualTo("$PARENT/rawImpressionUploads/owner/rawImpressionUploadModelLines/line1")
    assertThatProto(result.updateTime).isEqualTo(NOW.plusSeconds(2).toProtoTime())
    assertThatProto(result.evictionCompleteTime).isEqualTo(NOW.toProtoTime())
    assertThatProto(result.recoveryStartTime).isEqualTo(NOW.plusSeconds(1).toProtoTime())
    assertThatProto(result.completeTime).isEqualTo(NOW.plusSeconds(2).toProtoTime())
    assertThatProto(result.labeledOutputManifest).isEqualTo(OUTPUT_MANIFEST)
  }

  private fun machineStore() =
    SystemApiHealingOperationStore(
      PublicOperations.UploadHealingOperationServiceCoroutineStub(channel),
      controller(),
      VidLabelingRpcThrottlersTestHelper.alwaysReady(),
    )

  private fun operationWithStep() = uploadHealingOperation {
    dataProviderResourceId = "edp123"
    uploadHealingOperationId = OPERATION_ID
    state = InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_COMPLETE
    etag = "operation-etag"
    planFormatVersion = 2
    steps += internalStep {
      uploadHealingStepId = 1L
      sequenceNumber = 1L
      sourceRawImpressionUploadResourceId = "owner"
      rawImpressionUploadModelLineResourceId = "line1"
      cmmsModelLine = "modelProviders/mp/modelSuites/ms/modelLines/ml"
      state = InternalStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE
      replacementRawImpressionUploadResourceId = "replacement"
      rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
      recoveryDoneBlobGeneration = 73L
      evictionCompleteTime = NOW.toProtoTime()
      recoveryStartTime = NOW.plusSeconds(1).toProtoTime()
      completeTime = NOW.plusSeconds(2).toProtoTime()
      updateTime = NOW.plusSeconds(2).toProtoTime()
      etag = "step-result-etag"
      labeledOutputManifest = INTERNAL_OUTPUT_MANIFEST
    }
  }

  companion object {
    private const val PARENT = "dataProviders/edp123"
    private const val AUDIENCE = "https://system.example/vid-labeling-machine-api"
    private const val DISPATCHER = "111111111111111111111"
    private const val HEALER = "222222222222222222222"
    private const val CANDIDATE_ID = "11111111-1111-4111-8111-111111111111"
    private const val OPERATION_ID = "22222222-2222-4222-8222-222222222222"
    private const val REQUEST_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    private val NOW = Instant.parse("2030-01-01T00:00:00Z")
    private val CLOCK = Clock.fixed(NOW, ZoneOffset.UTC)
    private val KEY_PAIR =
      KeyPairGenerator.getInstance("RSA").apply { initialize(2048) }.generateKeyPair()
    private val OTHER_KEY_PAIR =
      KeyPairGenerator.getInstance("RSA").apply { initialize(2048) }.generateKeyPair()
    private val LOGGER =
      Logger.getLogger("org.wfanet.measurement.common.grpc.LoggingServerInterceptor")
    private val RUNFILES = Runfiles.preload().withSourceRepository("")

    private fun fixture(name: String) =
      requireNotNull(
          RUNFILES.getRuntimePath(
            Paths.get("wfa_measurement_system/src/main/k8s/testing/secretfiles/$name")
          )
        )
        .toFile()

    private fun headers(token: String) =
      Metadata().apply {
        put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer $token")
      }

    private fun token(
      subject: String?,
      audience: String = AUDIENCE,
      issuer: String = VidLabelingMachineAuthorizer.GOOGLE_ID_TOKEN_ISSUER,
      expiration: Long? = NOW.epochSecond + 3600,
      keyPair: KeyPair = KEY_PAIR,
    ): String {
      val encoder = Base64.getUrlEncoder().withoutPadding()
      val header =
        encoder.encodeToString(
          "{\"alg\":\"RS256\",\"kid\":\"test\",\"typ\":\"JWT\"}".toByteArray(UTF_8)
        )
      val payload =
        JsonObject().apply {
          addProperty("aud", audience)
          addProperty("iss", issuer)
          if (subject != null) addProperty("sub", subject)
          if (expiration != null) addProperty("exp", expiration)
        }
      val input = header + "." + encoder.encodeToString(payload.toString().toByteArray(UTF_8))
      val signer =
        Signature.getInstance("SHA256withRSA").apply {
          initSign(keyPair.private)
          update(input.toByteArray(UTF_8))
        }
      return input + "." + encoder.encodeToString(signer.sign())
    }

    private val OUTPUT_MANIFEST = publicManifest {
      blobs +=
        PublicManifestKt.blobVersion {
          blobUri = "gs://labeled/existing"
          generation = 51L
        }
      blobs += PublicManifestKt.blobVersion { blobUri = "gs://labeled/absent" }
    }
    private val INTERNAL_OUTPUT_MANIFEST = internalManifest {
      blobs +=
        InternalManifestKt.blobVersion {
          blobUri = "gs://labeled/existing"
          generation = 51L
        }
      blobs += InternalManifestKt.blobVersion { blobUri = "gs://labeled/absent" }
    }
    private val UPLOAD = rawImpressionUpload {
      dataProviderResourceId = "edp123"
      rawImpressionUploadResourceId = "upload-1"
      state = RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED
      doneBlobUri = "gs://raw/date/done"
      doneBlobGeneration = 20L
      doneBlobCreateTime = NOW.toProtoTime()
      correctionCandidateId = CANDIDATE_ID
      processingDeferred = true
      createTime = NOW.toProtoTime()
      updateTime = NOW.toProtoTime()
      etag = "etag"
    }
    private val COMPARISON =
      InternalCandidateKt.manifestComparison {
        val prior =
          InternalCandidateKt.manifestEntry {
            blobUri = "gs://raw/date/a"
            blobGeneration = 10L
            eventDate = date {
              year = 2026
              month = 1
              day = 1
            }
            outputSourceRawImpressionUploadResourceId = "owner"
          }
        val current =
          prior.copy {
            blobGeneration = 20L
            outputSourceRawImpressionUploadResourceId = "upload-1"
            eventDate = date {
              year = 2026
              month = 1
              day = 2
            }
          }
        val removed =
          prior.copy {
            blobUri = "gs://raw/date/removed"
            blobGeneration = 11L
            outputSourceRawImpressionUploadResourceId = "removed-owner"
          }
        val added =
          current.copy {
            blobUri = "gs://raw/date/added"
            blobGeneration = 30L
            outputSourceRawImpressionUploadResourceId = "added-owner"
          }
        priorManifest += listOf(prior, removed)
        currentManifest += listOf(current, added)
        differences +=
          InternalCandidateKt.manifestDifference {
            type = InternalCandidate.ManifestDifference.Type.TYPE_EDITED
            this.prior = prior
            this.current = current
          }
        differences +=
          InternalCandidateKt.manifestDifference {
            type = InternalCandidate.ManifestDifference.Type.TYPE_EVENT_DATE_CHANGED
            this.prior = prior
            this.current = current
          }
        differences +=
          InternalCandidateKt.manifestDifference {
            type = InternalCandidate.ManifestDifference.Type.TYPE_REMOVED
            this.prior = removed
          }
        differences +=
          InternalCandidateKt.manifestDifference {
            type = InternalCandidate.ManifestDifference.Type.TYPE_ADDED
            this.current = added
          }
      }
    private val CANDIDATE = rawImpressionUploadCorrectionCandidate {
      dataProviderResourceId = "edp123"
      rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
      rawImpressionUploadResourceId = "upload-1"
      classification = InternalCandidate.Classification.CLASSIFICATION_EDITED
      priorManifestDigest = com.google.protobuf.ByteString.copyFrom(ByteArray(32) { 1 })
      currentManifestDigest = com.google.protobuf.ByteString.copyFrom(ByteArray(32) { 2 })
      state = InternalCandidate.State.STATE_PENDING
      manifestComparison = COMPARISON
      expireTime = NOW.plusSeconds(86400).toProtoTime()
      createTime = NOW.toProtoTime()
      updateTime = NOW.toProtoTime()
      etag = "candidate-etag"
    }
  }
}
