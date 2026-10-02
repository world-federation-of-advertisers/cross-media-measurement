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

package org.wfanet.measurement.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation
import picocli.CommandLine

@RunWith(JUnit4::class)
class CorrectionPlanCommandsTest {
  private val service:
    UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase =
    mockService()

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(service) }

  private val client by lazy {
    CorrectionPlanClient(
      UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(
        grpcTestServerRule.channel
      )
    )
  }

  @Test
  fun `list command reads every actionable page without upload details`() =
    runBlocking<Unit> {
      whenever(service.listUploadHealingOperations(any()))
        .thenReturn(
          listUploadHealingOperationsResponse {
            uploadHealingOperations += PLAN
            nextPageToken = "next"
          },
          listUploadHealingOperationsResponse {
            uploadHealingOperations +=
              PLAN.copy { name = "$DATA_PROVIDER/uploadHealingOperations/2" }
          },
        )
      val output = mutableListOf<String>()

      val exitCode =
        CommandLine(ListCorrectionPlansCommand(client, output::add))
          .execute("--data-provider=$DATA_PROVIDER", *CONNECTION_ARGS)

      assertThat(exitCode).isEqualTo(0)
      assertThat(output).hasSize(2)
      assertThat(output.joinToString("\n")).doesNotContain("rawImpressionUploads")
      val requests =
        argumentCaptor<
          org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
        >()
      verify(service, times(2)).listUploadHealingOperations(requests.capture())
      assertThat(requests.firstValue.filter.stateInList)
        .containsExactly(
          UploadHealingOperation.State.APPROVAL_REQUIRED,
          UploadHealingOperation.State.NEEDS_ATTENTION,
        )
        .inOrder()
      assertThat(requests.secondValue.pageToken).isEqualTo("next")
    }

  @Test
  fun `get command prints the complete plan`() =
    runBlocking<Unit> {
      whenever(service.getUploadHealingOperation(any())).thenReturn(PLAN)
      val output = mutableListOf<String>()

      val exitCode =
        CommandLine(GetCorrectionPlanCommand(client, output::add))
          .execute(PLAN.name, *CONNECTION_ARGS)

      assertThat(exitCode).isEqualTo(0)
      assertThat(output.single()).contains(CANDIDATE)
    }

  @Test
  fun `approve command sends one decision for each candidate`() =
    runBlocking<Unit> {
      whenever(service.approveUploadHealingOperation(any()))
        .thenReturn(PLAN.copy { state = UploadHealingOperation.State.APPROVED })
      val output = mutableListOf<String>()

      val exitCode =
        CommandLine(ApproveCorrectionPlanCommand(client, output::add))
          .execute(
            PLAN.name,
            "--correct-candidate=$CANDIDATE",
            "--no-replacement-candidate=$SECOND_CANDIDATE",
            "--etag=etag",
            "--request-id=$REQUEST_ID",
            *CONNECTION_ARGS,
          )

      assertThat(exitCode).isEqualTo(0)
      val request =
        argumentCaptor<
            org.wfanet.measurement.edpaggregator.v1alpha.ApproveUploadHealingOperationRequest
          >()
          .let { captor ->
            verify(service).approveUploadHealingOperation(captor.capture())
            captor.firstValue
          }
      assertThat(request.name).isEqualTo(PLAN.name)
      assertThat(request.candidateDecisionsList.map { it.rawImpressionUploadCorrectionCandidate })
        .containsExactly(CANDIDATE, SECOND_CANDIDATE)
      assertThat(request.candidateDecisionsList.map { it.decision })
        .containsExactly(
          RawImpressionUploadCorrectionCandidate.Decision.DECISION_CORRECT,
          RawImpressionUploadCorrectionCandidate.Decision.DECISION_NO_REPLACEMENT,
        )
      assertThat(request.etag).isEqualTo("etag")
      assertThat(request.requestId).isEqualTo(REQUEST_ID)
    }

  @Test
  fun `retry command sends only concurrency and idempotency tokens`() =
    runBlocking<Unit> {
      whenever(service.retryUploadHealingOperation(any()))
        .thenReturn(PLAN.copy { state = UploadHealingOperation.State.EVICTING })
      val output = mutableListOf<String>()

      val exitCode =
        CommandLine(RetryCorrectionPlanCommand(client, output::add))
          .execute(PLAN.name, "--etag=etag", "--request-id=$REQUEST_ID", *CONNECTION_ARGS)

      assertThat(exitCode).isEqualTo(0)
      val request =
        argumentCaptor<
            org.wfanet.measurement.edpaggregator.v1alpha.RetryUploadHealingOperationRequest
          >()
          .let { captor ->
            verify(service).retryUploadHealingOperation(captor.capture())
            captor.firstValue
          }
      assertThat(request.name).isEqualTo(PLAN.name)
      assertThat(request.etag).isEqualTo("etag")
      assertThat(request.requestId).isEqualTo(REQUEST_ID)
    }

  @Test
  fun `commands are registered`() {
    assertThat(CommandLine(VidLabelingHeal()).subcommands.keys)
      .containsAtLeast(
        "list-correction-plans",
        "get-correction-plan",
        "approve-correction-plan",
        "retry-correction-plan",
      )
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp"
    private const val CANDIDATE = "$DATA_PROVIDER/rawImpressionUploadCorrectionCandidates/candidate"
    private const val SECOND_CANDIDATE =
      "$DATA_PROVIDER/rawImpressionUploadCorrectionCandidates/second-candidate"
    private const val REQUEST_ID = "11111111-1111-4111-8111-111111111111"
    private val PLAN = uploadHealingOperation {
      name = "$DATA_PROVIDER/uploadHealingOperations/operation"
      state = UploadHealingOperation.State.APPROVAL_REQUIRED
      rawImpressionUploadCorrectionCandidates += listOf(CANDIDATE, SECOND_CANDIDATE)
      etag = "etag"
    }
    private val CONNECTION_ARGS =
      arrayOf(
        "--edpa-public-api-target=unused",
        "--tls-cert-file=unused",
        "--tls-key-file=unused",
        "--cert-collection-file=unused",
      )
  }
}
