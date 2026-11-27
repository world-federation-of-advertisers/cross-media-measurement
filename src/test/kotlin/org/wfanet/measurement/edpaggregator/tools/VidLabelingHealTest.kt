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
import com.google.devtools.build.runfiles.Runfiles
import com.google.protobuf.Any
import io.grpc.Status
import io.grpc.netty.NettyServerBuilder
import java.nio.file.Paths
import java.util.concurrent.TimeUnit.SECONDS
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.toServerTlsContext
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.vidRankBuilderParams
import org.wfanet.measurement.edpaggregator.vidlabeling.WorkItemIds
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GetWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.copy
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import picocli.CommandLine

@RunWith(JUnit4::class)
class VidLabelingHealTest {
  @Test
  fun `command exposes only supported operator actions`() {
    assertThat(CommandLine(VidLabelingHeal()).subcommands.keys)
      .containsExactly(
        "mark-failed",
        "retry-failed",
        "backfill-model-line",
        "list-correction-candidates",
        "get-correction-candidate",
        "list-correction-plans",
        "get-correction-plan",
        "approve-correction-plan",
        "retry-correction-plan",
        "redeliver-dlq",
        "help",
      )
  }

  @Test
  fun `command line rejects negative RPC interval`() {
    val exception =
      assertFailsWith<CommandLine.ParameterException> {
        CommandLine(VidLabelingHeal())
          .parseArgs("retry-failed", "--metadata-read-rpc-min-interval=-1s")
      }

    assertThat(exception).hasMessageThat().contains("complete human-readable duration")
  }

  @Test
  fun `command line rejects partially malformed RPC interval`() {
    val exception =
      assertFailsWith<CommandLine.ParameterException> {
        CommandLine(VidLabelingHeal())
          .parseArgs("retry-failed", "--control-plane-rpc-min-interval=500msjunk")
      }

    assertThat(exception).hasMessageThat().contains("complete human-readable duration")
  }

  @Test
  fun `retry failed reads owner metadata from EDPA and republishes through the control plane`() {
    val upload = "dataProviders/dp/rawImpressionUploads/upload"
    val model = "modelProviders/mp/modelSuites/ms/modelLines/ml"
    val row = rawImpressionUploadModelLine {
      name = "$upload/rawImpressionUploadModelLines/ml"
      cmmsModelLine = model
      state = RawImpressionUploadModelLine.State.FAILED
      failureReason = RawImpressionUploadModelLine.FailureReason.PROCESSING_FAILURE
      failureAttemptId = "failed-attempt"
    }
    val models: RawImpressionUploadModelLineServiceCoroutineImplBase = mockService {
      onBlocking { listRawImpressionUploadModelLines(any()) }
        .thenReturn(
          listRawImpressionUploadModelLinesResponse { rawImpressionUploadModelLines += row }
        )
      onBlocking { markRawImpressionUploadModelLineRanking(any()) }
        .thenReturn(row.copy { state = RawImpressionUploadModelLine.State.RANKING })
    }
    val uploads: RawImpressionUploadServiceCoroutineImplBase = mockService {
      onBlocking { getRawImpressionUpload(any()) }
        .thenReturn(
          rawImpressionUpload {
            name = upload
            registrationComplete = true
            state = RawImpressionUpload.State.ACTIVE
          }
        )
      onBlocking { listRawImpressionUploads(any()) }.thenReturn(listRawImpressionUploadsResponse {})
    }
    val ranks: RankerJobServiceCoroutineImplBase = mockService {
      onBlocking { listRankerJobs(any()) }
        .thenReturn(
          listRankerJobsResponse {
            rankerJobs += rankerJob {
              name = "$upload/rankerJobs/rank"
              state = RankerJob.State.CREATED
            }
          }
        )
    }
    val labels: VidLabelingJobServiceCoroutineImplBase = mockService {
      onBlocking { listVidLabelingJobs(any()) }.thenReturn(listVidLabelingJobsResponse {})
    }
    val original = workItem {
      name = "workItems/${WorkItemIds.forVidRankBuilder("$upload/rankerJobs/rank")}"
      queue = "rank-queue"
      state = WorkItem.State.FAILED
      generation = 1L
      workItemParams =
        Any.pack(
          workItemParams {
            appParams =
              Any.pack(
                vidRankBuilderParams {
                  modelLine = model
                  allowedPriorSnapshotRawImpressionUploads += "stale-owner"
                }
              )
          }
        )
    }
    val workItems: WorkItemsCoroutineImplBase = mockService {
      onBlocking { getWorkItem(any()) }
        .thenAnswer { invocation ->
          if (invocation.getArgument<GetWorkItemRequest>(0).name != original.name) {
            throw Status.NOT_FOUND.asRuntimeException()
          }
          original
        }
      onBlocking { createWorkItem(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<CreateWorkItemRequest>(0)
          request.workItem.copy {
            name = "workItems/${request.workItemId}"
            state = WorkItem.State.QUEUED
            generation = 1L
          }
        }
    }
    val secrets =
      checkNotNull(
        Runfiles.preload()
          .withSourceRepository("")
          .getRuntimePath(
            Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
          )
      )
    val certs =
      SigningCerts.fromPemFiles(
        certificateFile = secrets.resolve("kingdom_tls.pem").toFile(),
        privateKeyFile = secrets.resolve("kingdom_tls.key").toFile(),
        trustedCertCollectionFile = secrets.resolve("edp7_root.pem").toFile(),
      )
    val edpa =
      NettyServerBuilder.forPort(0)
        .sslContext(certs.toServerTlsContext())
        .addService(models)
        .addService(uploads)
        .addService(ranks)
        .addService(labels)
        .build()
        .start()
    val controlPlane =
      NettyServerBuilder.forPort(0)
        .sslContext(certs.toServerTlsContext())
        .addService(workItems)
        .build()
        .start()
    try {
      val code =
        CommandLine(VidLabelingHeal())
          .execute(
            "retry-failed",
            "--edpa-public-api-target=localhost:${edpa.port}",
            "--edpa-public-api-cert-host=localhost",
            "--control-plane-api-target=localhost:${controlPlane.port}",
            "--control-plane-api-cert-host=localhost",
            "--raw-impression-upload=$upload",
            "--model-line=$model",
            "--tls-cert-file=${secrets.resolve("edp7_tls.pem")}",
            "--tls-key-file=${secrets.resolve("edp7_tls.key")}",
            "--cert-collection-file=${secrets.resolve("kingdom_root.pem")}",
            "--control-plane-tls-cert-file=${secrets.resolve("edp7_tls.pem")}",
            "--control-plane-tls-key-file=${secrets.resolve("edp7_tls.key")}",
            "--control-plane-cert-collection-file=${secrets.resolve("kingdom_root.pem")}",
            "--metadata-read-rpc-min-interval=1ms",
            "--metadata-write-rpc-min-interval=1ms",
            "--control-plane-rpc-min-interval=1ms",
          )
      assertThat(code).isEqualTo(0)
      verifyBlocking(uploads) { getRawImpressionUpload(any()) }
      val requests = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItems) { createWorkItem(requests.capture()) }
      val params =
        requests.firstValue.workItem.workItemParams
          .unpack(WorkItem.WorkItemParams::class.java)
          .appParams
          .unpack(VidRankBuilderParams::class.java)
      assertThat(params.allowedPriorSnapshotRawImpressionUploadsList).isEmpty()
    } finally {
      edpa.shutdownNow().awaitTermination(1, SECONDS)
      controlPlane.shutdownNow().awaitTermination(1, SECONDS)
    }
  }

  @Test
  fun `retry failed accepts separate EDPA and control plane TLS files`() {
    val result =
      CommandLine(VidLabelingHeal())
        .parseArgs(
          "retry-failed",
          "--edpa-public-api-target=edpa.example:443",
          "--control-plane-api-target=control-plane.example:443",
          "--raw-impression-upload=dataProviders/dp/rawImpressionUploads/upload",
          "--model-line=modelProviders/mp/modelSuites/ms/modelLines/ml",
          "--tls-cert-file=edpa.pem",
          "--tls-key-file=edpa.key",
          "--cert-collection-file=edpa-roots.pem",
          "--control-plane-tls-cert-file=control-plane.pem",
          "--control-plane-tls-key-file=control-plane.key",
          "--control-plane-cert-collection-file=control-plane-roots.pem",
        )

    val retryFailed = checkNotNull(result.subcommand())
    assertThat(retryFailed.hasMatchedOption("--tls-cert-file")).isTrue()
    assertThat(retryFailed.hasMatchedOption("--control-plane-tls-cert-file")).isTrue()
  }
}
