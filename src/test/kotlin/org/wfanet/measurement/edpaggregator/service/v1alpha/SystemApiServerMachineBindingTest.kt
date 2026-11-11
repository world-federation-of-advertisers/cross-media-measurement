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
import com.google.devtools.build.runfiles.Runfiles
import com.google.protobuf.TextFormat
import io.grpc.ManagedChannel
import io.grpc.Status
import io.grpc.StatusException
import io.netty.handler.ssl.ClientAuth
import java.net.ServerSocket
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.config.edpaggregator.vidLabelingMachineApiConfig
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidateServiceGrpcKt as PublicCandidates
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingCorrectionDetectionServiceGrpcKt as Detection
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.edpaggregator.v1alpha.quarantineRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.GetRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt as InternalCandidates
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadCorrectionCandidate

/** Exercises the deployed system-server entry point rather than its service factory alone. */
@RunWith(JUnit4::class)
class SystemApiServerMachineBindingTest {
  @get:Rule val temporaryFolder = TemporaryFolder()
  private lateinit var internalServer: CommonServer
  private lateinit var process: Process
  private lateinit var channel: ManagedChannel

  @Before
  fun start() {
    val internalService =
      object : InternalCandidates.RawImpressionUploadCorrectionCandidateServiceCoroutineImplBase() {
        override suspend fun getRawImpressionUploadCorrectionCandidate(
          request: GetRawImpressionUploadCorrectionCandidateRequest
        ): RawImpressionUploadCorrectionCandidate = rawImpressionUploadCorrectionCandidate {
          dataProviderResourceId = request.dataProviderResourceId
          rawImpressionUploadCorrectionCandidateId =
            request.rawImpressionUploadCorrectionCandidateId
          rawImpressionUploadResourceId = "upload"
        }
      }
    internalServer =
      CommonServer.fromParameters(
          verboseGrpcLogging = false,
          certs =
            SigningCerts.fromPemFiles(
              fixture("edp_aggregator_tls.pem"),
              fixture("edp_aggregator_tls.key"),
              fixture("edp_aggregator_root.pem"),
            ),
          clientAuth = ClientAuth.REQUIRE,
          nameForLogging = "InternalSystemBindingFixture",
          services = listOf(internalService.bindService()),
        )
        .start()
    val config = temporaryFolder.newFile("machine-config.textproto")
    config.writeText(
      TextFormat.printer()
        .printToString(
          vidLabelingMachineApiConfig {
            audience = "https://system.example/vid-labeling-machine-api"
            dispatcherServiceAccountSubjects += "111111111111111111111"
            healingControllerServiceAccountSubjects += "222222222222222222222"
          }
        )
    )
    val port = ServerSocket(0).use { it.localPort }
    val binary =
      requireNotNull(
          RUNFILES.getRuntimePath(
            Paths.get(
              "wfa_measurement_system/src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/common/server/SystemApiServer"
            )
          )
        )
        .toString()
    process =
      ProcessBuilder(
          binary,
          "--port=$port",
          "--health-port=0",
          "--tls-cert-file=${fixture("edp_aggregator_tls.pem")}",
          "--tls-key-file=${fixture("edp_aggregator_tls.key")}",
          "--cert-collection-file=${fixture("edp_aggregator_trusted_certs.pem")}",
          "--edp-aggregator-internal-api-target=localhost:${internalServer.port}",
          "--edp-aggregator-internal-api-cert-host=localhost",
          "--vid-labeling-machine-api-config-file=$config",
        )
        .redirectErrorStream(true)
        .redirectOutput(temporaryFolder.newFile("server.log"))
        .start()
    channel =
      buildMutualTlsChannel(
        "localhost:$port",
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
    if (::process.isInitialized) {
      process.destroy()
      if (!process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly()
    }
    if (::internalServer.isInitialized) internalServer.shutdown()
  }

  @Test
  fun `system server binds protected machine operations on its existing endpoint`() =
    runBlocking<Unit> {
      val stub =
        Detection.VidLabelingCorrectionDetectionServiceCoroutineStub(channel)
          .withWaitForReady()
          .withDeadlineAfter(15, TimeUnit.SECONDS)
      val error =
        assertFailsWith<StatusException> {
          stub.quarantineRawImpressionUpload(
            quarantineRawImpressionUploadRequest { parent = "dataProviders/edp123" }
          )
        }
      assertThat(error.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
      assertThat(process.isAlive).isTrue()
    }

  @Test
  fun `system server keeps ordinary candidate reads available through its internal channel`() =
    runBlocking {
      val name =
        "dataProviders/edp123/rawImpressionUploadCorrectionCandidates/11111111-1111-4111-8111-111111111111"
      val result =
        PublicCandidates.RawImpressionUploadCorrectionCandidateServiceCoroutineStub(channel)
          .withWaitForReady()
          .withDeadlineAfter(15, TimeUnit.SECONDS)
          .getRawImpressionUploadCorrectionCandidate(
            getRawImpressionUploadCorrectionCandidateRequest { this.name = name }
          )
      assertThat(result.name).isEqualTo(name)
      assertThat(result.rawImpressionUpload)
        .isEqualTo("dataProviders/edp123/rawImpressionUploads/upload")
    }

  companion object {
    private val RUNFILES = Runfiles.preload().withSourceRepository("")

    private fun fixture(name: String) =
      requireNotNull(
          RUNFILES.getRuntimePath(
            Paths.get("wfa_measurement_system/src/main/k8s/testing/secretfiles/$name")
          )
        )
        .toFile()
  }
}
