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
import com.google.protobuf.Empty
import com.google.type.date
import io.grpc.Server
import io.grpc.ServerServiceDefinition
import io.grpc.netty.NettyServerBuilder
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.TimeUnit.SECONDS
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.check
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.wheneverBlocking
import org.wfanet.measurement.api.v2alpha.BatchDeleteEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.BatchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ListEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupActivitiesResponse
import org.wfanet.measurement.api.v2alpha.eventGroupActivity
import org.wfanet.measurement.api.v2alpha.listEventGroupActivitiesResponse
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.toServerTlsContext
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.ExitInterceptingSecurityManager

@RunWith(JUnit4::class)
class SyncEventGroupActivitiesTest {
  private val activitiesServiceMock: EventGroupActivitiesCoroutineImplBase = mockService {
    onBlocking { listEventGroupActivities(any<ListEventGroupActivitiesRequest>()) }
      .thenReturn(listEventGroupActivitiesResponse {})
    onBlocking { batchUpdateEventGroupActivities(any<BatchUpdateEventGroupActivitiesRequest>()) }
      .thenReturn(batchUpdateEventGroupActivitiesResponse {})
    onBlocking { batchDeleteEventGroupActivities(any<BatchDeleteEventGroupActivitiesRequest>()) }
      .thenReturn(Empty.getDefaultInstance())
  }

  private val serverCerts =
    SigningCerts.fromPemFiles(
      certificateFile = SECRETS_DIR.resolve("kingdom_tls.pem").toFile(),
      privateKeyFile = SECRETS_DIR.resolve("kingdom_tls.key").toFile(),
      trustedCertCollectionFile = SECRETS_DIR.resolve("kingdom_root.pem").toFile(),
    )

  private val services: List<ServerServiceDefinition> = listOf(activitiesServiceMock.bindService())

  private val server: Server =
    NettyServerBuilder.forPort(0)
      .sslContext(serverCerts.toServerTlsContext())
      .addServices(services)
      .build()

  @get:Rule val tempDir = TemporaryFolder()

  @Before
  fun initServer() {
    server.start()
  }

  @After
  fun shutdownServer() {
    server.shutdown()
    server.awaitTermination(1, SECONDS)
  }

  private val commonArgs: Array<String>
    get() =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/kingdom_tls.pem",
        "--tls-key-file=$SECRETS_DIR/kingdom_tls.key",
        "--cert-collection-file=$SECRETS_DIR/kingdom_root.pem",
        "--kingdom-public-api-target=$HOST:${server.port}",
        "--data-provider=$DATA_PROVIDER",
      )

  private fun protoDate(dateString: String) =
    java.time.LocalDate.parse(dateString).let {
      date {
        year = it.year
        month = it.monthValue
        day = it.dayOfMonth
      }
    }

  private fun writeInputFile(json: String): File {
    val file = tempDir.newFile("input.json")
    file.writeText(json)
    return file
  }

  @Test
  fun `CLI fails when neither --blob-uri nor --file provided`() {
    val capturedOutput = CommandLineTesting.capturingOutput(commonArgs, ::main)

    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
  }

  @Test
  fun `CLI fails when both --blob-uri and --file provided`() {
    val file = writeInputFile("[]")
    val args = commonArgs + arrayOf("--file=${file.path}", "--blob-uri=gs://bucket/input.json")

    val capturedOutput = CommandLineTesting.capturingOutput(args, ::main)

    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
  }

  @Test
  fun `CLI fails when --data-provider not provided`() {
    val file = writeInputFile("[]")
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/kingdom_tls.pem",
        "--tls-key-file=$SECRETS_DIR/kingdom_tls.key",
        "--cert-collection-file=$SECRETS_DIR/kingdom_root.pem",
        "--kingdom-public-api-target=$HOST:${server.port}",
        "--file=${file.path}",
      )

    val capturedOutput = CommandLineTesting.capturingOutput(args, ::main)

    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
  }

  @Test
  fun `CLI reads from local file successfully`() {
    val file =
      writeInputFile(
        """
        [
          {"parent": "$DATA_PROVIDER/eventGroups/eg1", "event_group_activity_date": "2026-01-01T00:00:00Z"},
          {"parent": "$DATA_PROVIDER/eventGroups/eg1", "event_group_activity_date": "2026-01-02T00:00:00Z"}
        ]
        """
          .trimIndent()
      )
    val args = commonArgs + arrayOf("--file=${file.path}")

    val capturedOutput = CommandLineTesting.capturingOutput(args, ::main)

    CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    verifyBlocking(activitiesServiceMock, times(1)) { batchUpdateEventGroupActivities(any()) }
  }

  @Test
  fun `CLI passes --list-page-size through to list requests`() {
    val file =
      writeInputFile(
        """
        [{"parent": "$DATA_PROVIDER/eventGroups/eg1", "event_group_activity_date": "2026-01-01T00:00:00Z"}]
        """
          .trimIndent()
      )
    val args = commonArgs + arrayOf("--file=${file.path}", "--list-page-size=17")

    val capturedOutput = CommandLineTesting.capturingOutput(args, ::main)

    CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    verifyBlocking(activitiesServiceMock, times(1)) {
      listEventGroupActivities(check { request -> assertThat(request.pageSize).isEqualTo(17) })
    }
  }

  @Test
  fun `CLI honors --max-delete-fraction guard and skips deletes`() {
    wheneverBlocking { activitiesServiceMock.listEventGroupActivities(any()) }
      .thenReturn(
        listEventGroupActivitiesResponse {
          eventGroupActivities +=
            (1..10).map { day ->
              val dateString = "2026-01-%02d".format(day)
              eventGroupActivity {
                name = "$DATA_PROVIDER/eventGroups/eg1/eventGroupActivities/$dateString"
                date = protoDate(dateString)
              }
            }
        }
      )
    // File keeps only 2 of 10 existing dates -> would delete 8/10 = 0.8 > 0.5.
    val file =
      writeInputFile(
        """
        [
          {"parent": "$DATA_PROVIDER/eventGroups/eg1", "event_group_activity_date": "2026-01-01T00:00:00Z"},
          {"parent": "$DATA_PROVIDER/eventGroups/eg1", "event_group_activity_date": "2026-01-02T00:00:00Z"}
        ]
        """
          .trimIndent()
      )
    val args = commonArgs + arrayOf("--file=${file.path}", "--max-delete-fraction=0.5")

    val capturedOutput = CommandLineTesting.capturingOutput(args, ::main)

    // Guard records a SyncError, so the CLI exits non-zero.
    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
    verifyBlocking(activitiesServiceMock, never()) { batchDeleteEventGroupActivities(any()) }
  }

  @Test
  fun `CLI dry-run prints plan without making API calls`() {
    val file =
      writeInputFile(
        """
        [
          {"parent": "$DATA_PROVIDER/eventGroups/eg1", "event_group_activity_date": "2026-01-01T00:00:00Z"}
        ]
        """
          .trimIndent()
      )
    val args = commonArgs + arrayOf("--file=${file.path}", "--dry-run")

    val capturedOutput = CommandLineTesting.capturingOutput(args, ::main)

    CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    verifyBlocking(activitiesServiceMock, never()) { batchUpdateEventGroupActivities(any()) }
    verifyBlocking(activitiesServiceMock, never()) { batchDeleteEventGroupActivities(any()) }
  }

  companion object {
    init {
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }

    private const val HOST = "localhost"
    private const val MODULE_REPO_NAME = "wfa_measurement_system"
    private const val DATA_PROVIDER = "dataProviders/dp1"
    private val SECRETS_DIR: Path =
      getRuntimePath(Paths.get(MODULE_REPO_NAME, "src", "main", "k8s", "testing", "secretfiles"))!!
  }
}
