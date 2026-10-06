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
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidateKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidateServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadCorrectionCandidatesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadCorrectionCandidate
import picocli.CommandLine

@RunWith(JUnit4::class)
class CorrectionCandidateCommandsTest {
  private val service:
    RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineImplBase =
    mockService()

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(service) }

  private val reader by lazy {
    CorrectionCandidateReader(
      RawImpressionUploadCorrectionCandidateServiceGrpcKt
        .RawImpressionUploadCorrectionCandidateServiceCoroutineStub(grpcTestServerRule.channel)
    )
  }

  @Test
  fun `list command reads every page and excludes raw object URIs`() =
    runBlocking<Unit> {
      whenever(service.listRawImpressionUploadCorrectionCandidates(any()))
        .thenReturn(
          listRawImpressionUploadCorrectionCandidatesResponse {
            rawImpressionUploadCorrectionCandidates += CANDIDATE
            nextPageToken = "next"
          },
          listRawImpressionUploadCorrectionCandidatesResponse {
            rawImpressionUploadCorrectionCandidates +=
              CANDIDATE.copy { name = "$DATA_PROVIDER/rawImpressionUploadCorrectionCandidates/2" }
          },
        )

      val output = mutableListOf<String>()
      val commandLine = CommandLine(ListCorrectionCandidatesCommand(reader, output::add))

      val exitCode =
        commandLine.execute(
          "--data-provider=$DATA_PROVIDER",
          "--state=PLANNED",
          "--classification=MIXED",
          *CONNECTION_ARGS,
        )

      assertThat(exitCode).isEqualTo(0)
      assertThat(output).hasSize(2)
      assertThat(output.joinToString("\n")).doesNotContain("gs://")
      val requests =
        argumentCaptor<
          org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadCorrectionCandidatesRequest
        >()
      verify(service, times(2)).listRawImpressionUploadCorrectionCandidates(requests.capture())
      assertThat(requests.firstValue.filter.stateInList)
        .containsExactly(RawImpressionUploadCorrectionCandidate.State.PLANNED)
      assertThat(requests.firstValue.filter.classificationInList)
        .containsExactly(RawImpressionUploadCorrectionCandidate.Classification.MIXED)
      assertThat(requests.secondValue.pageToken).isEqualTo("next")
    }

  @Test
  fun `get command prints the detailed candidate`() =
    runBlocking<Unit> {
      whenever(service.getRawImpressionUploadCorrectionCandidate(any()))
        .thenReturn(DETAILED_CANDIDATE)
      val output = mutableListOf<String>()
      val commandLine = CommandLine(GetCorrectionCandidateCommand(reader, output::add))

      val exitCode = commandLine.execute(CANDIDATE.name, *CONNECTION_ARGS)

      assertThat(exitCode).isEqualTo(0)
      assertThat(output.single()).contains("gs://raw/secret")
    }

  @Test
  fun `commands are registered`() {
    assertThat(CommandLine(VidLabelingHeal()).subcommands.keys)
      .containsAtLeast("list-correction-candidates", "get-correction-candidate")
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp"
    private val CANDIDATE = rawImpressionUploadCorrectionCandidate {
      name = "$DATA_PROVIDER/rawImpressionUploadCorrectionCandidates/1"
      rawImpressionUpload = "$DATA_PROVIDER/rawImpressionUploads/upload"
      classification = RawImpressionUploadCorrectionCandidate.Classification.MIXED
      state = RawImpressionUploadCorrectionCandidate.State.PLANNED
    }
    private val DETAILED_CANDIDATE =
      CANDIDATE.copy {
        manifestDifferences +=
          RawImpressionUploadCorrectionCandidateKt.manifestDifference {
            blobUri = "gs://raw/secret"
          }
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
