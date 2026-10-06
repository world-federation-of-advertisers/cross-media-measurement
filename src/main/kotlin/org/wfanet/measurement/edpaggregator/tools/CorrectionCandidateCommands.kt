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

import io.grpc.ManagedChannel
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadCorrectionCandidatesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadCorrectionCandidatesRequest
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters

/** Lists correction candidates available to an operator. */
@Command(
  name = "list-correction-candidates",
  description = ["Lists raw-impression upload correction candidates."],
  mixinStandardHelpOptions = true,
)
class ListCorrectionCandidatesCommand(
  private val readerOverride: CorrectionCandidateReader? = null,
  private val output: (String) -> Unit = ::println,
) : EdpaApiCommand() {
  @Option(
    names = ["--data-provider"],
    description = ["DataProvider resource name."],
    required = true,
  )
  private lateinit var dataProvider: String

  @Option(names = ["--state"], description = ["Comma-separated candidate states."], split = ",")
  private var states: List<RawImpressionUploadCorrectionCandidate.State> =
    listOf(RawImpressionUploadCorrectionCandidate.State.PLANNED)

  @Option(
    names = ["--classification"],
    description = ["Comma-separated candidate classifications."],
    split = ",",
  )
  private var classifications: List<RawImpressionUploadCorrectionCandidate.Classification> =
    emptyList()

  override fun run() {
    var channel: ManagedChannel? = null
    try {
      runBlocking {
        val reader =
          readerOverride
            ?: buildEdpaChannel().let {
              channel = it
              CorrectionCandidateReader(
                RawImpressionUploadCorrectionCandidateServiceCoroutineStub(it)
              )
            }
        reader.list(dataProvider, states, classifications).forEach {
          output(formatCorrectionCandidateSummary(it))
        }
      }
    } finally {
      channel?.shutdown()
      channel?.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    }
  }
}

/** Retrieves one correction candidate and its manifest differences. */
@Command(
  name = "get-correction-candidate",
  description = ["Gets a raw-impression upload correction candidate."],
  mixinStandardHelpOptions = true,
)
class GetCorrectionCandidateCommand(
  private val readerOverride: CorrectionCandidateReader? = null,
  private val output: (String) -> Unit = ::println,
) : EdpaApiCommand() {
  @Parameters(index = "0", description = ["RawImpressionUploadCorrectionCandidate resource name."])
  private lateinit var name: String

  override fun run() {
    var channel: ManagedChannel? = null
    try {
      runBlocking {
        val reader =
          readerOverride
            ?: buildEdpaChannel().let {
              channel = it
              CorrectionCandidateReader(
                RawImpressionUploadCorrectionCandidateServiceCoroutineStub(it)
              )
            }
        output(reader.get(name).toString())
      }
    } finally {
      channel?.shutdown()
      channel?.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    }
  }
}

/** Reads correction candidates from the public API. */
class CorrectionCandidateReader(
  private val stub: RawImpressionUploadCorrectionCandidateServiceCoroutineStub
) {
  suspend fun list(
    parent: String,
    states: List<RawImpressionUploadCorrectionCandidate.State>,
    classifications: List<RawImpressionUploadCorrectionCandidate.Classification>,
  ): List<RawImpressionUploadCorrectionCandidate> {
    val candidates = mutableListOf<RawImpressionUploadCorrectionCandidate>()
    var pageToken = ""
    do {
      val response =
        stub.listRawImpressionUploadCorrectionCandidates(
          listRawImpressionUploadCorrectionCandidatesRequest {
            this.parent = parent
            pageSize = PAGE_SIZE
            if (pageToken.isNotEmpty()) this.pageToken = pageToken
            if (states.isNotEmpty() || classifications.isNotEmpty()) {
              filter =
                ListRawImpressionUploadCorrectionCandidatesRequestKt.filter {
                  stateIn += states
                  classificationIn += classifications
                }
            }
          }
        )
      candidates += response.rawImpressionUploadCorrectionCandidatesList
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return candidates
  }

  suspend fun get(name: String): RawImpressionUploadCorrectionCandidate =
    stub.getRawImpressionUploadCorrectionCandidate(
      getRawImpressionUploadCorrectionCandidateRequest { this.name = name }
    )

  companion object {
    private const val PAGE_SIZE = 100
  }
}

/** Formats a correction candidate without its manifest differences. */
fun formatCorrectionCandidateSummary(candidate: RawImpressionUploadCorrectionCandidate): String =
  listOf(
      candidate.name,
      "classification=${candidate.classification}",
      "state=${candidate.state}",
      "raw_impression_upload=${candidate.rawImpressionUpload}",
      "upload_healing_operation=${candidate.uploadHealingOperation.ifEmpty { "-" }}",
    )
    .joinToString("\t")
