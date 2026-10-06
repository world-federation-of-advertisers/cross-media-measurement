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
import org.wfanet.measurement.edpaggregator.v1alpha.ApproveUploadHealingOperationRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.approveUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.retryUploadHealingOperationRequest
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters

/** Base for correction-plan commands. */
abstract class CorrectionPlanCommand(
  private val clientOverride: CorrectionPlanClient?,
  protected val output: (String) -> Unit,
) : EdpaApiCommand() {
  protected fun runWithClient(block: suspend (CorrectionPlanClient) -> Unit) {
    var channel: ManagedChannel? = null
    try {
      runBlocking {
        val client =
          clientOverride
            ?: buildEdpaChannel().let {
              channel = it
              CorrectionPlanClient(UploadHealingOperationServiceCoroutineStub(it))
            }
        block(client)
      }
    } finally {
      channel?.shutdown()
      channel?.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    }
  }
}

/** Lists correction plans available to an operator. */
@Command(
  name = "list-correction-plans",
  description = ["Lists VID-labeling correction plans."],
  mixinStandardHelpOptions = true,
)
class ListCorrectionPlansCommand(
  clientOverride: CorrectionPlanClient? = null,
  output: (String) -> Unit = ::println,
) : CorrectionPlanCommand(clientOverride, output) {
  @Option(
    names = ["--data-provider"],
    description = ["DataProvider resource name."],
    required = true,
  )
  private lateinit var dataProvider: String

  @Option(
    names = ["--state"],
    description = ["Comma-separated plan states."],
    split = ",",
    defaultValue = "APPROVAL_REQUIRED,NEEDS_ATTENTION",
  )
  private lateinit var states: List<UploadHealingOperation.State>

  override fun run() {
    runWithClient { client ->
      client.list(dataProvider, states).forEach { output(formatCorrectionPlanSummary(it)) }
    }
  }
}

/** Retrieves one correction plan. */
@Command(
  name = "get-correction-plan",
  description = ["Gets a VID-labeling correction plan."],
  mixinStandardHelpOptions = true,
)
class GetCorrectionPlanCommand(
  clientOverride: CorrectionPlanClient? = null,
  output: (String) -> Unit = ::println,
) : CorrectionPlanCommand(clientOverride, output) {
  @Parameters(index = "0", description = ["UploadHealingOperation resource name."])
  private lateinit var name: String

  override fun run() {
    runWithClient { client -> output(client.get(name).toString()) }
  }
}

/** Approves one correction plan. */
@Command(
  name = "approve-correction-plan",
  description = ["Approves a VID-labeling correction plan."],
  mixinStandardHelpOptions = true,
)
class ApproveCorrectionPlanCommand(
  clientOverride: CorrectionPlanClient? = null,
  output: (String) -> Unit = ::println,
) : CorrectionPlanCommand(clientOverride, output) {
  @Parameters(index = "0", description = ["UploadHealingOperation resource name."])
  private lateinit var name: String

  @Option(
    names = ["--correct-candidate"],
    description = ["Correction candidate to replace; repeat for each candidate."],
  )
  private var correctCandidates: List<String> = emptyList()

  @Option(
    names = ["--no-replacement-candidate"],
    description = ["Correction candidate to remove; repeat for each candidate."],
  )
  private var noReplacementCandidates: List<String> = emptyList()

  @Option(names = ["--etag"], description = ["Current plan etag."], required = true)
  private lateinit var etag: String

  @Option(names = ["--request-id"], description = ["Idempotency UUID."], required = true)
  private lateinit var requestId: String

  override fun run() {
    runWithClient { client ->
      output(
        formatCorrectionPlanSummary(
          client.approve(
            name,
            buildMap {
              correctCandidates.forEach {
                put(it, RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE)
              }
              noReplacementCandidates.forEach {
                require(
                  put(
                    it,
                    RawImpressionUploadCorrectionCandidate.Decision
                      .DECISION_REMOVE_WITHOUT_REPLACEMENT,
                  ) == null
                ) {
                  "A correction candidate can have only one decision"
                }
              }
            },
            etag,
            requestId,
          )
        )
      )
    }
  }
}

/** Retries one correction plan that needs attention. */
@Command(
  name = "retry-correction-plan",
  description = ["Retries a VID-labeling correction plan that needs attention."],
  mixinStandardHelpOptions = true,
)
class RetryCorrectionPlanCommand(
  clientOverride: CorrectionPlanClient? = null,
  output: (String) -> Unit = ::println,
) : CorrectionPlanCommand(clientOverride, output) {
  @Parameters(index = "0", description = ["UploadHealingOperation resource name."])
  private lateinit var name: String

  @Option(names = ["--etag"], description = ["Current plan etag."], required = true)
  private lateinit var etag: String

  @Option(names = ["--request-id"], description = ["Idempotency UUID."], required = true)
  private lateinit var requestId: String

  override fun run() {
    runWithClient { client ->
      output(formatCorrectionPlanSummary(client.retry(name, etag, requestId)))
    }
  }
}

/** Reads and mutates correction plans through the public API. */
class CorrectionPlanClient(private val stub: UploadHealingOperationServiceCoroutineStub) {
  suspend fun list(
    parent: String,
    states: List<UploadHealingOperation.State>,
  ): List<UploadHealingOperation> {
    val operations = mutableListOf<UploadHealingOperation>()
    var pageToken = ""
    do {
      val response =
        stub.listUploadHealingOperations(
          listUploadHealingOperationsRequest {
            this.parent = parent
            pageSize = PAGE_SIZE
            if (pageToken.isNotEmpty()) this.pageToken = pageToken
            if (states.isNotEmpty()) {
              filter = ListUploadHealingOperationsRequestKt.filter { stateIn += states }
            }
          }
        )
      operations += response.uploadHealingOperationsList
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return operations
  }

  suspend fun get(name: String): UploadHealingOperation =
    stub.getUploadHealingOperation(getUploadHealingOperationRequest { this.name = name })

  suspend fun approve(
    name: String,
    decisions: Map<String, RawImpressionUploadCorrectionCandidate.Decision>,
    etag: String,
    requestId: String,
  ): UploadHealingOperation =
    stub.approveUploadHealingOperation(
      approveUploadHealingOperationRequest {
        this.name = name
        candidateDecisions +=
          decisions.entries
            .sortedBy { it.key }
            .map { (candidate, decision) ->
              ApproveUploadHealingOperationRequestKt.candidateDecision {
                rawImpressionUploadCorrectionCandidate = candidate
                this.decision = decision
              }
            }
        this.etag = etag
        this.requestId = requestId
      }
    )

  suspend fun retry(name: String, etag: String, requestId: String): UploadHealingOperation =
    stub.retryUploadHealingOperation(
      retryUploadHealingOperationRequest {
        this.name = name
        this.etag = etag
        this.requestId = requestId
      }
    )

  companion object {
    private const val PAGE_SIZE = 100
  }
}

/** Formats a correction plan without raw object URIs. */
fun formatCorrectionPlanSummary(operation: UploadHealingOperation): String =
  listOf(
      operation.name,
      "state=${operation.state}",
      "candidates=${operation.rawImpressionUploadCorrectionCandidatesCount}",
      "steps=${operation.stepsCount}",
      "etag=${operation.etag}",
    )
    .joinToString("\t")
