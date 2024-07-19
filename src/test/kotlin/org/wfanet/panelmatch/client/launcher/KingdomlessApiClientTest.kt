// Copyright 2024 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.panelmatch.client.launcher

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.sign
import org.wfanet.measurement.common.crypto.signatureAlgorithm
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.client.common.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.common.Identity
import org.wfanet.panelmatch.client.internal.ExchangeCheckpoint
import org.wfanet.panelmatch.client.internal.ExchangeCheckpoint.ExchangeState
import org.wfanet.panelmatch.client.internal.ExchangeCheckpoint.ProgressEntry
import org.wfanet.panelmatch.client.internal.ExchangeCheckpointKt.progressEntry
import org.wfanet.panelmatch.client.internal.ExchangeStepAttempt
import org.wfanet.panelmatch.client.internal.ExchangeStepAttemptKt.debugMessageEntry
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Party.DATA_PROVIDER
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Party.MODEL_PROVIDER
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.step
import org.wfanet.panelmatch.client.internal.SignedExchangeCheckpoint
import org.wfanet.panelmatch.client.internal.copy
import org.wfanet.panelmatch.client.internal.exchangeCheckpoint
import org.wfanet.panelmatch.client.internal.exchangeStepAttempt
import org.wfanet.panelmatch.client.internal.exchangeWorkflow
import org.wfanet.panelmatch.client.internal.signedExchangeCheckpoint
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.Fingerprinters.sha256
import org.wfanet.panelmatch.common.certificates.testing.TestCertificateManager
import org.wfanet.panelmatch.common.secrets.testing.TestSecretMap
import org.wfanet.panelmatch.common.storage.PrefixedStorageClient
import org.wfanet.panelmatch.common.storage.toByteString
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class KingdomlessApiClientTest {

  private val rootSharedStorageClient = InMemoryStorageClient()

  @Test
  fun claimExchangeStepReturnsNullWhenNoRecurringExchangeIdsAreConfigured() = runBlockingTest {
    val client = buildClient()

    assertThat(client.claimExchangeStep()).isNull()
  }

  @Test
  fun claimExchangeStepThrowsWhenCheckpointSignatureIsInvalid() = runBlockingTest {
    val clock = FakeClock()
    val client = buildClient(RECURRING_EXCHANGE_ID_1 to WORKFLOW, clock = clock)
    writeCheckpoint(
      WORKFLOW.toCheckpoint(
        DATA_PROVIDER,
        ExchangeDateKey(RECURRING_EXCHANGE_ID_1, CURRENT_DATE),
        ExchangeState.IN_PROGRESS,
        EDP_STEP.toProgressEntry(
          attemptNumber = 1,
          state = ExchangeStepAttempt.State.IN_PROGRESS,
          startTime = clock.instant(),
        ),
      )
    )

    assertThat(client.claimExchangeStep()).isNull()
  }

  @Test
  fun claimExchangeStepReturnsNullWhenAllAvailableExchangeStepsAreInProgress() = runBlockingTest {
    val clock = FakeClock()
    val client = buildClient(RECURRING_EXCHANGE_ID_1 to WORKFLOW, clock = clock)
    writeCheckpoint(
      checkpoint =
        WORKFLOW.toCheckpoint(
          DATA_PROVIDER,
          ExchangeDateKey(RECURRING_EXCHANGE_ID_1, CURRENT_DATE),
          ExchangeState.IN_PROGRESS,
          EDP_STEP.toProgressEntry(
            attemptNumber = 1,
            state = ExchangeStepAttempt.State.IN_PROGRESS,
            startTime = clock.instant(),
          ),
        ),
      signature = ByteString.EMPTY,
    )

    val exception = assertFailsWith<IllegalStateException> { client.claimExchangeStep() }

    assertThat(exception).hasMessageThat().contains("Invalid signature for exchange checkpoint")
  }

  @Test
  fun claimExchangeStepReturnsNullWhenAllExchangeStepsAreCompleted() = runBlockingTest {
    val clock = FakeClock()
    val client =
      buildClient(
        RECURRING_EXCHANGE_ID_1 to WORKFLOW,
        RECURRING_EXCHANGE_ID_2 to WORKFLOW,
        clock = clock,
      )
    writeCheckpoint(
      WORKFLOW.toCheckpoint(
        DATA_PROVIDER,
        ExchangeDateKey(RECURRING_EXCHANGE_ID_1, CURRENT_DATE),
        ExchangeState.SUCCEEDED,
        EDP_STEP.toProgressEntry(
          attemptNumber = 1,
          state = ExchangeStepAttempt.State.SUCCEEDED,
          startTime = clock.instant(),
        ),
      )
    )
    writeCheckpoint(
      WORKFLOW.toCheckpoint(
        DATA_PROVIDER,
        ExchangeDateKey(RECURRING_EXCHANGE_ID_2, CURRENT_DATE),
        ExchangeState.FAILED,
        EDP_STEP.toProgressEntry(
          attemptNumber = 1,
          state = ExchangeStepAttempt.State.FAILED_STEP,
          startTime = clock.instant(),
        ),
      )
    )

    assertThat(client.claimExchangeStep()).isNull()
  }

  @Test
  fun claimExchangeStepReturnsAvailableStep() = runBlockingTest {
    val client = buildClient(RECURRING_EXCHANGE_ID_1 to WORKFLOW)

    assertThat(client.claimExchangeStep())
      .isEqualTo(
        ApiClient.ClaimedExchangeStep(
          attemptKey =
            ExchangeStepAttemptKey(
              recurringExchangeId = RECURRING_EXCHANGE_ID_1,
              exchangeId = CURRENT_DATE.toString(),
              stepId = EDP_STEP.step.stepId,
              attemptId = "1",
            ),
          exchangeDate = CURRENT_DATE,
          stepIndex = EDP_STEP.index,
          workflow = WORKFLOW,
          workflowFingerprint = sha256(WORKFLOW.toByteString()),
        )
      )
  }

  @Test
  fun claimExchangeStepReturnsStepsInRoundRobinFashion() = runBlockingTest {
    val clock = FakeClock(step = STEP_TIMEOUT)
    val client =
      buildClient(
        RECURRING_EXCHANGE_ID_1 to WORKFLOW,
        RECURRING_EXCHANGE_ID_2 to WORKFLOW,
        RECURRING_EXCHANGE_ID_3 to WORKFLOW,
        clock = clock,
      )

    assertThat(client.claimExchangeStep()?.attemptKey?.recurringExchangeId)
      .isEqualTo(RECURRING_EXCHANGE_ID_1)
    assertThat(client.claimExchangeStep()?.attemptKey?.recurringExchangeId)
      .isEqualTo(RECURRING_EXCHANGE_ID_2)
    assertThat(client.claimExchangeStep()?.attemptKey?.recurringExchangeId)
      .isEqualTo(RECURRING_EXCHANGE_ID_3)
    assertThat(client.claimExchangeStep()?.attemptKey?.recurringExchangeId)
      .isEqualTo(RECURRING_EXCHANGE_ID_1)
    assertThat(client.claimExchangeStep()?.attemptKey?.recurringExchangeId)
      .isEqualTo(RECURRING_EXCHANGE_ID_2)
    assertThat(client.claimExchangeStep()?.attemptKey?.recurringExchangeId)
      .isEqualTo(RECURRING_EXCHANGE_ID_3)
  }

  @Test
  fun claimExchangeStepReturnsStepsFromFirstExchangeDateToNow() = runBlockingTest {
    val firstExchangeDate = WORKFLOW.firstExchangeDate.toLocalDate()
    val clock = FakeClock(startTime = CURRENT_TIME.plus(Duration.ofDays(3L)))
    val client = buildClient(RECURRING_EXCHANGE_ID_1 to WORKFLOW, clock = clock)

    assertThat(client.claimExchangeStep()?.exchangeDate).isEqualTo(firstExchangeDate)
    assertThat(client.claimExchangeStep()?.exchangeDate).isEqualTo(firstExchangeDate.plusDays(1L))
    assertThat(client.claimExchangeStep()?.exchangeDate).isEqualTo(firstExchangeDate.plusDays(2L))
    assertThat(client.claimExchangeStep()?.exchangeDate).isEqualTo(firstExchangeDate.plusDays(3L))
    assertThat(client.claimExchangeStep()?.exchangeDate).isNull()
  }

  @Test
  fun claimExchangeStepReturnsStepsBeginningFromLookbackWindowDaysAgo() = runBlockingTest {
    val startTime = CURRENT_TIME.plus(LOOKBACK_WINDOW.plusDays(7L))
    val clock = FakeClock(startTime = startTime, step = Duration.ofSeconds(1L))
    val client = buildClient(RECURRING_EXCHANGE_ID_1 to WORKFLOW, clock = clock)

    var currentTime = startTime.minus(LOOKBACK_WINDOW)
    while (currentTime <= startTime) {
      assertThat(client.claimExchangeStep()?.exchangeDate)
        .isEqualTo(currentTime.atZone(ZoneOffset.UTC).toLocalDate())
      currentTime = currentTime.plus(Duration.ofDays(1L))
    }
    assertThat(client.claimExchangeStep()).isNull()
  }

  @Test
  fun claimExchangeStepExpiresInProgressStepAndReturnsNewAttempt() = runBlockingTest {
    val clock = FakeClock(step = STEP_TIMEOUT)
    val client = buildClient(RECURRING_EXCHANGE_ID_1 to WORKFLOW, clock = clock)
    val exchangeDateKey = ExchangeDateKey(RECURRING_EXCHANGE_ID_1, CURRENT_DATE)
    val initialCheckpoint =
      WORKFLOW.toCheckpoint(
        DATA_PROVIDER,
        exchangeDateKey,
        ExchangeState.IN_PROGRESS,
        EDP_STEP.toProgressEntry(
          attemptNumber = 1,
          state = ExchangeStepAttempt.State.IN_PROGRESS,
          startTime = clock.instant(),
        ),
      )
    writeCheckpoint(initialCheckpoint)

    val claimedStep = client.claimExchangeStep()
    val updatedEntry = readCheckpoint(exchangeDateKey, DATA_PROVIDER).getProgressEntries(0)

    assertThat(claimedStep)
      .isEqualTo(
        ApiClient.ClaimedExchangeStep(
          attemptKey =
            ExchangeStepAttemptKey(
              recurringExchangeId = RECURRING_EXCHANGE_ID_1,
              exchangeId = CURRENT_DATE.toString(),
              stepId = EDP_STEP.step.stepId,
              attemptId = "2",
            ),
          exchangeDate = CURRENT_DATE,
          stepIndex = EDP_STEP.index,
          workflow = WORKFLOW,
          workflowFingerprint = sha256(WORKFLOW.toByteString()),
        )
      )
    assertThat(updatedEntry.hasEndTime()).isTrue()
    assertThat(updatedEntry.attempt.state).isEqualTo(ExchangeStepAttempt.State.FAILED)
    assertThat(updatedEntry.attempt.getDebugMessageEntries(0).message)
      .isEqualTo("Attempt timed out after $STEP_TIMEOUT")
  }

  @Test
  fun claimExchangeStepCreatesNewCheckpointForOwningParty() = runBlockingTest {
    val clock = FakeClock(step = Duration.ZERO)
    val client = buildClient(RECURRING_EXCHANGE_ID_1 to WORKFLOW, clock = clock)
    val exchangeDateKey = ExchangeDateKey(RECURRING_EXCHANGE_ID_1, CURRENT_DATE)

    client.claimExchangeStep()

    assertThat(readCheckpoint(exchangeDateKey, DATA_PROVIDER))
      .isEqualTo(
        WORKFLOW.toCheckpoint(
          DATA_PROVIDER,
          exchangeDateKey,
          ExchangeState.IN_PROGRESS,
          EDP_STEP.toProgressEntry(
            attemptNumber = 1,
            state = ExchangeStepAttempt.State.IN_PROGRESS,
            startTime = clock.instant(),
          ),
        )
      )
  }

  @Test
  fun claimExchangeStepUpdatesExistingCheckpointForOwningParty() = runBlockingTest {
    val clock = FakeClock(step = Duration.ZERO)
    val client = buildClient(RECURRING_EXCHANGE_ID_1 to WORKFLOW, clock = clock)
    val exchangeDateKey = ExchangeDateKey(RECURRING_EXCHANGE_ID_1, CURRENT_DATE)
    val initialCheckpoint =
      WORKFLOW.toCheckpoint(
        DATA_PROVIDER,
        exchangeDateKey,
        ExchangeState.IN_PROGRESS,
        EDP_STEP.toProgressEntry(
          attemptNumber = 1,
          state = ExchangeStepAttempt.State.FAILED,
          startTime = clock.instant(),
        ),
      )
    writeCheckpoint(initialCheckpoint)

    client.claimExchangeStep()

    assertThat(readCheckpoint(exchangeDateKey, DATA_PROVIDER))
      .isEqualTo(
        initialCheckpoint.copy {
          progressEntries +=
            EDP_STEP.toProgressEntry(
              attemptNumber = 2,
              state = ExchangeStepAttempt.State.IN_PROGRESS,
              startTime = clock.instant(),
            )
        }
      )
  }

  @Test
  fun appendLogEntryAddsDebugMessageToExistingAttempt() = runBlockingTest {
    val clock = FakeClock(step = Duration.ZERO)
    val client = buildClient(RECURRING_EXCHANGE_ID_1 to WORKFLOW, clock = clock)
    val claimedStep = client.claimExchangeStep()!!
    val initialCheckpoint =
      readCheckpoint(ExchangeDateKey(RECURRING_EXCHANGE_ID_1, CURRENT_DATE), DATA_PROVIDER)

    client.appendLogEntry(claimedStep.attemptKey, listOf("Some log message"))

    assertThat(
        readCheckpoint(ExchangeDateKey(RECURRING_EXCHANGE_ID_1, CURRENT_DATE), DATA_PROVIDER)
      )
      .isEqualTo(
        initialCheckpoint.copy {
          progressEntries[0] =
            progressEntries[0].copy {
              attempt =
                attempt.copy {
                  debugMessageEntries += debugMessageEntry {
                    message = "Some log message"
                    time = clock.instant().toProtoTime()
                  }
                }
            }
        }
      )
  }

  @Test
  fun finishExchangeStepAttemptThrowsWhenFinalStateIsNotTerminal() = runBlockingTest {
    val client = buildClient(RECURRING_EXCHANGE_ID_1 to WORKFLOW)
    val claimedStep = client.claimExchangeStep()!!

    val exception =
      assertFailsWith<IllegalArgumentException> {
        client.finishExchangeStepAttempt(
          claimedStep.attemptKey,
          ExchangeStepAttempt.State.IN_PROGRESS,
        )
      }

    assertThat(exception).hasMessageThat().matches("finalState must be one of .+ but was .+")
  }

  @Test
  fun finishExchangeStepAttemptUpdatesExchangeStateToSucceeded() = runBlockingTest {
    val clock = FakeClock(step = Duration.ZERO)
    val client = buildClient(RECURRING_EXCHANGE_ID_1 to WORKFLOW, clock = clock)
    val claimedStep = client.claimExchangeStep()!!
    val initialCheckpoint =
      readCheckpoint(ExchangeDateKey(RECURRING_EXCHANGE_ID_1, CURRENT_DATE), DATA_PROVIDER)

    client.finishExchangeStepAttempt(claimedStep.attemptKey, ExchangeStepAttempt.State.SUCCEEDED)

    assertThat(
        readCheckpoint(ExchangeDateKey(RECURRING_EXCHANGE_ID_1, CURRENT_DATE), DATA_PROVIDER)
      )
      .isEqualTo(
        initialCheckpoint.copy {
          progressEntries[0] =
            progressEntries[0].copy {
              attempt = attempt.copy { state = ExchangeStepAttempt.State.SUCCEEDED }
              endTime = clock.instant().toProtoTime()
            }
          exchangeState = ExchangeState.SUCCEEDED
        }
      )
  }

  @Test
  fun finishExchangeStepAttemptUpdatesExchangeStateToFailed() = runBlockingTest {
    val clock = FakeClock(step = Duration.ZERO)
    val client = buildClient(RECURRING_EXCHANGE_ID_1 to WORKFLOW, clock = clock)
    val claimedStep = client.claimExchangeStep()!!
    val initialCheckpoint =
      readCheckpoint(ExchangeDateKey(RECURRING_EXCHANGE_ID_1, CURRENT_DATE), DATA_PROVIDER)

    client.finishExchangeStepAttempt(
      claimedStep.attemptKey,
      ExchangeStepAttempt.State.FAILED_STEP,
      listOf("Some failure message"),
    )

    assertThat(
        readCheckpoint(ExchangeDateKey(RECURRING_EXCHANGE_ID_1, CURRENT_DATE), DATA_PROVIDER)
      )
      .isEqualTo(
        initialCheckpoint.copy {
          progressEntries[0] =
            progressEntries[0].copy {
              attempt =
                attempt.copy {
                  state = ExchangeStepAttempt.State.FAILED_STEP
                  debugMessageEntries += debugMessageEntry {
                    message = "Some failure message"
                    time = clock.instant().toProtoTime()
                  }
                }
              endTime = clock.instant().toProtoTime()
            }
          exchangeState = ExchangeState.FAILED
        }
      )
  }

  private suspend fun writeCheckpoint(
    checkpoint: ExchangeCheckpoint,
    signature: ByteString? = null,
  ) {
    val signedCheckpoint = signedExchangeCheckpoint {
      serializedCheckpoint = checkpoint.toByteString()
      this.signature =
        signature
          ?: TestCertificateManager.PRIVATE_KEY.sign(
            TestCertificateManager.CERTIFICATE.signatureAlgorithm!!,
            checkpoint.toByteString(),
          )
      signatureAlgorithmOid = TestCertificateManager.CERTIFICATE.sigAlgOID
      certName = TestCertificateManager.RESOURCE_NAME
    }
    val exchangeDateKey =
      ExchangeDateKey(
        recurringExchangeId = checkpoint.recurringExchangeId,
        date = checkpoint.exchangeDate.toLocalDate(),
      )
    val sharedStorage = buildSharedStorage(exchangeDateKey)
    sharedStorage.writeBlob(checkpoint.party.blobKey, signedCheckpoint.toByteString())
  }

  private suspend fun readCheckpoint(
    exchangeDateKey: ExchangeDateKey,
    party: ExchangeWorkflow.Party,
  ): ExchangeCheckpoint {
    val sharedStorage = buildSharedStorage(exchangeDateKey)
    val serializedSignedCheckpoint = sharedStorage.getBlob(party.blobKey)!!.toByteString()
    val signedCheckpoint = SignedExchangeCheckpoint.parseFrom(serializedSignedCheckpoint)
    return ExchangeCheckpoint.parseFrom(signedCheckpoint.serializedCheckpoint)
  }

  private fun buildClient(
    vararg recurringExchangeIdToWorkflow: Pair<String, ExchangeWorkflow>,
    clock: Clock = FakeClock(),
  ): KingdomlessApiClient {
    return KingdomlessApiClient(
      identity = IDENTITY,
      recurringExchangeIds = recurringExchangeIdToWorkflow.map { it.first },
      validExchangeWorkflows =
        TestSecretMap(
          *recurringExchangeIdToWorkflow.map { it.first to it.second.toByteString() }.toTypedArray()
        ),
      certificateManager = TestCertificateManager,
      algorithm = TestCertificateManager.CERTIFICATE.signatureAlgorithm!!,
      lookbackWindow = LOOKBACK_WINDOW,
      stepTimeout = STEP_TIMEOUT,
      clock = clock,
    ) {
      buildSharedStorage(it)
    }
  }

  private fun buildSharedStorage(exchangeDateKey: ExchangeDateKey): StorageClient {
    val (recurringExchangeId, exchangeDate) = exchangeDateKey
    return PrefixedStorageClient(
      delegate = rootSharedStorageClient,
      prefix = "recurringExchanges/$recurringExchangeId/exchanges/$exchangeDate",
    )
  }

  /**
   * Fake clock for testing. The first call to [instant] returns [startTime], and every subsequent
   * return value is incremented by [step].
   */
  private class FakeClock(
    private val startTime: Instant = CURRENT_TIME,
    private val step: Duration = Duration.ofMinutes(1L),
  ) : Clock() {

    private var nextTime = startTime

    override fun instant(): Instant {
      val time = nextTime
      nextTime = nextTime.plus(step)
      return time
    }

    override fun withZone(zone: ZoneId): Clock = this

    override fun getZone(): ZoneId = ZoneOffset.UTC
  }

  private data class IndexedStep(val step: ExchangeWorkflow.Step, val index: Int)

  companion object {

    private const val RECURRING_EXCHANGE_ID_1 = "some-recurring-exchange-id-1"
    private const val RECURRING_EXCHANGE_ID_2 = "some-recurring-exchange-id-2"
    private const val RECURRING_EXCHANGE_ID_3 = "some-recurring-exchange-id-3"

    private val IDENTITY = Identity(id = "some-data-provider-id", party = DATA_PROVIDER)
    private val CURRENT_TIME = Instant.parse("2024-06-01T00:00:00Z")
    private val CURRENT_DATE = CURRENT_TIME.atZone(ZoneOffset.UTC).toLocalDate()
    private val LOOKBACK_WINDOW = Duration.ofDays(30L)
    private val STEP_TIMEOUT = Duration.ofHours(1L)

    private val EDP_STEP =
      IndexedStep(
        step =
          step {
            stepId = "edp-step"
            party = DATA_PROVIDER
            outputLabels["output"] = "edp-output"
          },
        index = 0,
      )

    private val MP_STEP =
      IndexedStep(
        step =
          step {
            stepId = "mp-step"
            party = MODEL_PROVIDER
            outputLabels["output"] = "mp-output"
          },
        index = 1,
      )

    private val WORKFLOW = exchangeWorkflow {
      steps += EDP_STEP.step
      steps += MP_STEP.step
      firstExchangeDate = CURRENT_DATE.toProtoDate()
    }

    private fun ExchangeWorkflow.toCheckpoint(
      party: ExchangeWorkflow.Party,
      exchangeDateKey: ExchangeDateKey,
      exchangeState: ExchangeState,
      vararg progressEntries: ProgressEntry,
    ): ExchangeCheckpoint {
      return exchangeCheckpoint {
        this.party = party
        recurringExchangeId = exchangeDateKey.recurringExchangeId
        exchangeDate = exchangeDateKey.date.toProtoDate()
        workflowFingerprint = sha256(this@toCheckpoint.toByteString())
        this.exchangeState = exchangeState
        this.progressEntries += progressEntries.toList()
      }
    }

    private fun IndexedStep.toProgressEntry(
      attemptNumber: Int,
      state: ExchangeStepAttempt.State,
      startTime: Instant,
    ): ProgressEntry {
      return progressEntry {
        attempt = exchangeStepAttempt {
          stepId = step.stepId
          stepIndex = index
          this.attemptNumber = attemptNumber
          this.state = state
        }
        this.startTime = startTime.toProtoTime()
      }
    }

    private val ExchangeWorkflow.Party.blobKey: String
      get() {
        return when (this) {
          DATA_PROVIDER -> "checkpoints/edp-checkpoint"
          MODEL_PROVIDER -> "checkpoints/mp-checkpoint"
          else -> error("Unsupported party: $this")
        }
      }
  }
}
