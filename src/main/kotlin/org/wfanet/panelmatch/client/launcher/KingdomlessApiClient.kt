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

import com.google.protobuf.ByteString
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.time.delay
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.crypto.SignatureAlgorithm
import org.wfanet.measurement.common.crypto.sign
import org.wfanet.measurement.common.crypto.verifySignature
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.Store
import org.wfanet.panelmatch.client.common.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.common.ExchangeWorkflowDependencyGraph
import org.wfanet.panelmatch.client.common.Identity
import org.wfanet.panelmatch.client.internal.ExchangeCheckpoint
import org.wfanet.panelmatch.client.internal.ExchangeCheckpoint.ExchangeState
import org.wfanet.panelmatch.client.internal.ExchangeCheckpointKt.progressEntry
import org.wfanet.panelmatch.client.internal.ExchangeStepAttempt
import org.wfanet.panelmatch.client.internal.ExchangeStepAttemptKt.debugMessageEntry
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Party
import org.wfanet.panelmatch.client.internal.SignedExchangeCheckpoint
import org.wfanet.panelmatch.client.internal.copy
import org.wfanet.panelmatch.client.internal.exchangeCheckpoint
import org.wfanet.panelmatch.client.internal.exchangeStepAttempt
import org.wfanet.panelmatch.client.internal.signedExchangeCheckpoint
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.Fingerprinters.sha256
import org.wfanet.panelmatch.common.certificates.CertificateManager
import org.wfanet.panelmatch.common.secrets.SecretMap
import org.wfanet.panelmatch.common.storage.toByteString

/**
 * Implementation of [ApiClient] for running panel match without a Kingdom.
 *
 * @param identity the [Identity] of the party running this daemon
 * @param recurringExchangeIds recurring exchange IDs that this client will search for steps to
 *   execute
 * @param validExchangeWorkflows [SecretMap] containing valid pre-shared serialized workflows
 * @param certificateManager [CertificateManager] for accessing certificates to verify signed
 *   exchange checkpoints
 * @param algorithm [SignatureAlgorithm] to use when signing checkpoints
 * @param lookbackWindow positive duration of time representing how far in the past to search for
 *   steps to execute
 * @param stepTimeout positive duration of time after which in-progress steps will be marked as
 *   failed due to timeout
 * @param clock [Clock] for accessing the current time
 * @param getSharedStorage a function that builds a shared [StorageClient] given an
 *   [ExchangeDateKey]
 */
class KingdomlessApiClient(
  private val identity: Identity,
  private val recurringExchangeIds: List<String>,
  private val validExchangeWorkflows: SecretMap,
  private val certificateManager: CertificateManager,
  private val algorithm: SignatureAlgorithm,
  private val lookbackWindow: Duration,
  private val stepTimeout: Duration,
  private val clock: Clock,
  private val getSharedStorage: suspend (ExchangeDateKey) -> StorageClient,
) : ApiClient {

  init {
    require(lookbackWindow > Duration.ZERO) { "lookbackWindow must be positive" }
    require(stepTimeout > Duration.ZERO) { "stepTimeout must be positive" }
  }

  private val datedWorkflowIterator = DatedExchangeWorkflowIterator()

  override suspend fun claimExchangeStep(): ApiClient.ClaimedExchangeStep? {
    if (!datedWorkflowIterator.hasNext()) {
      return null
    }

    val firstWorkflow = datedWorkflowIterator.next()
    var currentWorkflow = firstWorkflow
    do {
      val checkpointManager = CheckpointManager(currentWorkflow)
      val attempt =
        checkpointMutex.withLock {
          checkpointManager.loadCheckpoints()
          if (checkpointManager.exchangeInTerminalState) {
            return@withLock null
          }
          val (step, stepIndex) = checkpointManager.findUnblockedStep() ?: return@withLock null
          val attempt = checkpointManager.claimStepAttempt(step.stepId, stepIndex)
          checkpointManager.writeCheckpointBlob()
          attempt
        }

      if (attempt != null) {
        val attemptKey =
          ExchangeStepAttemptKey(
            recurringExchangeId = currentWorkflow.exchangeDateKey.recurringExchangeId,
            exchangeId = currentWorkflow.exchangeDateKey.date.toString(),
            stepId = attempt.stepId,
            attemptId = attempt.attemptNumber.toString(),
          )
        return ApiClient.ClaimedExchangeStep(
          attemptKey = attemptKey,
          exchangeDate = currentWorkflow.exchangeDateKey.date,
          stepIndex = attempt.stepIndex,
          workflow = currentWorkflow.workflow,
          // Use the fingerprint computed by the other party. Later, when this ClaimedExchangeStep
          // is validated, this fingerprint will be compared against that of the local serialized
          // workflow.
          workflowFingerprint = checkpointManager.theirCheckpoint.workflowFingerprint,
        )
      }

      currentWorkflow = datedWorkflowIterator.next()
    } while (currentWorkflow != firstWorkflow)

    // No step available.
    return null
  }

  private fun CheckpointManager.claimStepAttempt(
    stepId: String,
    stepIndex: Int,
  ): ExchangeStepAttempt {
    val currentTime = clock.instant().toProtoTime()
    val greatestProgressEntry = findGreatestProgressEntry(stepId)
    if (greatestProgressEntry?.attempt?.state == ExchangeStepAttempt.State.IN_PROGRESS) {
      val updatedProgressEntry =
        greatestProgressEntry.copy {
          attempt =
            attempt.copy {
              state = ExchangeStepAttempt.State.FAILED
              debugMessageEntries += debugMessageEntry {
                message = "Attempt timed out after $stepTimeout"
                time = currentTime
              }
            }
          endTime = currentTime
        }
      updateProgressEntry(updatedProgressEntry)
    }

    val previousAttemptNumber = greatestProgressEntry?.attempt?.attemptNumber ?: 0
    val newProgressEntry = progressEntry {
      attempt = exchangeStepAttempt {
        this.stepId = stepId
        this.stepIndex = stepIndex
        attemptNumber = previousAttemptNumber + 1
        state = ExchangeStepAttempt.State.IN_PROGRESS
      }
      startTime = currentTime
    }
    appendProgressEntry(newProgressEntry)

    return newProgressEntry.attempt
  }

  override suspend fun appendLogEntry(key: ExchangeStepAttemptKey, messages: Iterable<String>) {
    val checkpointManager = CheckpointManager(key.toDatedExchangeWorkflow())
    checkpointMutex.withLock {
      checkpointManager.loadCheckpoints()
      val currentTime = clock.instant().toProtoTime()
      val progressEntry =
        checkpointManager.findProgressEntry(key.stepId, key.attemptId.toInt())
          ?: throw NoSuchElementException("No progress entry found for key: $key")
      checkpointManager.updateProgressEntry(
        progressEntry.copy {
          attempt =
            attempt.copy {
              debugMessageEntries +=
                messages.map { message ->
                  debugMessageEntry {
                    this.message = message
                    time = currentTime
                  }
                }
            }
        }
      )
      checkpointManager.writeCheckpointBlob()
    }
  }

  override suspend fun finishExchangeStepAttempt(
    key: ExchangeStepAttemptKey,
    finalState: ExchangeStepAttempt.State,
    logEntryMessages: Iterable<String>,
  ) {
    require(finalState in TERMINAL_ATTEMPT_STATES) {
      "finalState must be one of $TERMINAL_ATTEMPT_STATES but was $finalState"
    }
    val checkpointManager = CheckpointManager(key.toDatedExchangeWorkflow())
    checkpointMutex.withLock {
      checkpointManager.loadCheckpoints()
      val currentTime = clock.instant().toProtoTime()
      val progressEntry =
        checkpointManager.findProgressEntry(key.stepId, key.attemptId.toInt())
          ?: throw NoSuchElementException("No progress entry found for key: $key")
      checkpointManager.updateProgressEntry(
        progressEntry.copy {
          attempt =
            attempt.copy {
              state = finalState
              debugMessageEntries +=
                logEntryMessages.map { message ->
                  debugMessageEntry {
                    this.message = message
                    time = currentTime
                  }
                }
            }
          endTime = currentTime
        }
      )
      checkpointManager.writeCheckpointBlob()
    }
  }

  private suspend fun ExchangeStepAttemptKey.toDatedExchangeWorkflow(): DatedExchangeWorkflow {
    val serializedWorkflow =
      validExchangeWorkflows.get(recurringExchangeId)
        ?: throw NoSuchElementException("Recurring exchange not found: $recurringExchangeId")
    val workflow = ExchangeWorkflow.parseFrom(serializedWorkflow)
    val exchangeDateKey =
      ExchangeDateKey(recurringExchangeId = recurringExchangeId, date = LocalDate.parse(exchangeId))
    return DatedExchangeWorkflow(exchangeDateKey, workflow, serializedWorkflow)
  }

  private fun Instant.toLocalDate(): LocalDate = atZone(clock.zone).toLocalDate()

  private data class DatedExchangeWorkflow(
    val exchangeDateKey: ExchangeDateKey,
    val workflow: ExchangeWorkflow,
    val serializedWorkflow: ByteString,
  )

  /**
   * Helper for managing the state of an exchange for the given [datedWorkflow].
   *
   * After instantiating a [CheckpointManager], [loadCheckpoints] must be called in order to read
   * the checkpoints from shared storage and build the in-memory exchange state. If any changes are
   * made to this party's checkpoint, call [writeCheckpointBlob] to update the checkpoint in shared
   * storage.
   *
   * In order to prevent concurrent modification of checkpoints by this daemon, a mutex should be
   * acquired before calling [loadCheckpoints] and released after calling [writeCheckpointBlob].
   */
  private inner class CheckpointManager(private val datedWorkflow: DatedExchangeWorkflow) {

    private lateinit var checkpointStore: CheckpointStore
    private lateinit var dataProviderCheckpoint: ExchangeCheckpoint
    private lateinit var modelProviderCheckpoint: ExchangeCheckpoint
    private lateinit var dependencyGraph: ExchangeWorkflowDependencyGraph

    /** [ExchangeCheckpoint] belonging to the party that is running this daemon. */
    val myCheckpoint: ExchangeCheckpoint
      get() {
        return when (identity.party) {
          Party.DATA_PROVIDER -> dataProviderCheckpoint
          Party.MODEL_PROVIDER -> modelProviderCheckpoint
          Party.PARTY_UNSPECIFIED,
          Party.UNRECOGNIZED -> error("Unsupported party: ${identity.party}")
        }
      }

    /** [ExchangeCheckpoint] belonging to the party that is not running this daemon. */
    val theirCheckpoint: ExchangeCheckpoint
      get() {
        return when (identity.party) {
          Party.DATA_PROVIDER -> modelProviderCheckpoint
          Party.MODEL_PROVIDER -> dataProviderCheckpoint
          Party.PARTY_UNSPECIFIED,
          Party.UNRECOGNIZED -> error("Unsupported party: ${identity.party}")
        }
      }

    /** Returns true if the exchange is in a terminal state. */
    val exchangeInTerminalState: Boolean
      get() {
        val succeeded =
          dataProviderCheckpoint.exchangeState == ExchangeState.SUCCEEDED &&
            modelProviderCheckpoint.exchangeState == ExchangeState.SUCCEEDED
        val failed =
          dataProviderCheckpoint.exchangeState == ExchangeState.FAILED ||
            modelProviderCheckpoint.exchangeState == ExchangeState.FAILED
        return succeeded || failed
      }

    /**
     * Fetches each party's checkpoint from shared storage and builds the
     * [ExchangeWorkflowDependencyGraph] representing the current state of the exchange.
     */
    suspend fun loadCheckpoints(
      retryAttempts: Int = 3,
      retryBackoff: ExponentialBackoff = ExponentialBackoff(),
    ) {
      val storageClient = getSharedStorage(datedWorkflow.exchangeDateKey)
      checkpointStore = CheckpointStore(storageClient)
      dataProviderCheckpoint =
        retry(retryAttempts, retryBackoff, { it is EmptyCheckpointException }) {
          checkpointStore.readCheckpoint(Party.DATA_PROVIDER, datedWorkflow.exchangeDateKey)
            ?: createNewCheckpoint(Party.DATA_PROVIDER)
        }
      modelProviderCheckpoint =
        retry(retryAttempts, retryBackoff, { it is EmptyCheckpointException }) {
          checkpointStore.readCheckpoint(Party.MODEL_PROVIDER, datedWorkflow.exchangeDateKey)
            ?: createNewCheckpoint(Party.MODEL_PROVIDER)
        }
      dependencyGraph = buildDependencyGraph()
    }

    /**
     * Writes this party's checkpoint to shared storage, overwriting the checkpoint if it already
     * exists.
     */
    suspend fun writeCheckpointBlob() {
      checkpointStore.writeCheckpoint(datedWorkflow.exchangeDateKey, myCheckpoint)
    }

    /** Returns an unblocked step for this party, or null if none is available. */
    fun findUnblockedStep(): ExchangeWorkflowDependencyGraph.IndexedStep? {
      return dependencyGraph.getUnblockedStep(identity.party)
    }

    /**
     * Returns the single [ExchangeCheckpoint.ProgressEntry] belonging to this party with the given
     * [stepId] and [attemptNumber]. Returns null if there is no such entry, or if there are
     * multiple matching entries.
     */
    fun findProgressEntry(stepId: String, attemptNumber: Int): ExchangeCheckpoint.ProgressEntry? {
      return myCheckpoint.progressEntriesList.singleOrNull {
        it.attempt.stepId == stepId && it.attempt.attemptNumber == attemptNumber
      }
    }

    /**
     * Returns the [ExchangeCheckpoint.ProgressEntry] belonging to this party with the given
     * [stepId] having the greatest attempt number. Returns null if there is no entry with the given
     * [stepId].
     */
    fun findGreatestProgressEntry(stepId: String): ExchangeCheckpoint.ProgressEntry? {
      return myCheckpoint.progressEntriesList
        .filter { it.attempt.stepId == stepId }
        .maxByOrNull { it.attempt.attemptNumber }
    }

    /**
     * Appends the given [progressEntry] at the end of this party's in-memory checkpoint. Updates
     * the in-memory dependency graph to reflect the new progress entry. If the given entry causes
     * the exchange to reach a terminal state, then updates the exchange state accordingly.
     *
     * Does not write the updated checkpoint blob back to shared storage.
     */
    fun appendProgressEntry(progressEntry: ExchangeCheckpoint.ProgressEntry) {
      updateDependencyGraph(progressEntry)
      val updatedCheckpoint =
        myCheckpoint.copy {
          progressEntries += progressEntry
          if (exchangeState == ExchangeState.FAILED) {
            return@copy
          }
          if (progressEntry.attempt.state == ExchangeStepAttempt.State.FAILED_STEP) {
            exchangeState = ExchangeState.FAILED
          } else if (!dependencyGraph.hasRemainingSteps(identity.party)) {
            exchangeState = ExchangeState.SUCCEEDED
          }
        }
      setMyCheckpoint(updatedCheckpoint)
    }

    /**
     * Overwrites the given [progressEntry] in this party's in-memory checkpoint. The entry to
     * update is identified by matching the step ID and attempt number from the provided progress
     * entry. Updates the in-memory dependency graph to reflect the new progress entry. If the given
     * entry causes the exchange to reach a terminal state, then updates the exchange state
     * accordingly.
     *
     * Does not write the updated checkpoint blob back to shared storage.
     */
    fun updateProgressEntry(progressEntry: ExchangeCheckpoint.ProgressEntry) {
      updateDependencyGraph(progressEntry)

      val index =
        myCheckpoint.progressEntriesList
          .withIndex()
          .single { (_, entry) ->
            entry.attempt.stepId == progressEntry.attempt.stepId &&
              entry.attempt.attemptNumber == progressEntry.attempt.attemptNumber
          }
          .index
      val updatedCheckpoint =
        myCheckpoint.copy {
          progressEntries[index] = progressEntry
          if (exchangeState == ExchangeState.FAILED) {
            return@copy
          }
          if (progressEntry.attempt.state == ExchangeStepAttempt.State.FAILED_STEP) {
            exchangeState = ExchangeState.FAILED
          } else if (!dependencyGraph.hasRemainingSteps(identity.party)) {
            exchangeState = ExchangeState.SUCCEEDED
          }
        }

      setMyCheckpoint(updatedCheckpoint)
    }

    /** Updates [dependencyGraph] to reflect the state of the given [progressEntry]. */
    private fun updateDependencyGraph(progressEntry: ExchangeCheckpoint.ProgressEntry) {
      val stepId = progressEntry.attempt.stepId
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (progressEntry.attempt.state) {
        ExchangeStepAttempt.State.IN_PROGRESS -> dependencyGraph.markStepAsInProgress(stepId)
        ExchangeStepAttempt.State.SUCCEEDED -> dependencyGraph.markStepAsCompleted(stepId)
        ExchangeStepAttempt.State.STATE_UNSPECIFIED,
        ExchangeStepAttempt.State.UNRECOGNIZED,
        ExchangeStepAttempt.State.FAILED,
        ExchangeStepAttempt.State.FAILED_STEP -> {}
      }
    }

    /** Sets the in-memory checkpoint belonging to this party to [checkpoint]. */
    private fun setMyCheckpoint(checkpoint: ExchangeCheckpoint) {
      when (identity.party) {
        Party.DATA_PROVIDER -> dataProviderCheckpoint = checkpoint
        Party.MODEL_PROVIDER -> modelProviderCheckpoint = checkpoint
        Party.PARTY_UNSPECIFIED,
        Party.UNRECOGNIZED -> error("Unsupported party: ${identity.party}")
      }
    }

    /** Returns a new checkpoint for [owner] with no progress entries. */
    private fun createNewCheckpoint(owner: Party): ExchangeCheckpoint {
      return exchangeCheckpoint {
        party = owner
        recurringExchangeId = datedWorkflow.exchangeDateKey.recurringExchangeId
        exchangeDate = datedWorkflow.exchangeDateKey.date.toProtoDate()
        workflowFingerprint = sha256(datedWorkflow.serializedWorkflow)
        exchangeState = ExchangeState.IN_PROGRESS
      }
    }

    /**
     * Returns a new [ExchangeWorkflowDependencyGraph] for the current workflow whose state reflects
     * the progress of both [dataProviderCheckpoint] and [modelProviderCheckpoint].
     */
    private fun buildDependencyGraph(): ExchangeWorkflowDependencyGraph {
      val inProgressStepExpirationBoundary = clock.instant().minus(stepTimeout)
      val dependencyGraph = ExchangeWorkflowDependencyGraph.fromWorkflow(datedWorkflow.workflow)
      val orderedProgressEntries =
        listOf(dataProviderCheckpoint, modelProviderCheckpoint)
          .flatMap { it.progressEntriesList }
          .sortedBy { it.attempt.stepIndex }

      for (entry in orderedProgressEntries) {
        val attempt = entry.attempt
        val attemptStartTime = entry.startTime.toInstant()
        if (attempt.state == ExchangeStepAttempt.State.SUCCEEDED) {
          dependencyGraph.markStepAsCompleted(attempt.stepId)
        }
        if (
          attempt.state == ExchangeStepAttempt.State.IN_PROGRESS &&
            inProgressStepExpirationBoundary < attemptStartTime
        ) {
          dependencyGraph.markStepAsInProgress(attempt.stepId)
        }
      }

      return dependencyGraph
    }
  }

  /**
   * Suspending iterator for walking through [DatedExchangeWorkflow]s in a continuous loop.
   *
   * Provided that there is at least one entry in [recurringExchangeIds], calling [next] will always
   * return a [DatedExchangeWorkflow]. The return order begins with the oldest date for each
   * recurring exchange, then the next date for each recurring exchange, and so on until reaching
   * the present date. Here, the "oldest date" refers to the date obtained by subtracting
   * [lookbackWindow] from the current time. If the resulting date is before the first exchange date
   * for a given recurring exchange, then that date is skipped.
   *
   * After returning all dated workflows for all recurring exchanges, this iterator loops back to
   * the beginning. Since this process will continue indefinitely, it is up to the caller to detect
   * when they have looped back to the beginning and terminate the loop.
   */
  private inner class DatedExchangeWorkflowIterator {

    private val exchangeDateKeys: Iterator<ExchangeDateKey> =
      sequence {
          if (recurringExchangeIds.isEmpty()) {
            return@sequence
          }
          while (true) {
            val currentTime = clock.instant()
            val currentDate = currentTime.toLocalDate()
            var exchangeDate = currentTime.minus(lookbackWindow).toLocalDate()
            while (exchangeDate <= currentDate) {
              for (recurringExchangeId in recurringExchangeIds) {
                yield(ExchangeDateKey(recurringExchangeId, exchangeDate))
              }
              exchangeDate = exchangeDate.plusDays(1L)
            }
          }
        }
        .iterator()

    /** Returns true if this iterator has remaining elements, and false otherwise. */
    fun hasNext(): Boolean {
      return exchangeDateKeys.hasNext()
    }

    /** Returns the next element as described by the class documentation. */
    suspend fun next(): DatedExchangeWorkflow {
      if (!hasNext()) {
        throw IllegalStateException("No recurring exchanges found")
      }
      val exchangeDateKey = exchangeDateKeys.next()
      val serializedWorkflow =
        validExchangeWorkflows.get(exchangeDateKey.recurringExchangeId)
          ?: throw NoSuchElementException(
            "Recurring exchange not found: ${exchangeDateKey.recurringExchangeId}"
          )
      val workflow = ExchangeWorkflow.parseFrom(serializedWorkflow)
      val firstExchangeDate = workflow.firstExchangeDate.toLocalDate()
      return if (exchangeDateKey.date < firstExchangeDate) {
        next()
      } else {
        DatedExchangeWorkflow(exchangeDateKey, workflow, serializedWorkflow)
      }
    }
  }

  /** [Store] for reading and writing checkpoints. */
  private inner class CheckpointStore(sharedStorageClient: StorageClient) :
    Store<Party>(sharedStorageClient) {

    override val blobKeyPrefix: String
      get() = BLOB_KEY_PREFIX

    override fun deriveBlobKey(context: Party): String {
      return when (context) {
        Party.DATA_PROVIDER -> DATA_PROVIDER_CHECKPOINT_BLOB_KEY
        Party.MODEL_PROVIDER -> MODEL_PROVIDER_CHECKPOINT_BLOB_KEY
        Party.PARTY_UNSPECIFIED,
        Party.UNRECOGNIZED -> throw IllegalArgumentException("Unsupported party: $context")
      }
    }

    /**
     * Reads the [SignedExchangeCheckpoint] belonging to [party] with the given [exchangeDateKey],
     * verifies the signature, and returns the verified [ExchangeCheckpoint]. Returns null if no
     * checkpoint exists, indicating that the given party hasn't begun performing tasks for the
     * exchange date yet. Throws [EmptyCheckpointException] if the checkpoint is empty, which is a
     * possible indicator that [party] is currently writing the blob.
     */
    suspend fun readCheckpoint(
      party: Party,
      exchangeDateKey: ExchangeDateKey,
    ): ExchangeCheckpoint? {
      val serializedSignedCheckpoint = get(party)?.toByteString() ?: return null
      if (serializedSignedCheckpoint.isEmpty) {
        // TODO(@robinsons): It's possible to avoid this situation if StorageClient can perform a
        // direct upload. Investigate the feasibility and revisit this.
        throw EmptyCheckpointException(party, exchangeDateKey)
      }
      val signedCheckpoint = SignedExchangeCheckpoint.parseFrom(serializedSignedCheckpoint)
      val certificate =
        certificateManager.getCertificate(exchangeDateKey, signedCheckpoint.certName)
      val algorithm =
        checkNotNull(SignatureAlgorithm.fromOid(signedCheckpoint.signatureAlgorithmOid)) {
          "Invalid signature algorithm OID: ${signedCheckpoint.signatureAlgorithmOid}"
        }
      check(
        certificate.verifySignature(
          algorithm,
          signedCheckpoint.serializedCheckpoint,
          signedCheckpoint.signature,
        )
      ) {
        "Invalid signature for exchange checkpoint: key=$exchangeDateKey party=$party"
      }
      return ExchangeCheckpoint.parseFrom(signedCheckpoint.serializedCheckpoint)
    }

    /** Signs [checkpoint] and writes the resulting [SignedExchangeCheckpoint] to this store. */
    suspend fun writeCheckpoint(exchangeDateKey: ExchangeDateKey, checkpoint: ExchangeCheckpoint) {
      val certificateName = certificateManager.createForExchange(exchangeDateKey)
      val privateKey = certificateManager.getExchangePrivateKey(exchangeDateKey)
      val serializedCheckpoint = checkpoint.toByteString()
      val signature = privateKey.sign(algorithm, serializedCheckpoint)
      val signedCheckpoint = signedExchangeCheckpoint {
        this.serializedCheckpoint = serializedCheckpoint
        this.signature = signature
        signatureAlgorithmOid = algorithm.oid
        certName = certificateName
      }
      write(identity.party, signedCheckpoint.toByteString())
    }
  }

  class EmptyCheckpointException(party: Party, exchangeDateKey: ExchangeDateKey) :
    Exception("Empty checkpoint for party=$party exchangeDateKey=$exchangeDateKey")

  /**
   * Executes [block] and returns the result. If [block] throws an exception that satisfies
   * [shouldRetry], the exception is suppressed and [block] is retried according to the specified
   * [attempts] and [backoff]. If [block] does not succeed after retrying, the last exception
   * encountered is rethrown. If [shouldRetry] returns false, the exception is rethrown immediately
   * without further retries.
   */
  private suspend fun <T> retry(
    attempts: Int,
    backoff: ExponentialBackoff,
    shouldRetry: (Exception) -> Boolean,
    block: suspend () -> T,
  ): T {
    require(attempts > 0) { "attempts must be at least 1" }
    var lastException: Exception? = null
    for (attempt in 1..attempts) {
      try {
        return block()
      } catch (e: Exception) {
        if (shouldRetry(e)) {
          lastException = e
          delay(backoff.durationForAttempt(attempt))
        } else {
          throw e
        }
      }
    }
    throw lastException!!
  }

  companion object {
    private const val BLOB_KEY_PREFIX = "checkpoints"
    private const val DATA_PROVIDER_CHECKPOINT_BLOB_KEY = "edp-checkpoint"
    private const val MODEL_PROVIDER_CHECKPOINT_BLOB_KEY = "mp-checkpoint"

    private val TERMINAL_ATTEMPT_STATES =
      setOf(
        ExchangeStepAttempt.State.SUCCEEDED,
        ExchangeStepAttempt.State.FAILED,
        ExchangeStepAttempt.State.FAILED_STEP,
      )

    private val checkpointMutex = Mutex()
  }
}
