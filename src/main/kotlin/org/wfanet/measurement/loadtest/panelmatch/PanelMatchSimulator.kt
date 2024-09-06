/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.loadtest.panelmatch

import com.google.common.truth.Truth
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import io.grpc.StatusException
import java.time.LocalDate
import java.util.logging.Logger
import kotlin.test.assertNotNull
import kotlinx.coroutines.delay
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeKey
import org.wfanet.measurement.api.v2alpha.CanonicalRecurringExchangeKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.Exchange
import org.wfanet.measurement.api.v2alpha.ExchangeKey
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.exchangeIdentifiers
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.schedule
import org.wfanet.measurement.api.v2alpha.ExchangesGrpcKt
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsRequestKt
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.getExchangeRequest
import org.wfanet.measurement.api.v2alpha.listExchangeStepsRequest
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt
import org.wfanet.measurement.internal.kingdom.createRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.recurringExchange
import org.wfanet.measurement.internal.kingdom.recurringExchangeDetails
import org.wfanet.measurement.kingdom.service.api.v2alpha.toInternal
import org.wfanet.measurement.loadtest.panelmatch.PanelMatchCorrectnessTestInputProvider.HKDF_PEPPER
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.deploy.DaemonStorageClientDefaults
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyAndIdCollection
import org.wfanet.panelmatch.client.privatemembership.keyedDecryptedEventDataSet
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.common.parseDelimitedMessages
import org.wfanet.panelmatch.common.storage.toByteString
import org.wfanet.panelmatch.integration.testing.parsePlaintextResults

data class EntitiesData(
  val dataProviderKey: DataProviderKey,
  val modelProviderKey: ModelProviderKey,
) {
  val externalDataProviderId: Long
    get() = apiIdToExternalId(dataProviderKey.dataProviderId)

  val externalModelProviderId: Long
    get() = apiIdToExternalId(modelProviderKey.modelProviderId)
}

/** Simulator for PanelMatch flows. */
class PanelMatchSimulator(
  private val entitiesData: EntitiesData,
  private val recurringExchangeClient: RecurringExchangesGrpcKt.RecurringExchangesCoroutineStub,
  private val exchangeClient: ExchangesGrpcKt.ExchangesCoroutineStub,
  private val exchangeStepsClient: ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub,
  private val schedule: String,
  private val publicApiVersion: String,
  private val exchangeDate: LocalDate,
  private val dataProviderPrivateStorageDetails: StorageDetails,
  private val modelProviderPrivateStorageDetails: StorageDetails,
  private val dataProviderSharedStorageDetails: StorageDetails,
  private val modelProviderSharedStorageDetails: StorageDetails,
  private val dpForwardedStorage: StorageClient,
  private val mpForwardedStorage: StorageClient,
  private val dataProviderDefaults: DaemonStorageClientDefaults,
  private val modelProviderDefaults: DaemonStorageClientDefaults,
) {

  private lateinit var exchangeKey: ExchangeKey
  private val TERMINAL_STEP_STATES = setOf(ExchangeStep.State.SUCCEEDED, ExchangeStep.State.FAILED)
  private val READY_STEP_STATES =
    setOf(
      ExchangeStep.State.IN_PROGRESS,
      ExchangeStep.State.READY,
      ExchangeStep.State.READY_FOR_RETRY,
    )
  private val TERMINAL_EXCHANGE_STATES = setOf(Exchange.State.SUCCEEDED, Exchange.State.FAILED)
  private val logger = Logger.getLogger(this::class.java.name)

  fun populateWorkflow(workflow: ExchangeWorkflow): ExchangeWorkflow {
    return workflow.copy {
      exchangeIdentifiers = exchangeIdentifiers {
        dataProvider = entitiesData.dataProviderKey.toName()
        modelProvider = entitiesData.modelProviderKey.toName()
        sharedStorageOwner = ExchangeWorkflow.Party.DATA_PROVIDER
      }
      firstExchangeDate = exchangeDate.toProtoDate()
      repetitionSchedule = schedule { cronExpression = schedule }
    }
  }

  suspend fun executeDoubleBlindExchangeWorkflow(workflow: ExchangeWorkflow) {
    val initialDataProviderInputs =
      PanelMatchCorrectnessTestInputProvider.getInitialDataProviderInputsForTestType(
        PanelMatchCorrectnessTestInputProvider.TestType.DOUBLE_BLIND
      )
    val initialModelProviderInputs =
      PanelMatchCorrectnessTestInputProvider.getInitialModelProviderInputsForTestType(
        PanelMatchCorrectnessTestInputProvider.TestType.DOUBLE_BLIND
      )

    setupWorkflow(workflow, initialDataProviderInputs, initialModelProviderInputs)
    waitForExchangeToComplete()

    // TODO(@marcopremier): Check the specific parsed output
    val result = mpForwardedStorage.getBlob("mp-decrypted-join-keys")
    JoinKeyAndIdCollection.parseFrom(result?.toByteString())
  }

  suspend fun executeFullWithPreprocessingWorkflow(workflow: ExchangeWorkflow) {
    val initialDataProviderInputs =
      PanelMatchCorrectnessTestInputProvider.getInitialDataProviderInputsForTestType(
        PanelMatchCorrectnessTestInputProvider.TestType.FULL_WITH_PREPROCESSING
      )
    val initialModelProviderInputs =
      PanelMatchCorrectnessTestInputProvider.getInitialModelProviderInputsForTestType(
        PanelMatchCorrectnessTestInputProvider.TestType.FULL_WITH_PREPROCESSING
      )

    setupWorkflow(workflow, initialDataProviderInputs, initialModelProviderInputs)
    waitForExchangeToComplete()

    val blob = mpForwardedStorage.getBlob("decrypted-event-data-0-of-1")?.toByteString()
    assertNotNull(blob)

    val decryptedEvents =
      parsePlaintextResults(blob.parseDelimitedMessages(keyedDecryptedEventDataSet {})).map {
        it.joinKey to it.plaintexts
      }

    Truth.assertThat(decryptedEvents)
      .containsExactly(
        "join-key-1" to listOf("payload-1-for-join-key-1", "payload-2-for-join-key-1"),
        "join-key-2" to listOf("payload-1-for-join-key-2", "payload-2-for-join-key-2"),
      )
  }

  suspend fun executeMiniWorkflow(workflow: ExchangeWorkflow) {
    val initialDataProviderInputs =
      PanelMatchCorrectnessTestInputProvider.getInitialDataProviderInputsForTestType(
        PanelMatchCorrectnessTestInputProvider.TestType.MINI_EXCHANGE
      )
    val initialModelProviderInputs =
      PanelMatchCorrectnessTestInputProvider.getInitialModelProviderInputsForTestType(
        PanelMatchCorrectnessTestInputProvider.TestType.MINI_EXCHANGE
      )

    setupWorkflow(workflow, initialDataProviderInputs, initialModelProviderInputs)
    waitForExchangeToComplete()

    Truth.assertThat(mpForwardedStorage.getBlob("mp-hkdf-pepper")?.toByteString())
      .isEqualTo(HKDF_PEPPER.toByteStringUtf8())
  }

  /** Block flow execution until the [Exchange] reaches a [TERMINAL_EXCHANGE_STATES] */
  private suspend fun waitForExchangeToComplete() {
    while (!isDone()) {
      delay(10000)
    }
  }

  private suspend fun createRecurringExchange(exchangeWorkflow: ExchangeWorkflow): Long {
    val recurringExchangeId =
      recurringExchangeClient
        .createRecurringExchange(
          createRecurringExchangeRequest {
            recurringExchange = recurringExchange {
              externalDataProviderId = entitiesData.externalDataProviderId
              externalModelProviderId = entitiesData.externalModelProviderId
              state = RecurringExchange.State.ACTIVE
              details = recurringExchangeDetails {
                this.exchangeWorkflow = exchangeWorkflow.toInternal()
                cronSchedule = schedule
                externalExchangeWorkflow = exchangeWorkflow.toByteString()
                apiVersion = publicApiVersion
              }
              nextExchangeDate = exchangeDate.toProtoDate()
            }
          }
        )
        .externalRecurringExchangeId
    return recurringExchangeId
  }

  private suspend fun isDone(): Boolean {
    val request = getExchangeRequest { name = exchangeKey.toName() }
    return try {
      val exchange = exchangeClient.getExchange(request)
      val steps = getSteps()
      assertNotDeadlocked(steps)
      logger.info("Exchange is in state: ${exchange.state}.")
      exchange.state in TERMINAL_EXCHANGE_STATES
    } catch (e: StatusException) {
      false
    }
  }

  /** Create resources needed to run a panel exchange. */
  private suspend fun setupWorkflow(
    exchangeWorkflow: ExchangeWorkflow,
    initialDataProviderInputs: Map<String, ByteString>,
    initialModelProviderInputs: Map<String, ByteString>,
  ) {

    val externalRecurringExchangeId = createRecurringExchange(exchangeWorkflow)

    val recurringExchangeKey =
      CanonicalRecurringExchangeKey(externalIdToApiId(externalRecurringExchangeId))

    exchangeKey =
      CanonicalExchangeKey(
        recurringExchangeId = recurringExchangeKey.recurringExchangeId,
        exchangeId = exchangeDate.toString(),
      )

    // Setup data provider storage
    dataProviderDefaults.validExchangeWorkflows.put(
      recurringExchangeKey.recurringExchangeId,
      exchangeWorkflow.toByteString(),
    )
    dataProviderDefaults.privateStorageInfo.put(
      recurringExchangeKey.recurringExchangeId,
      dataProviderPrivateStorageDetails,
    )
    dataProviderDefaults.sharedStorageInfo.put(
      recurringExchangeKey.recurringExchangeId,
      dataProviderSharedStorageDetails,
    )
    for ((blobKey, value) in initialDataProviderInputs) {
      dpForwardedStorage.writeBlob(blobKey, value)
    }

    // Setup model provider storage
    modelProviderDefaults.validExchangeWorkflows.put(
      recurringExchangeKey.recurringExchangeId,
      exchangeWorkflow.toByteString(),
    )
    modelProviderDefaults.privateStorageInfo.put(
      recurringExchangeKey.recurringExchangeId,
      modelProviderPrivateStorageDetails,
    )
    modelProviderDefaults.sharedStorageInfo.put(
      recurringExchangeKey.recurringExchangeId,
      modelProviderSharedStorageDetails,
    )
    for ((blobKey, value) in initialModelProviderInputs) {
      mpForwardedStorage.writeBlob(blobKey, value)
    }
  }

  private fun assertNotDeadlocked(steps: Iterable<ExchangeStep>) {
    if (steps.any { it.state !in TERMINAL_STEP_STATES }) {
      Truth.assertThat(steps.any { it.state in READY_STEP_STATES }).isTrue()
    }
  }

  private suspend fun getSteps(): List<ExchangeStep> {
    return exchangeStepsClient
      .listExchangeSteps(
        listExchangeStepsRequest {
          parent = exchangeKey.toName()
          pageSize = 50
          filter = ListExchangeStepsRequestKt.filter { exchangeDates += exchangeDate.toProtoDate() }
        }
      )
      .exchangeStepsList
      .sortedBy { step -> step.stepIndex }
  }
}
