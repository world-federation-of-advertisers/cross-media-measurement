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
import io.grpc.ManagedChannel
import io.grpc.StatusException
import java.time.LocalDate
import java.util.logging.Logger
import kotlinx.coroutines.delay
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeKey
import org.wfanet.measurement.api.v2alpha.CanonicalRecurringExchangeKey
import org.wfanet.measurement.api.v2alpha.Exchange
import org.wfanet.measurement.api.v2alpha.ExchangeKey
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ExchangesGrpcKt
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsRequestKt
import org.wfanet.measurement.api.v2alpha.getExchangeRequest
import org.wfanet.measurement.api.v2alpha.listExchangeStepsRequest
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.loadtest.panelmatchresourcesetup.PanelMatchResourceSetup
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.forwarded.ForwardedStorageClient
import org.wfanet.panelmatch.client.common.joinKeyAndIdOf
import org.wfanet.panelmatch.client.deploy.DaemonStorageClientDefaults
import org.wfanet.panelmatch.client.exchangetasks.joinKeyAndIdCollection
import org.wfanet.panelmatch.client.storage.StorageDetails

class PanelMatchSimulator(private val panelMatchResourceSetup: PanelMatchResourceSetup,
                          private val schedule: String,
                          private val apiVersion: String,
                          private val exchangeDate: LocalDate,
                          private val dataProviderPrivateStorageDetails: StorageDetails,
                          private val modelProviderPrivateStorageDetails: StorageDetails,
                          private val dataProviderSharedStorageDetails: StorageDetails,
                          private val modelProviderSharedStorageDetails: StorageDetails,
                          private val dpForwardedStorage: StorageClient,
                          private val mpForwardedStorage: StorageClient,
                          private val publicChannel: ManagedChannel,
                          private val dataProviderDefaults: DaemonStorageClientDefaults,
                          private val modelProviderDefaults: DaemonStorageClientDefaults,) {

  private lateinit var exchangeKey: ExchangeKey
  private val TERMINAL_STEP_STATES = setOf(ExchangeStep.State.SUCCEEDED, ExchangeStep.State.FAILED)
  private val READY_STEP_STATES =
    setOf(
      ExchangeStep.State.IN_PROGRESS,
      ExchangeStep.State.READY,
      ExchangeStep.State.READY_FOR_RETRY
    )
  private val TERMINAL_EXCHANGE_STATES = setOf(Exchange.State.SUCCEEDED, Exchange.State.FAILED)
  private val logger = Logger.getLogger(this::class.java.name)

  suspend fun executeDoubleBlindExchangeWorkflow(workflow: ExchangeWorkflow) {
    val EDP_COMMUTATIVE_DETERMINISTIC_KEY = "some-key".toByteStringUtf8()
    val PLAINTEXT_JOIN_KEYS = joinKeyAndIdCollection {
      joinKeyAndIds +=
        joinKeyAndIdOf("join-key-1".toByteStringUtf8(), "join-key-id-1".toByteStringUtf8())
      joinKeyAndIds +=
        joinKeyAndIdOf("join-key-2".toByteStringUtf8(), "join-key-id-2".toByteStringUtf8())
    }
    val initialDataProviderInputs: Map<String, ByteString> =
      mapOf("edp-commutative-deterministic-key" to EDP_COMMUTATIVE_DETERMINISTIC_KEY)

    val initialModelProviderInputs: Map<String, ByteString> =
      mapOf("mp-plaintext-join-keys" to PLAINTEXT_JOIN_KEYS.toByteString())
    setupWorkflow(workflow, initialDataProviderInputs, initialModelProviderInputs)
    waitForExchangeAndValidate()
  }

  suspend fun executeFullWithPreprocessingWorkflow(workflow: ExchangeWorkflow) {}

  suspend fun executeFullWorkflow(workflow: ExchangeWorkflow) {}

  suspend fun executeMiniWorkflow(workflow: ExchangeWorkflow) {
    val HKDF_PEPPER = "some-hkdf-pepper".toByteStringUtf8()
    val initialDataProviderInputs: Map<String, ByteString> =
      mapOf("edp-hkdf-pepper" to HKDF_PEPPER)
    val initialModelProviderInputs: Map<String, ByteString> = emptyMap()

    setupWorkflow(workflow, initialDataProviderInputs, initialModelProviderInputs)
    waitForExchangeAndValidate()
  }

  suspend fun executeSingleStepWorkflow(workflow: ExchangeWorkflow) {}

  private suspend fun waitForExchangeAndValidate() {
    val exchangeClient = ExchangesGrpcKt.ExchangesCoroutineStub(publicChannel)
    val exchangeStepsClient = ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub(publicChannel)
    while (!isDone(exchangeClient, exchangeStepsClient)) {
      delay(10000)
    }
    //validate()
  }

  private suspend fun setupWorkflow(exchangeWorkflow: ExchangeWorkflow,
                                    initialDataProviderInputs: Map<String, ByteString>,
                                    initialModelProviderInputs: Map<String, ByteString>) {

    val externalRecurringExchangeId =
      panelMatchResourceSetup.createRecurringExchange(
        externalDataProvider = PanelMatchResourceSetup.dataProviderId,
        externalModelProvider = PanelMatchResourceSetup.modelProviderId,
        exchangeDate = exchangeDate.toProtoDate(),
        exchangeSchedule = schedule,
        publicApiVersion = apiVersion,
        exchangeWorkflow = exchangeWorkflow
      )

    val recurringExchangeKey =
      CanonicalRecurringExchangeKey(externalIdToApiId(externalRecurringExchangeId))

    exchangeKey =
      CanonicalExchangeKey(
        recurringExchangeId = recurringExchangeKey.recurringExchangeId,
        exchangeId = exchangeDate.toString()
      )

    // Setup data provider storage
    dataProviderDefaults.validExchangeWorkflows.put(
      recurringExchangeKey.recurringExchangeId,
      exchangeWorkflow.toByteString()
    )
    dataProviderDefaults.privateStorageInfo.put(
      recurringExchangeKey.recurringExchangeId,
      dataProviderPrivateStorageDetails
    )
    dataProviderDefaults.sharedStorageInfo.put(
      recurringExchangeKey.recurringExchangeId,
      dataProviderSharedStorageDetails
    )
    for ((blobKey, value) in initialDataProviderInputs) {
      dpForwardedStorage.writeBlob(blobKey, value)
    }

    // Setup model provider storage
    modelProviderDefaults.validExchangeWorkflows.put(
      recurringExchangeKey.recurringExchangeId,
      exchangeWorkflow.toByteString()
    )
    modelProviderDefaults.privateStorageInfo.put(
      recurringExchangeKey.recurringExchangeId,
      modelProviderPrivateStorageDetails
    )
    modelProviderDefaults.sharedStorageInfo.put(
      recurringExchangeKey.recurringExchangeId,
      modelProviderSharedStorageDetails
    )
    for ((blobKey, value) in initialModelProviderInputs) {
      mpForwardedStorage.writeBlob(blobKey, value)
    }

  }

  private suspend fun isDone(
    exchangesClient: ExchangesGrpcKt.ExchangesCoroutineStub,
    exchangeStepsClient: ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
  ): Boolean {
    val request = getExchangeRequest { name = exchangeKey.toName() }
    return try {
      val exchange = exchangesClient.getExchange(request)
      val steps = getSteps(exchangeStepsClient)
      assertNotDeadlocked(steps)
      logger.info("Exchange is in state: ${exchange.state}.")
      exchange.state in TERMINAL_EXCHANGE_STATES
    } catch (e: StatusException) {
      false
    }
  }

  private fun assertNotDeadlocked(steps: Iterable<ExchangeStep>) {
    if (steps.any { it.state !in TERMINAL_STEP_STATES }) {
      Truth.assertThat(steps.any { it.state in READY_STEP_STATES }).isTrue()
    }
  }

  private suspend fun getSteps(
    exchangeStepsClient: ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
  ): List<ExchangeStep> {
    return exchangeStepsClient
      .listExchangeSteps(
        listExchangeStepsRequest {
          parent = exchangeKey.toName()
          pageSize = 50
          filter =
            ListExchangeStepsRequestKt.filter { exchangeDates += exchangeDate.toProtoDate() }
        }
      )
      .exchangeStepsList
      .sortedBy { step -> step.stepIndex }
  }

}
