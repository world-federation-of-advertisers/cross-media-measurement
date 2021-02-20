// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.util.logging.Logger
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.RequisitionGrpcKt.RequisitionCoroutineStub
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.testing.CloseableResource
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.launchAsAutoCloseable
import org.wfanet.measurement.common.testing.pollFor
import org.wfanet.measurement.kingdom.db.KingdomRelationalDatabase
import org.wfanet.measurement.system.v1alpha.ConfirmGlobalComputationRequest
import org.wfanet.measurement.system.v1alpha.FinishGlobalComputationRequest
import org.wfanet.measurement.system.v1alpha.FulfillMetricRequisitionRequest
import org.wfanet.measurement.system.v1alpha.GetGlobalComputationRequest
import org.wfanet.measurement.system.v1alpha.GlobalComputation
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.MetricRequisitionKey
import org.wfanet.measurement.system.v1alpha.RequisitionGrpcKt.RequisitionCoroutineStub as SystemRequisitionCoroutineStub
import org.wfanet.measurement.system.v1alpha.StreamActiveGlobalComputationsRequest

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of KingdomRelationalDatabase can all run the
 * same tests easily.
 */
abstract class InProcessKingdomIntegrationTestBase {
  /** Provides a [KingdomRelationalDatabase] to the test. */
  abstract val kingdomRelationalDatabaseRule: ProviderRule<KingdomRelationalDatabase>

  private val kingdomRelationalDatabase: KingdomRelationalDatabase
    get() = kingdomRelationalDatabaseRule.value

  private var duchyId: String = "some-duchy"

  private val kingdom = InProcessKingdom { kingdomRelationalDatabase }

  private val globalComputations = mutableListOf<GlobalComputation>()
  private val globalComputationsMutex = Mutex()
  private val globalComputationsReader = CloseableResource {
    GlobalScope.launchAsAutoCloseable {
      var continuationToken = ""
      while (true) {
        val request =
          StreamActiveGlobalComputationsRequest.newBuilder()
            .setContinuationToken(continuationToken)
            .build()
        logger.info("Reading global computations: $request")
        globalComputationsStub
          .streamActiveGlobalComputations(request)
          .onEach { continuationToken = it.continuationToken }
          .map { it.globalComputation }
          .onEach { logger.info("Found GlobalComputation: $it") }
          .collect {
            globalComputationsMutex.withLock {
              globalComputations.add(it)
            }
          }
      }
    }
  }

  @get:Rule
  val ruleChain: TestRule by lazy {
    chainRulesSequentially(
      DuchyIdSetter(duchyId),
      kingdomRelationalDatabaseRule,
      kingdom,
      globalComputationsReader
    )
  }

  private val requisitionsStub by lazy {
    RequisitionCoroutineStub(kingdom.publicApiChannel).withDuchyId(duchyId)
  }

  private val systemRequisitionsStub by lazy {
    SystemRequisitionCoroutineStub(kingdom.publicApiChannel).withDuchyId(duchyId)
  }

  private val globalComputationsStub by lazy {
    GlobalComputationsCoroutineStub(kingdom.publicApiChannel).withDuchyId(duchyId)
  }

  @Test
  fun `entire computation`() = runBlocking {
    val (dataProviders, campaigns) = kingdom.populateKingdomRelationalDatabase()
    val (externalDataProviderId1, externalDataProviderId2) = dataProviders
    val (externalCampaignId1, externalCampaignId2, externalCampaignId3) = campaigns
    logger.info("Database is populated")

    // At this point, the ReportMaker daemon should pick up pick up on the ReportConfigSchedule and
    // create a Report.
    //
    // Next, the RequisitionLinker daemon should create two Requisitions for the Report.

    val requisition1 = pollFor {
      readRequisition(externalDataProviderId1, externalCampaignId1).firstOrNull()
    }
    logger.info("Found first requisition: $requisition1")

    val requisition2 = pollFor {
      readRequisition(externalDataProviderId2, externalCampaignId2).firstOrNull()
    }
    logger.info("Found second requisition: $requisition2")

    val requisition3 = pollFor {
      readRequisition(externalDataProviderId2, externalCampaignId3).firstOrNull()
    }
    logger.info("Found third requisition: $requisition3")

    val requisitions = listOf(requisition1, requisition2, requisition3)
    requisitions.forEach { fulfillRequisition(it) }

    val expectedMetricRequisition1 = MetricRequisition.newBuilder().apply {
      keyBuilder.apply {
        dataProviderId = externalDataProviderId1.apiId.value
        campaignId = externalCampaignId1.apiId.value
      }
    }.build()

    val expectedMetricRequisition2 = MetricRequisition.newBuilder().apply {
      keyBuilder.apply {
        dataProviderId = externalDataProviderId2.apiId.value
        campaignId = externalCampaignId2.apiId.value
      }
    }.build()

    val expectedMetricRequisition3 = MetricRequisition.newBuilder().apply {
      keyBuilder.apply {
        dataProviderId = externalDataProviderId2.apiId.value
        campaignId = externalCampaignId3.apiId.value
      }
    }.build()

    assertThat(requisitions)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        expectedMetricRequisition1,
        expectedMetricRequisition2,
        expectedMetricRequisition3
      )

    // When the Report is first created, it will be in state AWAITING_REQUISITIONS.
    // After the RequisitionLinker is done, the ReportStarter daemon will transition it to state
    // AWAITING_DUCHY_CONFIRMATION.
    //
    // These states are exposed in GlobalComputation as CREATED and CONFIRMING.
    logger.info("Awaiting a GlobalComputation in CONFIRMING state")
    val computation = findLastComputationInState(GlobalComputation.State.CONFIRMING)

    logger.info("Confirming Duchy readiness")
    globalComputationsStub.confirmGlobalComputation(
      ConfirmGlobalComputationRequest.newBuilder().apply {
        key = computation.key
        addAllReadyRequisitions(requisitions.map { it.key.toSystemKey() })
      }.build()
    )

    logger.info("Awaiting a GlobalComputation in RUNNING state")
    val startedComputation = findLastComputationInState(GlobalComputation.State.RUNNING)

    assertThat(startedComputation)
      .isEqualTo(computation.toBuilder().setState(GlobalComputation.State.RUNNING).build())

    logger.info("Finishing GlobalComputation")
    globalComputationsStub.finishGlobalComputation(
      FinishGlobalComputationRequest.newBuilder().apply {
        key = computation.key
        resultBuilder.apply {
          reach = 12345L
          putFrequency(6L, 0.4)
          putFrequency(8L, 0.6)
        }
      }.build()
    )

    val finalComputation = globalComputationsStub.getGlobalComputation(
      GetGlobalComputationRequest.newBuilder().setKey(computation.key).build()
    )
    assertThat(finalComputation)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        startedComputation.toBuilder().apply {
          state = GlobalComputation.State.SUCCEEDED
          resultBuilder.apply {
            reach = 12345L
            putFrequency(6L, 0.4)
            putFrequency(8L, 0.6)
          }
        }.build()
      )
  }

  private suspend fun findLastComputationInState(
    state: GlobalComputation.State
  ): GlobalComputation {
    return pollFor {
      globalComputationsMutex.withLock {
        globalComputations.findLast {
          it.state == state
        }
      }
    }
  }

  private suspend fun readRequisition(
    dataProviderId: ExternalId,
    campaignId: ExternalId
  ): List<MetricRequisition> {
    val request = ListMetricRequisitionsRequest.newBuilder().apply {
      parentBuilder.apply {
        this.dataProviderId = dataProviderId.apiId.value
        this.campaignId = campaignId.apiId.value
      }
      pageSize = 1
      filterBuilder.apply {
        addStates(MetricRequisition.State.UNFULFILLED)
        addStates(MetricRequisition.State.FULFILLED)
      }
    }.build()
    logger.info("Listing requisitions: $request")
    val response = requisitionsStub.listMetricRequisitions(request)
    logger.info("Got requisitions: $response")
    return response.metricRequisitionsList
  }

  private suspend fun fulfillRequisition(metricRequisition: MetricRequisition) {
    logger.info("Fulfilling requisition: $metricRequisition")
    systemRequisitionsStub.fulfillMetricRequisition(
      FulfillMetricRequisitionRequest.newBuilder().apply {
        key = metricRequisition.key.toSystemKey()
      }.build()
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

private fun MetricRequisition.Key.toSystemKey(): MetricRequisitionKey {
  return MetricRequisitionKey.newBuilder().also {
    it.dataProviderId = dataProviderId
    it.campaignId = campaignId
    it.metricRequisitionId = metricRequisitionId
  }.build()
}
