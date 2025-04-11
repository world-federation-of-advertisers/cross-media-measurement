/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common

import com.google.protobuf.ByteString
import io.grpc.Channel
import java.security.cert.X509Certificate
import java.util.Timer
import kotlin.concurrent.fixedRateTimer
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.EventGroup as ExternalEventGroup
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.eventgroups.EventGroupSync
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcher
import org.wfanet.measurement.gcloud.pubsub.GooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import org.wfanet.measurement.loadtest.resourcesetup.Resources
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.service.internal.Services
import org.wfanet.measurement.securecomputation.teesdk.testing.FakeFulfillingRequisitionTeeApp
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import java.time.Clock
import kotlinx.coroutines.flow.emptyFlow
import java.time.Duration
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup

class InProcessEdpAggregatorComponents(
  private val internalServicesRule: ProviderRule<Services>,
  private val pubSubClient: GooglePubSubClient,
  private val kingdomChannel: Channel,
  private val storageClient: StorageClient,
) : TestRule {

  private val internalServices: Services
    get() = internalServicesRule.value

  val secureComputationPublicApi =
    InProcessSecureComputationPublicApi(internalServicesProvider = { internalServices })

  private val workItemsClient: WorkItemsCoroutineStub by lazy {
    WorkItemsCoroutineStub(secureComputationPublicApi.publicApiChannel)
  }

  private val requisitionsClient: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(kingdomChannel)
  }

  private val eventGroupsClient: EventGroupsCoroutineStub by lazy {
    EventGroupsCoroutineStub(kingdomChannel)
  }

  private val dataWatcher: DataWatcher by lazy { DataWatcher(workItemsClient, DATA_WATCHER_CONFIG) }

  private val requisitionFetcher: RequisitionFetcher by lazy {
    RequisitionFetcher(
      requisitionsClient,
      storageClient,
      "some-data-prover",
      "some-storage-prefix",
      10,
    )
  }

  private val eventGroupSync: EventGroupSync by lazy {
    EventGroupSync(
      "some-edp-name",
      eventGroupsClient,
      emptyFlow<EventGroup>(),
      MinimumIntervalThrottler(
        Clock.systemUTC(),
        Duration.ofMillis(1000L),
      ),
    )
  }

  private val subscriber = Subscriber(PROJECT_ID, pubSubClient)

  private val teeApp =
    FakeFulfillingRequisitionTeeApp(
      parser = WorkItem.parser(),
      subscriptionId = "some-subscription",
      workItemsClient = workItemsClient,
      workItemAttemptsClient =
        WorkItemAttemptsCoroutineStub(secureComputationPublicApi.publicApiChannel),
      queueSubscriber = subscriber,
    )

  val ruleChain: TestRule by lazy {
    chainRulesSequentially(internalServicesRule, secureComputationPublicApi)
  }

  private lateinit var edpDisplayNameToResourceMap: Map<String, Resources.Resource>
  lateinit var externalEventGroups: List<ExternalEventGroup>

  private suspend fun createAllResources() {}

  fun getDataProviderResourceNames(): List<String> {
    return edpDisplayNameToResourceMap.values.map { it.name }
  }

  lateinit var requisitionFetcherTimer: Timer
  lateinit var eventGroupSyncTimer: Timer

  fun startDaemons() = runBlocking {
    // Create all resources
    createAllResources()
    requisitionFetcherTimer =
      fixedRateTimer("timer", false, 0L, 1000L) {
        runBlocking { requisitionFetcher.fetchAndStoreRequisitions() }
      }
    eventGroupSyncTimer =
      fixedRateTimer("timer", false, 0L, 1000L) {
        runBlocking { eventGroupSync.sync() }
      }
  }

  suspend fun stopDaemons() {
    pubSubClient.deleteTopic(PROJECT_ID, TOPIC_ID)
    // pubSubClient.deleteSubscription(PROJECT_ID, SUBSCRIPTION_ID)
    requisitionFetcherTimer.cancel()
    eventGroupSyncTimer.cancel()
  }

  override fun apply(statement: Statement, description: Description): Statement {
    return ruleChain.apply(statement, description)
  }

  companion object {
    val MC_ENTITY_CONTENT: EntityContent = createEntityContent(MC_DISPLAY_NAME)
    val MC_ENCRYPTION_PRIVATE_KEY: TinkPrivateKeyHandle =
      loadEncryptionPrivateKey("${MC_DISPLAY_NAME}_enc_private.tink")
    val TRUSTED_CERTIFICATES: Map<ByteString, X509Certificate> =
      loadTestCertCollection("all_root_certs.pem").associateBy {
        checkNotNull(it.subjectKeyIdentifier)
      }

    @JvmStatic fun initConfig() {}
  }
}
