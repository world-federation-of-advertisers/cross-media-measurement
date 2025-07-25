// Copyright 2021 The Cross-Media Measurement Authors
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

import com.google.protobuf.ByteString
import io.grpc.Channel
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import org.jetbrains.annotations.Blocking
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.Health
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.dataprovider.DataProviderData
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.InMemoryBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketFilter
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManager
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.TestPrivacyBucketMapper
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.InMemoryVidIndexMap
import org.wfanet.measurement.loadtest.dataprovider.EdpSimulator
import org.wfanet.measurement.loadtest.dataprovider.SyntheticGeneratorEventQuery
import org.wfanet.measurement.loadtest.dataprovider.toPopulationSpec

/** An in process EDP simulator. */
class InProcessEdpSimulator(
  val displayName: String,
  resourceName: String,
  private val certificateKey: DataProviderCertificateKey,
  mcResourceName: String,
  kingdomPublicApiChannel: Channel,
  duchyPublicApiChannelMap: Map<String, Channel>,
  trustedCertificates: Map<ByteString, X509Certificate>,
  eventQuery: SyntheticGeneratorEventQuery,
  coroutineContext: CoroutineContext = Dispatchers.Default,
  honestMajorityShareShuffleSupported: Boolean = true,
) : Health {
  private val loggingName = "${javaClass.simpleName} $displayName"
  private val backgroundScope =
    CoroutineScope(
      coroutineContext +
        CoroutineName(loggingName) +
        CoroutineExceptionHandler { _, e ->
          logger.log(Level.SEVERE, e) { "Error in $loggingName" }
        }
    )

  private val delegate: EdpSimulator

  init {
    val populationSpec: PopulationSpec = eventQuery.populationSpec.toPopulationSpec()
    val hmssVidIndexMap =
      if (honestMajorityShareShuffleSupported) {
        InMemoryVidIndexMap.build(populationSpec)
      } else {
        null
      }

    delegate =
      EdpSimulator(
        edpData = createEdpData(displayName, resourceName),
        measurementConsumerName = mcResourceName,
        measurementConsumersStub =
          MeasurementConsumersCoroutineStub(kingdomPublicApiChannel)
            .withPrincipalName(resourceName),
        certificatesStub =
          CertificatesCoroutineStub(kingdomPublicApiChannel).withPrincipalName(resourceName),
        dataProvidersStub =
          DataProvidersCoroutineStub(kingdomPublicApiChannel).withPrincipalName(resourceName),
        eventGroupsStub =
          EventGroupsCoroutineStub(kingdomPublicApiChannel).withPrincipalName(resourceName),
        eventGroupMetadataDescriptorsStub =
          EventGroupMetadataDescriptorsCoroutineStub(kingdomPublicApiChannel)
            .withPrincipalName(resourceName),
        requisitionsStub =
          RequisitionsCoroutineStub(kingdomPublicApiChannel).withPrincipalName(resourceName),
        requisitionFulfillmentStubsByDuchyId =
          duchyPublicApiChannelMap.mapValues {
            RequisitionFulfillmentCoroutineStub(it.value).withPrincipalName(resourceName)
          },
        eventQuery = eventQuery,
        throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        privacyBudgetManager =
          PrivacyBudgetManager(
            PrivacyBucketFilter(TestPrivacyBucketMapper()),
            InMemoryBackingStore(),
            10.0f,
            100.0f,
          ),
        trustedCertificates = trustedCertificates,
        hmssVidIndexMap = hmssVidIndexMap,
        knownEventGroupMetadataTypes = listOf(EventGroupMetadata.getDescriptor().file),
        random = random,
      )
  }

  private lateinit var edpJob: Job

  override val healthy: Boolean
    get() = delegate.healthy

  override suspend fun waitUntilHealthy() = delegate.waitUntilHealthy()

  fun start() {
    edpJob = backgroundScope.launch { delegate.run() }
  }

  suspend fun stop() {
    edpJob.cancelAndJoin()
    backgroundScope.cancel()
  }

  suspend fun ensureEventGroup() =
    delegate.ensureEventGroup(
      EVENT_TEMPLATES,
      eventGroupMetadata {
        adMetadata =
          EventGroupMetadataKt.adMetadata {
            campaignMetadata =
              EventGroupMetadataKt.AdMetadataKt.campaignMetadata {
                campaignName = "$displayName campaign"
                brandName = "Brand"
              }
          }
      },
    )

  /**
   * Builds a [DataProviderData] object for the Edp with a certain [displayName] and [resourceName].
   */
  @Blocking
  private fun createEdpData(displayName: String, resourceName: String) =
    DataProviderData(
      name = resourceName,
      displayName = displayName,
      certificateKey = certificateKey,
      privateEncryptionKey = loadEncryptionPrivateKey("${displayName}_enc_private.tink"),
      signingKeyHandle =
        loadSigningKey("${displayName}_cs_cert.der", "${displayName}_cs_private.der"),
    )

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val RANDOM_SEED: Long = 1
    private val random = Random(RANDOM_SEED)

    private val EVENT_TEMPLATES = EdpSimulator.buildEventTemplates(TestEvent.getDescriptor())
  }
}
