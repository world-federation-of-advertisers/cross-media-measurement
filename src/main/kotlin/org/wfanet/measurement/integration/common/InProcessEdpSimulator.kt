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
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.InMemoryBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketFilter
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManager
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.TestPrivacyBucketMapper
import org.wfanet.measurement.loadtest.dataprovider.EdpData
import org.wfanet.measurement.loadtest.dataprovider.EdpSimulator
import org.wfanet.measurement.loadtest.dataprovider.RandomEventQuery
import org.wfanet.measurement.loadtest.dataprovider.SketchGenerationParams
import org.wfanet.measurement.loadtest.storage.SketchStore
import org.wfanet.measurement.storage.StorageClient

/** An in process EDP simulator. */
class InProcessEdpSimulator(
  val displayName: String,
  resourceName: String,
  mcResourceName: String,
  storageClient: StorageClient,
  kingdomPublicApiChannel: Channel,
  duchyPublicApiChannel: Channel,
  eventTemplateNames: List<String>,
  trustedCertificates: Map<ByteString, X509Certificate>,
  coroutineContext: CoroutineContext = Dispatchers.Default,
) {
  private val loggingName = "${javaClass.simpleName} $displayName"
  private val backgroundScope =
    CoroutineScope(
      coroutineContext +
        CoroutineName(loggingName) +
        CoroutineExceptionHandler { _, e ->
          logger.log(Level.SEVERE, e) { "Error in $loggingName" }
        }
    )

  private val delegate =
    EdpSimulator(
      edpData = createEdpData(displayName, resourceName),
      measurementConsumerName = mcResourceName,
      measurementConsumersStub =
        MeasurementConsumersCoroutineStub(kingdomPublicApiChannel).withPrincipalName(resourceName),
      certificatesStub =
        CertificatesCoroutineStub(kingdomPublicApiChannel).withPrincipalName(resourceName),
      eventGroupsStub =
        EventGroupsCoroutineStub(kingdomPublicApiChannel).withPrincipalName(resourceName),
      eventGroupMetadataDescriptorsStub =
        EventGroupMetadataDescriptorsCoroutineStub(kingdomPublicApiChannel)
          .withPrincipalName(resourceName),
      requisitionsStub =
        RequisitionsCoroutineStub(kingdomPublicApiChannel).withPrincipalName(resourceName),
      requisitionFulfillmentStub =
        RequisitionFulfillmentCoroutineStub(duchyPublicApiChannel).withPrincipalName(resourceName),
      sketchStore = SketchStore(storageClient),
      eventQuery = RandomEventQuery(SketchGenerationParams(reach = 1000, universeSize = 10_000)),
      throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
      eventTemplateNames = eventTemplateNames,
      privacyBudgetManager =
        PrivacyBudgetManager(
          PrivacyBucketFilter(TestPrivacyBucketMapper()),
          InMemoryBackingStore(),
          100.0f,
          100.0f
        ),
      trustedCertificates = trustedCertificates
    )

  private lateinit var edpJob: Job

  fun start() {
    edpJob = backgroundScope.launch { delegate.run() }
  }

  suspend fun stop() {
    edpJob.cancelAndJoin()
  }

  suspend fun createEventGroup() = delegate.createEventGroup()

  /** Builds a [EdpData] object for the Edp with a certain [displayName] and [resourceName]. */
  private fun createEdpData(displayName: String, resourceName: String) =
    EdpData(
      name = resourceName,
      displayName = displayName,
      encryptionKey = loadEncryptionPrivateKey("${displayName}_enc_private.tink"),
      signingKey = loadSigningKey("${displayName}_cs_cert.der", "${displayName}_cs_private.der")
    )

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
