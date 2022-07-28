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

import io.grpc.Channel
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.InMemoryBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketFilter
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManager
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.TestPrivacyBucketMapper
import org.wfanet.measurement.loadtest.dataprovider.CsvEventQuery
import org.wfanet.measurement.loadtest.dataprovider.EdpData
import org.wfanet.measurement.loadtest.dataprovider.EdpSimulator
import org.wfanet.measurement.loadtest.storage.SketchStore
import org.wfanet.measurement.storage.StorageClient

/** An in process EDP simulator. */
class InProcessEdpSimulator(
  val displayName: String,
  private val storageClient: StorageClient,
  kingdomPublicApiChannel: Channel,
  duchyPublicApiChannel: Channel,
  private val eventTemplateNames: List<String>
) {
  private val backgroundScope = CoroutineScope(Dispatchers.Default)

  private val eventGroupsClient by lazy { EventGroupsCoroutineStub(kingdomPublicApiChannel) }
  private val certificatesClient by lazy { CertificatesCoroutineStub(kingdomPublicApiChannel) }
  private val requisitionsClient by lazy { RequisitionsCoroutineStub(kingdomPublicApiChannel) }
  private val requisitionFulfillmentClient by lazy {
    RequisitionFulfillmentCoroutineStub(duchyPublicApiChannel).withPrincipalName(edpName)
  }

  private lateinit var edpJob: Job
  private lateinit var edpName: String

  fun start(edpName: String, mcName: String) {
    this.edpName = edpName
    edpJob =
      backgroundScope.launch {
        val edpData = createEdpData(displayName, edpName)

        EdpSimulator(
            edpData = edpData,
            measurementConsumerName = mcName,
            certificatesStub = certificatesClient.withPrincipalName(edpData.name),
            eventGroupsStub = eventGroupsClient.withPrincipalName(edpData.name),
            requisitionsStub = requisitionsClient.withPrincipalName(edpData.name),
            requisitionFulfillmentStub = requisitionFulfillmentClient,
            sketchStore = SketchStore(storageClient),
            // eventQuery =
            // RandomEventQuery(SketchGenerationParams(reach = 1000, universeSize = 10_000)),
            eventQuery = CsvEventQuery(),
            throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
            eventTemplateNames = eventTemplateNames,
            PrivacyBudgetManager(
              PrivacyBucketFilter(TestPrivacyBucketMapper()),
              InMemoryBackingStore(),
              100.0f,
              100.0f
            )
          )
          .process()
      }
  }

  suspend fun stop() {
    edpJob.cancelAndJoin()
  }

  /** Builds a [EdpData] object for the Edp with a certain [displayName] and [resourceName]. */
  private fun createEdpData(displayName: String, resourceName: String) =
    EdpData(
      name = resourceName,
      displayName = displayName,
      encryptionKey = loadEncryptionPrivateKey("${displayName}_enc_private.tink"),
      signingKey = loadSigningKey("${displayName}_cs_cert.der", "${displayName}_cs_private.der")
    )
}
