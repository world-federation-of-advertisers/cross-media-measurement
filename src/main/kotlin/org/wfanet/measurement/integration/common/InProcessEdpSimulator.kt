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
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.consent.crypto.keystore.KeyStore
import org.wfanet.measurement.loadtest.dataprovider.CONSENT_SIGNALING_PRIVATE_KEY_HANDLE_KEY
import org.wfanet.measurement.loadtest.dataprovider.ENCRYPTION_PRIVATE_KEY_HANDLE_KEY
import org.wfanet.measurement.loadtest.dataprovider.EdpData
import org.wfanet.measurement.loadtest.dataprovider.EdpSimulator
import org.wfanet.measurement.loadtest.dataprovider.RandomEventQuery
import org.wfanet.measurement.loadtest.dataprovider.SketchGenerationParams
import org.wfanet.measurement.loadtest.storage.SketchStore
import org.wfanet.measurement.storage.StorageClient

/** An in process EDP simulator. */
class InProcessEdpSimulator(
  val displayName: String,
  private val keyStore: KeyStore,
  private val storageClient: StorageClient,
  kingdomPublicApiChannel: Channel,
  duchyPublicApiChannel: Channel,
) {

  private val backgroundScope = CoroutineScope(Dispatchers.Default)

  private val eventGroupsClient by lazy { EventGroupsCoroutineStub(kingdomPublicApiChannel) }
  private val certificatesClient by lazy { CertificatesCoroutineStub(kingdomPublicApiChannel) }
  private val requisitionsClient by lazy { RequisitionsCoroutineStub(kingdomPublicApiChannel) }
  private val requisitionFulfillmentClient by lazy {
    RequisitionFulfillmentCoroutineStub(duchyPublicApiChannel)
  }

  private lateinit var edpJob: Job

  fun start(edpName: String, mcName: String) {
    edpJob =
      backgroundScope.launch {
        val edpData = createEdpData(displayName, edpName)
        keyStore.storePrivateKeyDer(
          edpData.encryptionPrivateKeyId,
          loadTestCertDerFile("${displayName}_enc_private.der")
        )
        keyStore.storePrivateKeyDer(
          edpData.consentSignalingPrivateKeyId,
          loadTestCertDerFile("${displayName}_cs_private.der")
        )

        EdpSimulator(
            edpData = edpData,
            measurementConsumerName = mcName,
            certificatesStub = certificatesClient,
            eventGroupsStub = eventGroupsClient,
            requisitionsStub = requisitionsClient,
            requisitionFulfillmentStub = requisitionFulfillmentClient,
            sketchStore = SketchStore(storageClient),
            keyStore = keyStore,
            eventQuery =
              RandomEventQuery(SketchGenerationParams(reach = 1000, universeSize = 10_000)),
            throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
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
      encryptionPrivateKeyId = ENCRYPTION_PRIVATE_KEY_HANDLE_KEY,
      consentSignalingPrivateKeyId = CONSENT_SIGNALING_PRIVATE_KEY_HANDLE_KEY,
      consentSignalCertificateDer = loadTestCertDerFile("${displayName}_cs_cert.der")
    )
}
