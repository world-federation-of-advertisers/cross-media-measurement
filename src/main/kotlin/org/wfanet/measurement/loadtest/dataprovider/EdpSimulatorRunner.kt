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

package org.wfanet.measurement.loadtest.dataprovider

import io.grpc.ManagedChannel
import java.time.Clock
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.consent.crypto.keystore.testing.InMemoryKeyStore
import org.wfanet.measurement.loadtest.storage.SketchStore
import org.wfanet.measurement.storage.StorageClient
import picocli.CommandLine

/* Key handle of the EDP's private key */
const val CONSENT_SIGNALING_PRIVATE_KEY_HANDLE_KEY = "edp-consent-signaling-private-key"
const val ENCRYPTION_PRIVATE_KEY_HANDLE_KEY = "edp-encryption-private-key"

data class SketchGenerationParams(
  val reach: Int,
  val universeSize: Int,
)

/** The base class of the EdpSimulator runner. */
abstract class EdpSimulatorRunner : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: EdpSimulatorFlags
    private set

  protected fun run(storageClient: StorageClient) {

    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = flags.tlsFlags.certFile,
        privateKeyFile = flags.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = flags.tlsFlags.certCollectionFile
      )

    val v2alphaPublicApiChannel: ManagedChannel =
      buildMutualTlsChannel(
        flags.kingdomPublicApiFlags.target,
        clientCerts,
        flags.kingdomPublicApiFlags.certHost
      )
    val requisitionsStub = RequisitionsCoroutineStub(v2alphaPublicApiChannel)
    val eventGroupsStub = EventGroupsCoroutineStub(v2alphaPublicApiChannel)
    val certificatesStub = CertificatesCoroutineStub(v2alphaPublicApiChannel)

    val requisitionFulfillmentStub =
      RequisitionFulfillmentCoroutineStub(
        buildMutualTlsChannel(
          flags.requisitionFulfillmentServiceFlags.target,
          clientCerts,
          flags.requisitionFulfillmentServiceFlags.certHost,
        )
      )

    runBlocking {
      val inMemoryKeyStore = InMemoryKeyStore()
      inMemoryKeyStore.storePrivateKeyDer(
        ENCRYPTION_PRIVATE_KEY_HANDLE_KEY,
        flags.edpEncryptionPrivateKeyDerFile.readBytes().toByteString()
      )
      inMemoryKeyStore.storePrivateKeyDer(
        CONSENT_SIGNALING_PRIVATE_KEY_HANDLE_KEY,
        flags.edpCsPrivateKeyDerFile.readBytes().toByteString()
      )

      val edpData =
        EdpData(
          flags.dataProviderResourceName,
          ENCRYPTION_PRIVATE_KEY_HANDLE_KEY,
          CONSENT_SIGNALING_PRIVATE_KEY_HANDLE_KEY,
          flags.edpCsCertificateDerFile.readBytes().toByteString()
        )
      EdpSimulator(
          edpData,
          flags.mcResourceName,
          certificatesStub,
          eventGroupsStub,
          requisitionsStub,
          requisitionFulfillmentStub,
          SketchStore(storageClient),
          inMemoryKeyStore,
          SketchGenerationParams(
            reach = flags.edpSketchReach,
            universeSize = flags.edpUniverseSize
          ),
          MinimumIntervalThrottler(Clock.systemUTC(), flags.throttlerMinimumInterval)
        )
        .process()
    }
  }
}
