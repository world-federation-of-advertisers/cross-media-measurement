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
import java.io.File
import java.time.Clock
import java.time.Duration
import java.util.logging.Logger
import kotlin.properties.Delegates
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.consent.crypto.keystore.testing.InMemoryKeyStore
import org.wfanet.measurement.loadtest.KingdomPublicApiFlags
import org.wfanet.measurement.loadtest.RequisitionFulfillmentServiceFlags
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

/** [EdpSimulator] runs the [RequisitionFulfillmentWorkflow] that does the actual work */
abstract class EdpSimulator : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: Flags
    private set

  protected fun run(storageClient: StorageClient) {
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), flags.throttlerMinimumInterval)

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

    val inMemoryKeyStore = InMemoryKeyStore()

    val edp =
      EdpData(
        flags.dataProviderResourceName,
        ENCRYPTION_PRIVATE_KEY_HANDLE_KEY,
        CONSENT_SIGNALING_PRIVATE_KEY_HANDLE_KEY,
        flags.edpCsCertificateDerFile.readBytes().toByteString()
      )

    val workflow =
      RequisitionFulfillmentWorkflow(
        edp,
        requisitionsStub,
        requisitionFulfillmentStub,
        SketchStore(storageClient),
        inMemoryKeyStore,
        certificatesStub,
        SketchGenerationParams(reach = flags.edpSketchReach, universeSize = flags.edpUniverseSize)
      )

    runBlocking {
      inMemoryKeyStore.storePrivateKeyDer(
        ENCRYPTION_PRIVATE_KEY_HANDLE_KEY,
        flags.edpEncryptionPrivateKeyDerFile.readBytes().toByteString()
      )
      inMemoryKeyStore.storePrivateKeyDer(
        CONSENT_SIGNALING_PRIVATE_KEY_HANDLE_KEY,
        flags.edpCsPrivateKeyDerFile.readBytes().toByteString()
      )

      val eventGroup =
        eventGroupsStub.createEventGroup(
          createEventGroupRequest {
            parent = flags.dataProviderResourceName
            eventGroup = eventGroup { measurementConsumer = flags.mcResourceName }
          }
        )
      logger.info("Successfully created eventGroup ${eventGroup.name}...")

      throttler.loopOnReady { workflow.execute() }
    }
  }

  protected class Flags {

    @CommandLine.Mixin
    lateinit var tlsFlags: TlsFlags
      private set

    @CommandLine.Option(
      names = ["--data-provider-resource-name"],
      description = ["The public API resource name of this data provider."],
      required = true
    )
    lateinit var dataProviderResourceName: String
      private set

    @CommandLine.Option(
      names = ["--data-provider-encryption-private-key-der-file"],
      description = ["The EDP's Encryption private key (DER format) file."],
      required = true
    )
    lateinit var edpEncryptionPrivateKeyDerFile: File
      private set

    @CommandLine.Option(
      names = ["--data-provider-consent-signaling-private-key-der-file"],
      description = ["The EDP's consent signaling private key (DER format) file."],
      required = true
    )
    lateinit var edpCsPrivateKeyDerFile: File
      private set

    @CommandLine.Option(
      names = ["--data-provider-consent-signaling-certificate-der-file"],
      description = ["The EDP's consent signaling private key (DER format) file."],
      required = true
    )
    lateinit var edpCsCertificateDerFile: File
      private set

    @CommandLine.Option(
      names = ["--mc-resource-name"],
      description = ["The public API resource name of the Measurement Consumer."],
      required = true
    )
    lateinit var mcResourceName: String
      private set

    @CommandLine.Option(
      names = ["--throttler-minimum-interval"],
      description = ["Minimum throttle interval"],
      defaultValue = "2s"
    )
    lateinit var throttlerMinimumInterval: Duration
      private set

    @set:CommandLine.Option(
      names = ["--edp-sketch-reach"],
      description = ["The reach for sketches generated by this EDP Simulator"],
      defaultValue = "10000"
    )
    var edpSketchReach by Delegates.notNull<Int>()
      private set

    @set:CommandLine.Option(
      names = ["--edp-sketch-universe-size"],
      description = ["The size of the universe for sketches generated by this EDP Simulator"],
      defaultValue = "10000"
    )
    var edpUniverseSize by Delegates.notNull<Int>()
      private set

    @CommandLine.Mixin
    lateinit var kingdomPublicApiFlags: KingdomPublicApiFlags
      private set

    @CommandLine.Mixin
    lateinit var requisitionFulfillmentServiceFlags: RequisitionFulfillmentServiceFlags
      private set
  }

  companion object {
    const val DAEMON_NAME = "EdpSimulator"
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
