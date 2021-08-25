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

import java.time.Clock
import java.time.Duration
import kotlin.properties.Delegates
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.config.PublicApiProtocolConfigs
import org.wfanet.measurement.loadtest.KingdomPublicApiFlags
import org.wfanet.measurement.loadtest.RequisitionFulfillmentServiceFlags
import org.wfanet.measurement.loadtest.storage.SketchStore
import picocli.CommandLine

/** [EdpSimulator] runs the [RequisitionFulfillmentWorkflow] that does the actual work */
abstract class EdpSimulator : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: Flags
    private set

  abstract val storageClient: SketchStore

  override fun run() {
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), flags.throttlerMinimumInterval)

    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = flags.tlsFlags.certFile,
        privateKeyFile = flags.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = flags.tlsFlags.certCollectionFile
      )

    val requisitionsStub =
      RequisitionsCoroutineStub(
        buildMutualTlsChannel(
            flags.kingdomPublicApiFlags.target,
            clientCerts,
            flags.kingdomPublicApiFlags.certHost
          )
          .withVerboseLogging(flags.debugVerboseGrpcClientLogging)
      )

    val requisitionFulfillmentStub =
      RequisitionFulfillmentCoroutineStub(
        buildMutualTlsChannel(
            flags.requisitionFulfillmentServiceFlags.target,
            clientCerts,
            flags.requisitionFulfillmentServiceFlags.certHost,
          )
          .withVerboseLogging(flags.debugVerboseGrpcClientLogging)
      )

    val eventGroupStub =
      EventGroupsCoroutineStub(
        buildMutualTlsChannel(
            flags.requisitionFulfillmentServiceFlags.target,
            clientCerts,
            flags.requisitionFulfillmentServiceFlags.certHost,
          )
          .withVerboseLogging(flags.debugVerboseGrpcClientLogging)
      )

    var request = createEventGroupRequest {
      parent = flags.dataProviderResourceName
      eventGroup = eventGroup { measurementConsumer = flags.measurementConsumerResourceName }
    }

    val workflow =
      RequisitionFulfillmentWorkflow(
        flags.dataProviderResourceName,
        flags
          .publicApiProtocolConfigs
          .reader()
          .use { parseTextProto(it, PublicApiProtocolConfigs.getDefaultInstance()) }
          .configsMap,
        requisitionsStub,
        requisitionFulfillmentStub,
        storageClient,
      )

    runBlocking {
      eventGroupStub.createEventGroup(request)

      throttler.loopOnReady { workflow.execute() }
    }
  }

  protected class Flags {

    @CommandLine.Mixin
    lateinit var tlsFlags: TlsFlags
      private set

    @CommandLine.Option(
      names = ["--public-api-protocol-configs"],
      description = ["PublicApiProtocolConfigs proto message in text format."],
      required = true
    )
    lateinit var publicApiProtocolConfigs: String
      private set

    @CommandLine.Option(
      names = ["--data-provider-resource-name"],
      description = ["The public API resource name of this data provider."],
      required = true
    )
    lateinit var dataProviderResourceName: String
      private set

    @CommandLine.Option(
      names = ["--measurement-consumer-resource-name"],
      description = ["The public API resource name of the Measurement Consumer."],
      required = true
    )
    lateinit var measurementConsumerResourceName: String
      private set

    @CommandLine.Option(
      names = ["--throttler-minimum-interval"],
      description = ["Minimum throttle interval"],
      defaultValue = "1s"
    )
    lateinit var throttlerMinimumInterval: Duration
      private set

    @CommandLine.Mixin
    lateinit var server: CommonServer.Flags
      private set

    @CommandLine.Mixin
    lateinit var kingdomPublicApiFlags: KingdomPublicApiFlags
      private set

    @CommandLine.Mixin
    lateinit var requisitionFulfillmentServiceFlags: RequisitionFulfillmentServiceFlags
      private set

    var debugVerboseGrpcClientLogging by Delegates.notNull<Boolean>()
      private set
  }

  companion object {
    const val DAEMON_NAME = "EdpSimulator"
  }
}
