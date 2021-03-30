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

package org.wfanet.measurement.dataprovider.deploy.common

import java.io.File
import java.time.Clock
import java.time.Duration
import kotlin.properties.Delegates
import kotlinx.coroutines.runBlocking
import org.wfanet.anysketch.crypto.EncryptSketchRequest.DestroyedRegisterStrategy.FLAGGED_KEY
import org.wfanet.measurement.api.v1alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt.PublisherDataCoroutineStub
import org.wfanet.measurement.api.v1alpha.Sketch
import org.wfanet.measurement.api.v1alpha.SketchConfig
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.buildChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.dataprovider.common.JniSketchEncrypter
import org.wfanet.measurement.dataprovider.fake.FakeDataProvider
import picocli.CommandLine

class Flags {
  @CommandLine.Option(
    names = ["--external-data-provider-id"],
    required = true
  )
  lateinit var externalDataProviderId: String
    private set

  @CommandLine.Option(
    names = ["--publisher-data-service-target"],
    required = true
  )
  lateinit var internalServicesTarget: String
    private set

  @CommandLine.Option(
    names = ["--throttler-minimum-interval"],
    defaultValue = "1s"
  )
  lateinit var throttlerMinimumInterval: Duration
    private set

  @set:CommandLine.Option(
    names = ["--debug-verbose-grpc-client-logging"],
    description = ["Enables full gRPC request and response logging for outgoing gRPCs"],
    defaultValue = "false"
  )
  var debugVerboseGrpcClientLogging by Delegates.notNull<Boolean>()
    private set

  // TODO: once the SketchConfigs service exists, this is no longer needed.
  @CommandLine.Option(
    names = ["--sketch-config-file"],
    description = ["File path for SketchConfig proto message in textproto format."],
  )
  lateinit var sketchConfigFile: File
    private set
}

@CommandLine.Command(
  name = "fake_data_provider",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(@CommandLine.Mixin flags: Flags) {
  val channel =
    buildChannel(flags.internalServicesTarget)
      .withVerboseLogging(flags.debugVerboseGrpcClientLogging)
  val stub = PublisherDataCoroutineStub(channel)

  val throttler = MinimumIntervalThrottler(Clock.systemUTC(), flags.throttlerMinimumInterval)

  val sketchConfig = parseTextProto(flags.sketchConfigFile, SketchConfig.getDefaultInstance())

  // TODO: replace this with a more interesting sketch generator, configurable from flags.
  val emptySketchGenerator = { _: MetricRequisition ->
    Sketch.newBuilder().apply {
      config = sketchConfig
    }.build()
  }

  val sketchEncrypter =
    JniSketchEncrypter(maximumValue = 10, destroyedRegisterStrategy = FLAGGED_KEY)

  val sketchGenerator = { requisition: MetricRequisition, key: ElGamalPublicKey ->
    sketchEncrypter.encrypt(emptySketchGenerator(requisition), key)
  }

  val fakeDataProvider =
    FakeDataProvider(
      publisherDataStub = stub,
      externalDataProviderId = ApiId(flags.externalDataProviderId).externalId,
      throttler = throttler,
      generateSketch = sketchGenerator
    )

  runBlocking {
    fakeDataProvider.start()
  }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
