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

package org.wfanet.measurement.dataprovider.deploy.fake

import com.google.protobuf.ByteString
import java.time.Clock
import java.time.Duration
import kotlin.properties.Delegates
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import org.wfanet.anysketch.crypto.EncryptSketchRequest.DestroyedRegisterStrategy.FLAGGED_KEY
import org.wfanet.measurement.api.v2alpha.CombinedPublicKeysGrpcKt.CombinedPublicKeysCoroutineStub
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.Sketch
import org.wfanet.measurement.api.v2alpha.SketchConfig
import org.wfanet.measurement.api.v2alpha.SketchConfigsGrpcKt.SketchConfigsCoroutineStub
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.buildChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.dataprovider.common.DefaultEncryptedSketchGenerator
import org.wfanet.measurement.dataprovider.common.GrpcElGamalPublicKeyCache
import org.wfanet.measurement.dataprovider.common.GrpcRequisitionFulfiller
import org.wfanet.measurement.dataprovider.common.GrpcSketchConfigCache
import org.wfanet.measurement.dataprovider.common.GrpcUnfulfilledRequisitionProvider
import org.wfanet.measurement.dataprovider.common.JniSketchEncrypter
import org.wfanet.measurement.dataprovider.daemon.RequisitionFulfillmentWorkflow
import org.wfanet.measurement.dataprovider.daemon.loopOnReadySuppressingExceptions
import org.wfanet.measurement.dataprovider.fake.FakeRequisitionDecoder
import picocli.CommandLine

class Flags {
  @CommandLine.Option(
    names = ["--external-data-provider-id"],
    required = true
  )
  lateinit var externalDataProviderId: String
    private set

  @CommandLine.Option(
    names = ["--combined-public-keys-service-target"],
    required = true
  )
  lateinit var combinedPublicKeysServiceTarget: String
    private set

  val combinedPublicKeysStub by lazy {
    CombinedPublicKeysCoroutineStub(
      buildChannel(combinedPublicKeysServiceTarget)
        .withVerboseLogging(debugVerboseGrpcClientLogging)
    )
  }

  @CommandLine.Option(
    names = ["--requisitions-service-target"],
    required = true
  )
  lateinit var requisitionsServiceTarget: String
    private set

  val requisitionsStub by lazy {
    RequisitionsCoroutineStub(
      buildChannel(requisitionsServiceTarget)
        .withVerboseLogging(debugVerboseGrpcClientLogging)
    )
  }

  @CommandLine.Option(
    names = ["--requisition-fulfillment-service-target"],
    required = true
  )
  lateinit var requisitionFulfillmentServiceTarget: String
    private set

  val requisitionFulfillmentStub by lazy {
    RequisitionFulfillmentCoroutineStub(
      buildChannel(requisitionFulfillmentServiceTarget)
        .withVerboseLogging(debugVerboseGrpcClientLogging)
    )
  }

  @CommandLine.Option(
    names = ["--sketch-configs-service-target"],
    required = true
  )
  lateinit var sketchConfigsServiceTarget: String
    private set

  val sketchConfigsStub by lazy {
    SketchConfigsCoroutineStub(
      buildChannel(sketchConfigsServiceTarget)
        .withVerboseLogging(debugVerboseGrpcClientLogging)
    )
  }

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
}

object EmptySketchGenerator {
  private val sketchEncrypter =
    JniSketchEncrypter(maximumValue = 10, destroyedRegisterStrategy = FLAGGED_KEY)

  @Suppress("UNUSED_PARAMETER")
  fun generate(
    requisitionSpec: RequisitionSpec,
    encryptionKey: ElGamalPublicKey,
    sketchConfig: SketchConfig
  ): Flow<ByteString> {
    // TODO: replace this with a more interesting sketch generator, configurable from flags.
    val sketch = Sketch.newBuilder().apply {
      config = sketchConfig
    }.build()

    return sketchEncrypter.encrypt(sketch, encryptionKey).asBufferedFlow(1024)
  }
}

@CommandLine.Command(
  name = "fake_requisition_fulfillment_daemon",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(@CommandLine.Mixin flags: Flags) {
  val throttler = MinimumIntervalThrottler(Clock.systemUTC(), flags.throttlerMinimumInterval)

  val workflow = RequisitionFulfillmentWorkflow(
    GrpcUnfulfilledRequisitionProvider(
      ApiId(flags.externalDataProviderId).externalId,
      flags.requisitionsStub
    ),
    FakeRequisitionDecoder(),
    DefaultEncryptedSketchGenerator(
      GrpcElGamalPublicKeyCache(flags.combinedPublicKeysStub),
      GrpcSketchConfigCache(flags.sketchConfigsStub),
      EmptySketchGenerator::generate
    ),
    GrpcRequisitionFulfiller(flags.requisitionFulfillmentStub)
  )

  runBlocking {
    throttler.loopOnReadySuppressingExceptions {
      workflow.execute()
    }
  }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
