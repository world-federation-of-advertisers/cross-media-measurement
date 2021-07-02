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

package org.wfanet.measurement.dataprovider.daemon

import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import picocli.CommandLine

abstract class DataProviderServer : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: Flags
    private set

  private fun run(@CommandLine.Mixin flags: Flags) {
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), flags.throttlerMinimumInterval)

    /*
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
    */

    runBlocking {
      throttler.loopOnReadySuppressingExceptions {
        //        workflow.execute()
      }
    }
  }

  protected class Flags {

    @CommandLine.Option(names = ["--throttler-minimum-interval"], defaultValue = "1s")
    lateinit var throttlerMinimumInterval: Duration
      private set

    @CommandLine.Mixin
    lateinit var server: CommonServer.Flags
      private set

    @CommandLine.Option(
      names = ["--computations-service-target"],
      description =
        ["gRPC target (authority string or URI) for Duchy internal Computations service."],
      required = true
    )
    lateinit var computationsServiceTarget: String
      private set

    @CommandLine.Option(
      names = ["--system-requisitions-service-target"],
      description =
        ["gRPC target (authority string or URI) for Requisitions service in the system API."],
      required = true
    )
    lateinit var systemRequisitionsServiceTarget: String
      private set
  }

  companion object {
    const val SERVICE_NAME = "DataProvider"
  }
}
