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

package org.wfanet.panelmatch.client.deploy

import java.time.Clock
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.buildChannel
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.panelmatch.client.launcher.BlockingJobLauncher
import org.wfanet.panelmatch.client.launcher.ExchangeStepLauncher
import org.wfanet.panelmatch.client.launcher.ExchangeStepValidatorImpl
import org.wfanet.panelmatch.client.launcher.GrpcApiClient
import org.wfanet.panelmatch.client.launcher.Identity
import picocli.CommandLine

@CommandLine.Command(
  name = "ExchangeWorkflowDaemon",
  description = ["Daemon for Exchange workflow."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(@CommandLine.Mixin flags: ExchangeWorkflowFlags) {
  val exchangeStepsClient =
    ExchangeStepsCoroutineStub(
      buildChannel(flags.exchangeStepsServiceTarget.toString(), flags.channelShutdownTimeout)
    )
  val exchangeStepAttemptsClient =
    ExchangeStepAttemptsCoroutineStub(
      buildChannel(flags.exchangeStepAttemptsServiceTarget.toString(), flags.channelShutdownTimeout)
    )
  val grpcApiClient =
    GrpcApiClient(
      Identity(flags.id, flags.partyType),
      exchangeStepsClient,
      exchangeStepAttemptsClient,
      Clock.systemUTC()
    )
  val pollingThrottler = MinimumIntervalThrottler(Clock.systemUTC(), flags.pollingInterval)
  val exchangeStepLauncher =
    ExchangeStepLauncher(grpcApiClient, ExchangeStepValidatorImpl(), BlockingJobLauncher())

  runBlocking {
    pollingThrottler.loopOnReady {
      logAndSuppressExceptionSuspend { exchangeStepLauncher.findAndRunExchangeStep() }
    }
  }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
