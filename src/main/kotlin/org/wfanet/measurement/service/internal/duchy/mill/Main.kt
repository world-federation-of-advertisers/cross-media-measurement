// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.service.internal.duchy.mill

import io.grpc.ManagedChannelBuilder
import java.time.Duration
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.addChannelShutdownHooks
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt
import picocli.CommandLine

private class Flags {
  @CommandLine.Option(
    names = ["--channel-shutdown-timeout"],
    defaultValue = "5s"
  )
  lateinit var channelShutdownTimeout: Duration
    private set

  @CommandLine.Option(
    names = ["--minimum-polling-interval"],
    defaultValue = "1s"
  )
  lateinit var minimumPollingInterval: Duration
    private set

  @CommandLine.Option(
    names = ["--next-worker"],
    description = ["The URI of the next WorkerService in the ring."],
    defaultValue = "localhost:8080"
  )
  lateinit var nextWorker: String
    private set
}

@CommandLine.Command(
  name = "mill",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(@CommandLine.Mixin flags: Flags) {
  // TODO Figure out how to configure a collection of duchies. Config file?

  val channel =
    ManagedChannelBuilder.forTarget(flags.nextWorker)
      .usePlaintext()
      .build()

  addChannelShutdownHooks(Runtime.getRuntime(), flags.channelShutdownTimeout, channel)

  val stub = ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub(channel)

  val mill = Mill(mapOf(Pair("Alsace", stub)), minimumPollingInterval.toMillis())
  runBlocking {
    mill.pollForWork().collect {
      // Deliberately empty
    }
  }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
