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
import org.wfanet.measurement.common.Flags
import org.wfanet.measurement.common.addChannelShutdownHooks
import org.wfanet.measurement.common.durationFlag
import org.wfanet.measurement.common.stringFlag
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt

fun main(args: Array<String>) {
  val channelShutdownTimeout =
    durationFlag("channel-shutdown-timeout", Duration.ofSeconds(5))
  val minimumPollingInterval =
    durationFlag("minimum-polling-interval", Duration.ofSeconds(1))
  // TODO Figure out how to configure a collection of duchies. Config file?
  // The URI of the next ComputationControlService in the ring.
  val nextWorker = stringFlag("next-worker", "localhost:8080")
  Flags.parse(args.asIterable())

  val channel =
    ManagedChannelBuilder.forTarget(nextWorker.value)
      .usePlaintext()
      .build()

  addChannelShutdownHooks(Runtime.getRuntime(), channelShutdownTimeout.value, channel)

  val stub = ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub(channel)

  val mill = Mill(mapOf(Pair("Alsace", stub)), minimumPollingInterval.value.toMillis())
  runBlocking {
    mill.pollForWork().collect {
      // Deliberately empty
    }
  }
}
