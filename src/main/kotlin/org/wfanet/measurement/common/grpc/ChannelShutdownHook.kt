// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.grpc

import io.grpc.ManagedChannel
import java.time.Duration
import java.util.concurrent.TimeUnit

private val MAX_SHUTDOWN_TIMEOUT = Duration.ofMinutes(10)

private class ChannelShutdownHook(
  private val channel: ManagedChannel,
  private val timeoutMillis: Long
) : Runnable {
  override fun run() {
    // Use stderr since the logger may have been reset by its JVM shutdown hook.
    System.err.println("*** Shutting down channel: $channel")
    // Give the channel some time to finish what it is doing.
    try {
      channel.shutdown().awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)
    } catch (e: InterruptedException) {
      Thread.currentThread().interrupt()
      System.err.println("*** Channel shutdown interrupted: $e")
    }
    System.err.println("*** Channel shutdown complete: $channel")
  }
}

fun addChannelShutdownHooks(r: Runtime, timeout: Duration, vararg channels: ManagedChannel) {
  channels.forEach {
    r.addShutdownHook(it, timeout)
  }
}

fun Runtime.addShutdownHook(channel: ManagedChannel, timeout: Duration) {
  require(timeout <= MAX_SHUTDOWN_TIMEOUT) {
    "The timeout duration for channel shutdown hooks cannot be longer than $MAX_SHUTDOWN_TIMEOUT."
  }
  addShutdownHook(Thread(ChannelShutdownHook(channel, timeout.toMillis())))
}
