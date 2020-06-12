package org.wfanet.measurement.common

import io.grpc.ManagedChannel
import java.time.Duration
import java.util.concurrent.TimeUnit

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
  val maxTimeoutDuration = Duration.ofMinutes(10)
  require(timeout <= maxTimeoutDuration) {
    "The timeout duration for channel shutdown hooks cannot be longer than $maxTimeoutDuration."
  }
  channels.forEach {
    r.addShutdownHook(Thread(ChannelShutdownHook(it, timeout.toMillis())))
  }
}
