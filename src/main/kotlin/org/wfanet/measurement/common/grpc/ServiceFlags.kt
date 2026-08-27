/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.grpc

import io.opentelemetry.api.metrics.LongCounter
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.NamedThreadFactory
import picocli.CommandLine

/** Flags (command-line options) for a set of gRPC services. */
class ServiceFlags {
  @set:CommandLine.Option(
    names = ["--grpc-thread-pool-size"],
    description =
      [
        "Size of thread pool for gRPC services.",
        "Defaults to number of cores or 2, whichever is larger.",
      ],
    required = false,
  )
  private var threadPoolSize: Int = DEFAULT_THREAD_POOL_SIZE
    set(value) {
      require(value > 0) { "--grpc-thread-pool-size must be positive, got $value" }
      field = value
    }

  /**
   * Executor for gRPC services.
   *
   * The work queue is deliberately unbounded: the only rejection this executor can produce is from
   * being shut down, never from saturation under load.
   */
  val executor: Executor by lazy {
    ThreadPoolExecutor(
        threadPoolSize,
        threadPoolSize,
        KEEP_ALIVE_SECONDS,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(),
        NamedThreadFactory(Executors.defaultThreadFactory(), THREAD_POOL_NAME),
        LoggingRejectedExecutionHandler(),
      )
      .apply { allowCoreThreadTimeOut(true) }
      .also { Instrumentation.instrumentThreadPool(THREAD_POOL_NAME, it) }
  }

  /**
   * Logs a sampled rejection (1 in [LOG_SAMPLE_RATE]) and records it to [rejectionCounter], then
   * falls back to the default abort behavior.
   */
  private class LoggingRejectedExecutionHandler : RejectedExecutionHandler {
    private val abortPolicy = ThreadPoolExecutor.AbortPolicy()
    private val rejectionCount = AtomicLong(0)
    private val rejectionCounter: LongCounter =
      Instrumentation.meter
        .counterBuilder("${Instrumentation.ROOT_NAMESPACE}.thread_pool.rejected_count")
        .setDescription("Number of tasks rejected by the thread pool")
        .build()

    override fun rejectedExecution(runnable: Runnable, executor: ThreadPoolExecutor) {
      rejectionCounter.add(1)
      val count = rejectionCount.incrementAndGet()
      if (count == 1L || count % LOG_SAMPLE_RATE == 0L) {
        logger.log(Level.WARNING) {
          "$THREAD_POOL_NAME executor has rejected $count task(s) total; latest: $executor"
        }
      }
      abortPolicy.rejectedExecution(runnable, executor)
    }
  }

  companion object {
    private const val THREAD_POOL_NAME = "grpc-services"
    private const val KEEP_ALIVE_SECONDS = 60L
    private const val LOG_SAMPLE_RATE = 100L
    private val DEFAULT_THREAD_POOL_SIZE =
      Runtime.getRuntime().availableProcessors().coerceAtLeast(2)
    private val logger: Logger = Logger.getLogger(ServiceFlags::class.java.name)
  }
}
