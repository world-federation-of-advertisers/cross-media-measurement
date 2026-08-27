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
   * `corePoolSize` is set equal to `maximumPoolSize` (rather than a smaller fixed value) because
   * [ThreadPoolExecutor] with an unbounded work queue only ever creates up to `corePoolSize`
   * threads -- workers beyond that are queued instead of triggering new thread creation, since new
   * threads are only spawned when the queue rejects a task, which an unbounded queue never does. A
   * smaller `corePoolSize` here would silently make `--grpc-thread-pool-size` a no-op.
   *
   * The unbounded queue is deliberate, not just a leftover default: a coroutine service dispatched
   * through this executor may already have performed a non-idempotent side effect before suspending
   * and needing to be redispatched to resume. A bounded queue that rejects that redispatch under
   * load would force a choice between hanging the RPC or surfacing a status that implies it's safe
   * to retry the whole thing from scratch, when it may not be. The only rejection this executor can
   * produce is if it has already been shut down.
   *
   * Since all threads are now core threads, `allowCoreThreadTimeOut` is enabled so the keep-alive
   * still does something: without it, [ThreadPoolExecutor] never times out core threads regardless
   * of the keep-alive time, so every server using this executor would hold `threadPoolSize` live
   * idle threads forever, even at zero QPS.
   */
  val executor: Executor by lazy {
    ThreadPoolExecutor(
        threadPoolSize,
        threadPoolSize,
        KEEP_ALIVE_SECONDS,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(),
        NamedThreadFactory(Executors.defaultThreadFactory(), THREAD_POOL_NAME),
        LoggingRejectedExecutionHandler,
      )
      .apply { allowCoreThreadTimeOut(true) }
      .also { Instrumentation.instrumentThreadPool(THREAD_POOL_NAME, it) }
  }

  /**
   * Logs a sampled rejection (1 in [LOG_SAMPLE_RATE]), then falls back to the default abort
   * behavior.
   */
  private object LoggingRejectedExecutionHandler : RejectedExecutionHandler {
    private val abortPolicy = ThreadPoolExecutor.AbortPolicy()
    private val rejectionCount = AtomicLong(0)

    override fun rejectedExecution(runnable: Runnable, executor: ThreadPoolExecutor) {
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
