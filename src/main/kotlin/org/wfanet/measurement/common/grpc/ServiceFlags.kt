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
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.NamedThreadFactory
import picocli.CommandLine

/** Flags (command-line options) for a set of gRPC services. */
class ServiceFlags {
  @CommandLine.Option(
    names = ["--grpc-thread-pool-size"],
    description =
      [
        "Size of thread pool for gRPC services.",
        "Defaults to number of cores or 2, whichever is larger.",
      ],
    required = false,
  )
  private var threadPoolSize: Int = DEFAULT_THREAD_POOL_SIZE

  /**
   * Executor for gRPC services.
   *
   * `corePoolSize` is set equal to `maximumPoolSize` (rather than a smaller fixed value) because
   * [ThreadPoolExecutor] with an unbounded work queue only ever creates up to `corePoolSize`
   * threads -- workers beyond that are queued instead of triggering new thread creation, since new
   * threads are only spawned when the queue rejects a task, which an unbounded queue never does. A
   * smaller `corePoolSize` here would silently make `--grpc-thread-pool-size` a no-op.
   */
  val executor: Executor by lazy {
    ThreadPoolExecutor(
        threadPoolSize,
        threadPoolSize,
        60L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(),
        NamedThreadFactory(Executors.defaultThreadFactory(), THREAD_POOL_NAME),
      )
      .also { Instrumentation.instrumentThreadPool(THREAD_POOL_NAME, it) }
  }

  companion object {
    private const val THREAD_POOL_NAME = "grpc-services"
    private val DEFAULT_THREAD_POOL_SIZE =
      Runtime.getRuntime().availableProcessors().coerceAtLeast(2)
  }
}
