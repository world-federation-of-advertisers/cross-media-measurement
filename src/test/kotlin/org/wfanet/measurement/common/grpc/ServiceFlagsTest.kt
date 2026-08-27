/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.test.assertFailsWith
import org.junit.After
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.Instrumentation
import picocli.CommandLine
import picocli.CommandLine.ParameterException

@RunWith(JUnit4::class)
class ServiceFlagsTest {
  @After
  fun resetInstrumentation() {
    Instrumentation.resetForTest()
  }

  @Test
  fun `constructor rejects a non-positive pool size`() {
    val flags = ServiceFlags()
    // Picocli wraps the setter's exception in a ParameterException; the underlying cause is the
    // actual validation failure. This only exercises picocli's real binding path because the
    // option is declared with a `set:` use-site target -- a bare annotation targets the backing
    // field by default and picocli would set it directly, silently bypassing this validation.
    val thrown =
      assertFailsWith<ParameterException> {
        CommandLine(flags).parseArgs("--grpc-thread-pool-size=0")
      }
    assertThat(thrown.cause).isInstanceOf(IllegalArgumentException::class.java)
  }

  @Test
  fun `executor runs tasks concurrently up to the configured pool size`() {
    val flags = ServiceFlags()
    CommandLine(flags).parseArgs("--grpc-thread-pool-size=4")

    val startedLatch = CountDownLatch(4)
    val releaseLatch = CountDownLatch(1)
    repeat(4) {
      flags.executor.execute {
        startedLatch.countDown()
        releaseLatch.await()
      }
    }

    // All 4 tasks must start concurrently. With an unbounded work queue and corePoolSize fixed
    // below maximumPoolSize, ThreadPoolExecutor never creates threads beyond corePoolSize -- only
    // the core threads would start and this would time out.
    assertThat(startedLatch.await(5, TimeUnit.SECONDS)).isTrue()
    releaseLatch.countDown()
  }

  @Test
  fun `executor queues rather than rejects tasks beyond the configured pool size`() {
    val flags = ServiceFlags()
    CommandLine(flags).parseArgs("--grpc-thread-pool-size=1")

    val startedLatch = CountDownLatch(1)
    val releaseLatch = CountDownLatch(1)
    val completedCount = CountDownLatch(5)

    flags.executor.execute {
      startedLatch.countDown()
      releaseLatch.await()
      completedCount.countDown()
    }
    assertThat(startedLatch.await(5, TimeUnit.SECONDS)).isTrue()

    // With only one thread already busy, submitting 4 more tasks beyond the configured pool size
    // must not throw -- they should simply queue behind the first task rather than being
    // rejected. A coroutine dispatched through this executor may have already performed a
    // non-idempotent side effect before needing to be redispatched to resume; rejecting that
    // redispatch under load would make it unsafe to imply the whole RPC can just be retried.
    repeat(4) { flags.executor.execute { completedCount.countDown() } }

    releaseLatch.countDown()
    assertWithMessage("Not all queued tasks completed")
      .that(completedCount.await(5, TimeUnit.SECONDS))
      .isTrue()
  }

  @Test
  fun `shutdown executor still rejects rather than accepting new work indefinitely`() {
    val flags = ServiceFlags()
    CommandLine(flags).parseArgs("--grpc-thread-pool-size=1")

    flags.executor.execute {}
    (flags.executor as java.util.concurrent.ExecutorService).shutdown()

    assertFailsWith<java.util.concurrent.RejectedExecutionException> { flags.executor.execute {} }
  }

  @Test
  fun `idle core threads are eligible for reclamation rather than held forever`() {
    val flags = ServiceFlags()
    CommandLine(flags).parseArgs("--grpc-thread-pool-size=4")

    // With corePoolSize equal to maximumPoolSize, every thread is a core thread; without
    // allowCoreThreadTimeOut, ThreadPoolExecutor never reclaims core threads regardless of the
    // keep-alive time, so every server using this executor would hold 4 live idle threads
    // forever, even at zero QPS.
    assertThat((flags.executor as ThreadPoolExecutor).allowsCoreThreadTimeOut()).isTrue()
  }
}
