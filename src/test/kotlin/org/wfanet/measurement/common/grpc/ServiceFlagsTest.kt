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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.LogRecord
import java.util.logging.Logger
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.After
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.Instrumentation
import picocli.CommandLine

@RunWith(JUnit4::class)
class ServiceFlagsTest {
  @After
  fun resetInstrumentation() {
    Instrumentation.resetForTest()
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

    // All 4 tasks must start concurrently. With corePoolSize=0 and a SynchronousQueue, each
    // task submission that finds no idle thread spawns a new one up to maximumPoolSize -- if
    // maximumPoolSize were left below the configured size, some tasks would queue forever
    // (SynchronousQueue has no capacity) and this would time out.
    assertTrue(startedLatch.await(5, TimeUnit.SECONDS))
    releaseLatch.countDown()
  }

  @Test
  fun `executor rejects tasks beyond the configured pool size`() {
    val flags = ServiceFlags()
    CommandLine(flags).parseArgs("--grpc-thread-pool-size=1")

    val startedLatch = CountDownLatch(1)
    val releaseLatch = CountDownLatch(1)
    flags.executor.execute {
      startedLatch.countDown()
      releaseLatch.await()
    }
    assertTrue(startedLatch.await(5, TimeUnit.SECONDS))

    // The sole thread is busy and SynchronousQueue has no capacity to hold a second task, so a
    // task submitted beyond the configured pool size is rejected rather than queued.
    assertFailsWith<RejectedExecutionException> { flags.executor.execute {} }

    releaseLatch.countDown()
  }

  @Test
  fun `executor rejection logging is sampled rather than logging every occurrence`() {
    val flags = ServiceFlags()
    CommandLine(flags).parseArgs("--grpc-thread-pool-size=1")

    val startedLatch = CountDownLatch(1)
    val releaseLatch = CountDownLatch(1)
    flags.executor.execute {
      startedLatch.countDown()
      releaseLatch.await()
    }
    assertTrue(startedLatch.await(5, TimeUnit.SECONDS))

    val records = mutableListOf<LogRecord>()
    val handler =
      object : Handler() {
        override fun publish(record: LogRecord) {
          records.add(record)
        }

        override fun flush() {}

        override fun close() {}
      }
    val logger = Logger.getLogger(ServiceFlags::class.java.name)
    logger.addHandler(handler)
    try {
      // Under sustained overload, this handler runs once per rejected task -- logging every
      // occurrence at WARNING would itself become a log storm.
      repeat(250) { runCatching { flags.executor.execute {} } }
    } finally {
      logger.removeHandler(handler)
    }

    val warnings = records.filter { it.level == Level.WARNING }
    assertTrue(warnings.size < 250, "Expected sampled logging, but got ${warnings.size} log lines")
    assertTrue(warnings.isNotEmpty(), "Expected at least the first rejection to be logged")

    releaseLatch.countDown()
  }
}
