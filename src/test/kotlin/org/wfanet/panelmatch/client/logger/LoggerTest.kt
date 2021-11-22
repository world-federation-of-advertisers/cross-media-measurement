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

package org.wfanet.panelmatch.client.logger

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.common.loggerFor

private class JobTestClass1 {
  suspend fun logWithDelay() {
    logger.addToTaskLog("logWithDelay: Log Message A")
    delay(100)
    logger.addToTaskLog("logWithDelay: Log Message B")
  }
  companion object {
    val logger by loggerFor()
  }
}

private class JobTestClass2 {
  suspend fun logWithDelay(): Unit = coroutineScope {
    launch {
      logger.addToTaskLog("logWithDelay: Log Message C")
      delay(100)
      logger.addToTaskLog("logWithDelay: Log Message D")
    }
  }
  companion object {
    val logger by loggerFor()
  }
}

private class JobTestClass3 {
  suspend fun logWithDelay(): Unit = coroutineScope {
    val attemptKey = java.util.UUID.randomUUID().toString()
    launch(CoroutineName(attemptKey) + Dispatchers.Default) {
      logger.addToTaskLog("logWithDelay: Log Message C")
      delay(100)
      logger.addToTaskLog("logWithDelay: Log Message D")
    }
  }
  companion object {
    val logger by loggerFor()
  }
}

@RunWith(JUnit4::class)
class LoggerTest {
  @Before
  fun clearLogsForTesting() {
    clearLogs()
  }

  @Test
  fun `write single task log from coroutine and suspend function`() = runBlocking {
    val attemptKey = java.util.UUID.randomUUID().toString()
    val job =
      async(CoroutineName(attemptKey) + Dispatchers.Default) {
        logger.addToTaskLog("Log Message 0")
        JobTestClass1().logWithDelay()
        val log = getAndClearTaskLog()
        assertThat(log).hasSize(3)
        log.forEach { assertThat(it).contains(attemptKey) }
      }
    job.join()
  }

  @Test
  fun `multiple jobs have separate task logs`() =
    runBlocking<Unit> {
      val attemptKey0 = java.util.UUID.randomUUID().toString()
      val attemptKey1 = java.util.UUID.randomUUID().toString()
      awaitAll(
        async(CoroutineName(attemptKey0) + Dispatchers.Default) {
          logger.addToTaskLog("Log Message 0")
          JobTestClass1().logWithDelay()
          val log = getAndClearTaskLog()
          assertThat(log).hasSize(3)
          log.forEach { assertThat(it).contains(attemptKey0) }
        },
        async(CoroutineName(attemptKey1) + Dispatchers.Default) {
          JobTestClass1().logWithDelay()
          val log = getAndClearTaskLog()
          assertThat(log).hasSize(2)
          log.forEach { assertThat(it).contains(attemptKey1) }
        }
      )
    }

  @Test
  fun `cannot add to a task log unless you are in a job`() =
    runBlocking<Unit> {
      assertFailsWith(IllegalArgumentException::class) { logger.addToTaskLog("Log Message 0") }
    }

  @Test
  fun `jobs inside of jobs use the same log`() = runBlocking {
    val attemptKey = java.util.UUID.randomUUID().toString()
    val job =
      async(CoroutineName(attemptKey) + Dispatchers.Default) {
        logger.addToTaskLog("Log Message 0")
        val subJob = launch { JobTestClass2().logWithDelay() }
        JobTestClass1().logWithDelay()
        subJob.join()
        val log = getAndClearTaskLog()
        assertThat(log).hasSize(5)
        log.forEach { assertThat(it).contains(attemptKey) }
      }
    job.join()
  }
  @Test
  fun `jobs inside of jobs with different coroutine names use the different logs`() = runBlocking {
    val attemptKey = java.util.UUID.randomUUID().toString()
    val job =
      async(CoroutineName(attemptKey) + Dispatchers.Default) {
        logger.addToTaskLog("Log Message 0")
        val subJob = launch { JobTestClass3().logWithDelay() }
        JobTestClass1().logWithDelay()
        subJob.join()
        val log = getAndClearTaskLog()
        assertThat(log).hasSize(3)
        log.forEach { assertThat(it).contains(attemptKey) }
      }
    job.join()
  }
  companion object {
    val logger by loggerFor()
  }
}
