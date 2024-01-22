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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.common.loggerFor
import org.wfanet.panelmatch.common.testing.runBlockingTest

private const val NAME = "some-name"
private const val ANOTHER_NAME = "another-name"

private class JobTestClass1 {
  suspend fun logWithDelay() {
    logger.addToTaskLog("logWithDelay: Log Message A")
    delay(100)
    logger.addToTaskLog("logWithDelay: Log Message B")
  }

  companion object {
    private val logger by loggerFor()
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
    private val logger by loggerFor()
  }
}

private class JobTestClass3 {
  suspend fun logWithDelay(): Unit = coroutineScope {
    launch(TaskLog(NAME) + Dispatchers.Default) {
      logger.addToTaskLog("logWithDelay: Log Message C")
      delay(100)
      logger.addToTaskLog("logWithDelay: Log Message D")
    }
  }

  companion object {
    private val logger by loggerFor()
  }
}

@RunWith(JUnit4::class)
class LoggerTest {
  @Test
  fun `write single task log from coroutine and suspend function`() = runBlocking {
    val job =
      async(TaskLog(NAME) + Dispatchers.Default) {
        logger.addToTaskLog("Log Message 0")
        JobTestClass1().logWithDelay()
        val log = getAndClearTaskLog()
        assertThat(log).hasSize(3)
        log.forEach { assertThat(it).contains(NAME) }
      }
    job.join()
  }

  @Test
  fun `multiple jobs have separate task logs`() = runBlockingTest {
    awaitAll(
      async(TaskLog(NAME) + Dispatchers.Default) {
        logger.addToTaskLog("Log Message 0")
        JobTestClass1().logWithDelay()
        val log = getAndClearTaskLog()
        assertThat(log).hasSize(3)
        log.forEach { assertThat(it).contains(NAME) }
      },
      async(TaskLog(ANOTHER_NAME) + Dispatchers.Default) {
        JobTestClass1().logWithDelay()
        val log = getAndClearTaskLog()
        assertThat(log).hasSize(2)
        log.forEach { assertThat(it).contains(ANOTHER_NAME) }
      },
    )
  }

  @Test
  fun `cannot add to a task log unless you are in a job`() = runBlockingTest {
    assertFailsWith<IllegalArgumentException> { logger.addToTaskLog("Log Message 0") }
  }

  @Test
  fun `jobs inside of jobs use the same log`() = runBlockingTest {
    val job =
      async(TaskLog(NAME) + Dispatchers.Default) {
        logger.addToTaskLog("Log Message 0")
        val subJob = launch { JobTestClass2().logWithDelay() }
        JobTestClass1().logWithDelay()
        subJob.join()
        val log = getAndClearTaskLog()
        assertThat(log).hasSize(5)
        log.forEach { assertThat(it).contains(NAME) }
      }
    job.join()
  }

  @Test
  fun `jobs inside of jobs with different coroutine names use the different logs`() = runBlocking {
    val job =
      async(TaskLog(NAME) + Dispatchers.Default) {
        logger.addToTaskLog("Log Message 0")
        val subJob = launch { JobTestClass3().logWithDelay() }
        JobTestClass1().logWithDelay()
        subJob.join()
        val log = getAndClearTaskLog()
        assertThat(log).hasSize(3)
        log.forEach { assertThat(it).contains(NAME) }
      }
    job.join()
  }

  companion object {
    private val logger by loggerFor()
  }
}
