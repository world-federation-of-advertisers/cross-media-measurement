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

package org.wfanet.measurement.securecomputation.deploy.gcloud.spanner

import com.google.common.truth.Truth.assertThat
import java.util.concurrent.CountDownLatch
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import picocli.CommandLine

@RunWith(JUnit4::class)
class InternalApiServerTest {
  @Test
  fun `command line rejects zero RPC interval`() {
    val exception =
      assertFailsWith<CommandLine.ParameterException> {
        CommandLine(InternalApiServer()).parseArgs("--metadata-read-rpc-min-interval=0s")
      }

    assertThat(exception).hasMessageThat().contains("positive human-readable duration")
  }

  @Test
  fun `command line rejects negative RPC interval`() {
    val exception =
      assertFailsWith<CommandLine.ParameterException> {
        CommandLine(InternalApiServer()).parseArgs("--metadata-read-rpc-min-interval=-1s")
      }

    assertThat(exception).hasMessageThat().contains("complete human-readable duration")
  }

  @Test
  fun `command line rejects partially malformed RPC interval`() {
    val exception =
      assertFailsWith<CommandLine.ParameterException> {
        CommandLine(InternalApiServer()).parseArgs("--control-plane-rpc-min-interval=500msjunk")
      }

    assertThat(exception).hasMessageThat().contains("complete human-readable duration")
  }

  @Test
  fun `command line rejects zero WorkItem publication interval`() {
    val exception =
      assertFailsWith<CommandLine.ParameterException> {
        CommandLine(InternalApiServer()).parseArgs("--work-item-publication-poll-interval=0s")
      }

    assertThat(exception).hasMessageThat().contains("positive human-readable duration")
  }

  @Test
  fun `publication runner executes while server wait blocks`() = runBlocking {
    val stopServer = CountDownLatch(1)
    val serverStarted = CompletableDeferred<Unit>()
    var publicationRunnerExecuted = false

    withTimeout(5_000) {
      runInternalApiServerJobs(
        blockingServer = {
          serverStarted.complete(Unit)
          stopServer.await()
        },
        backgroundJobs =
          listOf {
            serverStarted.await()
            publicationRunnerExecuted = true
            stopServer.countDown()
          },
      )
    }

    assertThat(publicationRunnerExecuted).isTrue()
  }
}
