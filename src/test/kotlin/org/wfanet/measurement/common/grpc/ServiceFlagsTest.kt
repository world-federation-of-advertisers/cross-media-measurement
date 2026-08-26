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
import java.util.concurrent.TimeUnit
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import picocli.CommandLine

@RunWith(JUnit4::class)
class ServiceFlagsTest {
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
    assertTrue(startedLatch.await(5, TimeUnit.SECONDS))
    releaseLatch.countDown()
  }
}
