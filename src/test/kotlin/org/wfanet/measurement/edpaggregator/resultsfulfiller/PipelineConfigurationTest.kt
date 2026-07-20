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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class PipelineConfigurationTest {
  private fun config(
    batchSize: Int = 1,
    channelCapacity: Int = 1,
    threadPoolSize: Int = 1,
    workers: Int = 1,
    readConcurrency: Int = PipelineConfiguration.DEFAULT_READ_CONCURRENCY,
  ) = PipelineConfiguration(batchSize, channelCapacity, threadPoolSize, workers, readConcurrency)

  @Test
  fun `validate accepts a valid configuration`() {
    config().validate()
  }

  @Test
  fun `validate rejects non-positive readConcurrency`() {
    assertFailsWith<IllegalArgumentException> { config(readConcurrency = 0).validate() }
  }

  @Test
  fun `validate rejects non-positive batchSize`() {
    assertFailsWith<IllegalArgumentException> { config(batchSize = 0).validate() }
  }

  @Test
  fun `validate rejects non-positive channelCapacity`() {
    assertFailsWith<IllegalArgumentException> { config(channelCapacity = 0).validate() }
  }

  @Test
  fun `validate rejects non-positive workers`() {
    assertFailsWith<IllegalArgumentException> { config(workers = 0).validate() }
  }

  @Test
  fun `validate rejects non-positive threadPoolSize`() {
    assertFailsWith<IllegalArgumentException> { config(threadPoolSize = 0).validate() }
  }
}
