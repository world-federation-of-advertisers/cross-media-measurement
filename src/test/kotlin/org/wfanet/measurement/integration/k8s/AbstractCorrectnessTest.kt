/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.k8s

import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.loadtest.frontend.FrontendSimulator

/** Test for correctness of the CMMS on Kubernetes. */
abstract class AbstractCorrectnessTest(private val measurementSystem: MeasurementSystem) {
  private val runId: String
    get() = measurementSystem.runId
  private val testHarness: FrontendSimulator
    get() = measurementSystem.testHarness

  @Test(timeout = 1 * 60 * 1000)
  fun `impression measurement completes with expected result`() = runBlocking {
    testHarness.executeImpression("$runId-impression")
  }

  @Test(timeout = 1 * 60 * 1000)
  fun `duration measurement completes with expected result`() = runBlocking {
    testHarness.executeDuration("$runId-duration")
  }

  @Test(timeout = 8 * 60 * 1000)
  fun `reach and frequency measurement completes with expected result`() = runBlocking {
    testHarness.executeReachAndFrequency("$runId-reach-and-freq")
  }

  interface MeasurementSystem {
    val runId: String
    val testHarness: FrontendSimulator
  }
}
