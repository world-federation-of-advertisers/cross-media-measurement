// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common

import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.storage.StorageClient

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessLifeOfAMeasurementIntegrationTest {
  abstract val kingdomDataServicesRule: ProviderRule<DataServices>

  /** Provides a function from Duchy to the dependencies needed to start the Duchy to the test. */
  abstract val duchyDependenciesRule: ProviderRule<(String) -> InProcessDuchy.DuchyDependencies>

  abstract val storageClient: StorageClient

  @get:Rule
  val inProcessComponents: InProcessComponents by lazy {
    InProcessComponents(
      kingdomDataServicesRule,
      duchyDependenciesRule,
      storageClient
    )
  }

  @Before
  fun startDaemons() {
    inProcessComponents.startDaemons()
  }

  @After
  fun stopComponents() {
    inProcessComponents.stopEdpSimulators()
    inProcessComponents.stopDuchyDaemons()
  }

  @Test
  fun `create a RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a reach and frequency measurement and verify its result.
      inProcessComponents.frontendSimulator.executeReachAndFrequency("1234")
    }

  @Test
  fun `create a direct RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a direct reach and frequency measurement and verify its
      // result.
      inProcessComponents.frontendSimulator.executeDirectReachAndFrequency("1234")
    }

  @Test
  fun `create a reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a reach and frequency measurement and verify its result.
      inProcessComponents.frontendSimulator.executeReachOnly("1234")
    }

  @Test
  fun `create an impression measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create an impression measurement and verify its result.
      inProcessComponents.frontendSimulator.executeImpression("1234")
    }

  @Test
  fun `create a duration measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a duration measurement and verify its result.
      inProcessComponents.frontendSimulator.executeDuration("1234")
    }

  @Test
  fun `create a RF measurement of invalid params and check the result contains error info`() =
    runBlocking {
      // Use frontend simulator to create an invalid reach and frequency measurement and verify
      // its error info.
      inProcessComponents.frontendSimulator.executeInvalidReachAndFrequency("1234")
    }

  // TODO(@renjiez): Add Multi-round test given the same input to verify correctness.

  companion object {
    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessComponents.initConfig()
    }
  }
}
