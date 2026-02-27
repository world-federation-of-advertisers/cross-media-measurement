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

package org.wfanet.measurement.loadtest.edpaggregator.tools

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.CommandLineTesting.assertThat
import org.wfanet.measurement.common.testing.ExitInterceptingSecurityManager

@RunWith(JUnit4::class)
class GenerateSyntheticDataTest {

  private fun callCli(args: Array<String>) = CommandLineTesting.capturingOutput(args, ::main)

  @Test
  fun `exits with error when required flags are missing`() {
    val capturedOutput = callCli(arrayOf())

    assertThat(capturedOutput).status().isNotEqualTo(0)
  }

  @Test
  fun `exits with error for invalid kms type`() {
    val capturedOutput =
      callCli(
        arrayOf(
          "--kms-type",
          "INVALID",
          "--event-group-reference-id",
          "eg-1",
          "--model-line",
          "modelProviders/mp1/modelSuites/ms1/modelLines/ml1",
          "--output-bucket",
          "test-bucket",
          "--population-spec-resource-path",
          "pop.textproto",
          "--data-spec-resource-path",
          "data.textproto",
          "--impression-metadata-base-path",
          "/some/path",
        )
      )

    assertThat(capturedOutput).status().isNotEqualTo(0)
    assertThat(capturedOutput).err().contains("INVALID")
  }

  companion object {
    init {
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }
  }
}
