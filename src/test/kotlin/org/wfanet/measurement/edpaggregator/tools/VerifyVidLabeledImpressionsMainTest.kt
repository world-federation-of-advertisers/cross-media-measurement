// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.tools

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.ExitInterceptingSecurityManager

@RunWith(JUnit4::class)
class VerifyVidLabeledImpressionsMainTest {
  @Test
  fun `main prints help without requiring verification flags`() {
    val capturedOutput = CommandLineTesting.capturingOutput(arrayOf("--help"), ::main)

    val subject = CommandLineTesting.assertThat(capturedOutput)
    subject.status().isEqualTo(0)
    subject.out().contains("--kms-type")
    subject.out().contains("--kek-uri")
    subject.out().contains("--aws-role-arn")
    subject.out().contains("--gcp-to-aws-role-arn")
    subject.out().contains("--aws-audience")
  }

  @Test
  fun `main exits nonzero when required flags are missing`() {
    val capturedOutput = CommandLineTesting.capturingOutput(emptyArray(), ::main)

    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
  }

  // CommandLineTesting uses this security manager to observe the real main function's exit code.
  @Suppress("DEPRECATION")
  companion object {
    init {
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }
  }
}
