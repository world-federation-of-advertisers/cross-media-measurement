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

package org.wfanet.measurement.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import picocli.CommandLine

@RunWith(JUnit4::class)
class VidLabelingHealTest {
  @Test
  fun `command exposes only supported operator actions`() {
    assertThat(CommandLine(VidLabelingHeal()).subcommands.keys)
      .containsExactly(
        "mark-failed",
        "retry-failed",
        "backfill-model-line",
        "list-correction-candidates",
        "get-correction-candidate",
        "list-correction-plans",
        "get-correction-plan",
        "approve-correction-plan",
        "retry-correction-plan",
        "redeliver-dlq",
        "help",
      )
  }

  @Test
  fun `command line rejects negative RPC interval`() {
    val exception =
      assertFailsWith<CommandLine.ParameterException> {
        CommandLine(VidLabelingHeal())
          .parseArgs("retry-failed", "--metadata-read-rpc-min-interval=-1s")
      }

    assertThat(exception).hasMessageThat().contains("complete human-readable duration")
  }

  @Test
  fun `command line rejects partially malformed RPC interval`() {
    val exception =
      assertFailsWith<CommandLine.ParameterException> {
        CommandLine(VidLabelingHeal())
          .parseArgs("retry-failed", "--control-plane-rpc-min-interval=500msjunk")
      }

    assertThat(exception).hasMessageThat().contains("complete human-readable duration")
  }

  @Test
  fun `retry failed accepts separate EDPA and control plane TLS files`() {
    val result =
      CommandLine(VidLabelingHeal())
        .parseArgs(
          "retry-failed",
          "--edpa-public-api-target=edpa.example:443",
          "--control-plane-api-target=control-plane.example:443",
          "--raw-impression-upload=dataProviders/dp/rawImpressionUploads/upload",
          "--model-line=modelProviders/mp/modelSuites/ms/modelLines/ml",
          "--tls-cert-file=edpa.pem",
          "--tls-key-file=edpa.key",
          "--cert-collection-file=edpa-roots.pem",
          "--control-plane-tls-cert-file=control-plane.pem",
          "--control-plane-tls-key-file=control-plane.key",
          "--control-plane-cert-collection-file=control-plane-roots.pem",
        )

    val retryFailed = checkNotNull(result.subcommand())
    assertThat(retryFailed.hasMatchedOption("--tls-cert-file")).isTrue()
    assertThat(retryFailed.hasMatchedOption("--control-plane-tls-cert-file")).isTrue()
  }
}
