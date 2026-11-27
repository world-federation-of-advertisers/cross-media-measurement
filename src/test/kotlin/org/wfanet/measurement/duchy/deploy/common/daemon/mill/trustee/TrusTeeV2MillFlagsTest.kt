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

package org.wfanet.measurement.duchy.deploy.common.daemon.mill.trustee

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.duchy.NoiseMechanism
import picocli.CommandLine

@RunWith(JUnit4::class)
class TrusTeeV2MillFlagsTest {
  @Test
  fun `parse leaves the TrusTEE v2 Mill disabled without its flags`() {
    assertThat(parse()).isNull()
  }

  @Test
  fun `parse reads the noise mechanism thresholds and cache size`() {
    val flags: TrusTeeV2MillFlags =
      checkNotNull(
        parse(
          "--trus-tee-v2-noise-mechanism=DETERMINISTIC_TRUNCATED_LAPLACE",
          "--trus-tee-v2-min-users=10",
          "--trus-tee-v2-min-impressions=20",
          "--trus-tee-v2-reach-max-frequency-per-user=5",
          "--trus-tee-v2-hash-rank-order-cache-size=3",
        )
      )

    assertThat(flags.noiseMechanism).isEqualTo(NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)
    val thresholds = checkNotNull(flags.resultMinimumThresholds)
    assertThat(thresholds.minUsers).isEqualTo(10)
    assertThat(thresholds.minImpressions).isEqualTo(20)
    assertThat(thresholds.reachMaxFrequencyPerUser).isEqualTo(5)
    assertThat(flags.hashRankOrderCacheSize).isEqualTo(3)
    assertThat(flags.fallbackEventDescriptor).isNull()
  }

  @Test
  fun `parse defaults the cache size to one`() {
    val flags: TrusTeeV2MillFlags = checkNotNull(parse("--trus-tee-v2-noise-mechanism=NONE"))

    assertThat(flags.hashRankOrderCacheSize).isEqualTo(1)
    assertThat(flags.resultMinimumThresholds).isNull()
  }

  @Test
  fun `parse throws when a threshold of the group is missing`() {
    assertFailsWith<CommandLine.ParameterException> {
      parse("--trus-tee-v2-noise-mechanism=NONE", "--trus-tee-v2-min-users=10")
    }
  }

  /** A command holding only the TrusTEE v2 flag group, as [TrusTeeMillFlags] holds it. */
  class TestCommand {
    @CommandLine.ArgGroup(exclusive = false, multiplicity = "0..1")
    var trusTeeV2: TrusTeeV2MillFlags? = null
  }

  private fun parse(vararg args: String): TrusTeeV2MillFlags? {
    val command = TestCommand()
    CommandLine(command).parseArgs(*args)
    return command.trusTeeV2
  }
}
