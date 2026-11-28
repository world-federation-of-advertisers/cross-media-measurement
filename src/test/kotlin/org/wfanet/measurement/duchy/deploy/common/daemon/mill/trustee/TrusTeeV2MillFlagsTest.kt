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
import picocli.CommandLine

@RunWith(JUnit4::class)
class TrusTeeV2MillFlagsTest {
  @Test
  fun `parse defaults the cache size to one without a fallback event descriptor`() {
    val flags: TrusTeeV2MillFlags = parse()

    assertThat(flags.hashRankOrderCacheSize).isEqualTo(1)
    assertThat(flags.fallbackEventDescriptor).isNull()
  }

  @Test
  fun `parse reads the cache size and fallback event descriptor`() {
    val flags: TrusTeeV2MillFlags =
      parse(
        "--trus-tee-v2-hash-rank-order-cache-size=3",
        "--trus-tee-v2-event-descriptor-set=/descriptors.pb",
        "--trus-tee-v2-event-message-type=wfa.Event",
      )

    assertThat(flags.hashRankOrderCacheSize).isEqualTo(3)
    val descriptor = checkNotNull(flags.fallbackEventDescriptor)
    assertThat(descriptor.descriptorSetFile.path).isEqualTo("/descriptors.pb")
    assertThat(descriptor.eventMessageType).isEqualTo("wfa.Event")
  }

  @Test
  fun `parse throws when half of the fallback event descriptor is set`() {
    assertFailsWith<CommandLine.ParameterException> {
      parse("--trus-tee-v2-event-message-type=wfa.Event")
    }
  }

  @Test
  fun `parse rejects privacy params, which are compiled in`() {
    assertFailsWith<CommandLine.ParameterException> { parse("--trus-tee-v2-min-users=1") }
    assertFailsWith<CommandLine.ParameterException> { parse("--trus-tee-v2-noise-mechanism=NONE") }
  }

  /** A command holding only the TrusTEE v2 flags, as [TrusTeeMillFlags] mixes them in. */
  class TestCommand {
    @CommandLine.Mixin lateinit var trusTeeV2: TrusTeeV2MillFlags
  }

  private fun parse(vararg args: String): TrusTeeV2MillFlags {
    val command = TestCommand()
    CommandLine(command).parseArgs(*args)
    return command.trusTeeV2
  }
}
