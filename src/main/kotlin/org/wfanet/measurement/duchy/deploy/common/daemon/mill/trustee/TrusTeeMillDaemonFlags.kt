// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.common.daemon.mill.trustee

import java.io.File
import java.time.Duration
import kotlin.properties.Delegates
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import org.wfanet.measurement.duchy.deploy.common.ComputationsServiceFlags
import org.wfanet.measurement.duchy.deploy.common.SystemApiFlags
import org.wfanet.measurement.duchy.mill.MillFlags
import org.wfanet.measurement.internal.duchy.NoiseMechanism
import picocli.CommandLine

class TrusTeeMillFlags : MillFlags() {
  @CommandLine.Mixin
  lateinit var duchy: CommonDuchyFlags
    private set

  @CommandLine.Mixin
  lateinit var systemApiFlags: SystemApiFlags
    private set

  @CommandLine.Mixin
  lateinit var computationsServiceFlags: ComputationsServiceFlags
    private set

  @CommandLine.Option(
    names = ["--attestation-token-file"],
    description = ["The file of attestation token for KMS credential."],
    required = true,
  )
  lateinit var attestationTokenFile: File
    private set

  @CommandLine.Option(
    names = ["--polling-interval"],
    defaultValue = "2s",
    description =
      ["How long to wait after an empty claim or an exception escaping work processing."],
  )
  lateinit var pollingInterval: Duration
    private set

  /** Flags of the TrusTEE multimeasurement Mill, which runs only when they are set. */
  @CommandLine.ArgGroup(exclusive = false, multiplicity = "0..1", heading = "TrusTEE v2 Mill%n")
  var trusTeeV2: TrusTeeV2MillFlags? = null
    private set
}

class TrusTeeV2MillFlags {
  @CommandLine.Option(
    names = ["--trus-tee-v2-noise-mechanism"],
    description =
      ["Noise mechanism of every released cell. One of NONE, DETERMINISTIC_TRUNCATED_LAPLACE."],
    required = true,
  )
  lateinit var noiseMechanism: NoiseMechanism
    private set

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "0..1")
  var resultMinimumThresholds: ResultMinimumThresholdsFlags? = null
    private set

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "0..1")
  var fallbackEventDescriptor: EventDescriptorFlags? = null
    private set

  @set:CommandLine.Option(
    names = ["--trus-tee-v2-hash-rank-order-cache-size"],
    description = ["How many PopulationSpecs' VID hash rank orders are kept between computations."],
    defaultValue = "1",
  )
  var hashRankOrderCacheSize: Int by Delegates.notNull()
    private set

  class ResultMinimumThresholdsFlags {
    @set:CommandLine.Option(
      names = ["--trus-tee-v2-min-users"],
      description = ["Minimum noised reach of a released cell and of each DataProvider's slice."],
      required = true,
    )
    var minUsers: Int by Delegates.notNull()
      private set

    @set:CommandLine.Option(
      names = ["--trus-tee-v2-min-impressions"],
      description = ["Minimum noised impressions of a released cell."],
      required = true,
    )
    var minImpressions: Int by Delegates.notNull()
      private set

    @set:CommandLine.Option(
      names = ["--trus-tee-v2-reach-max-frequency-per-user"],
      description = ["Most impressions one VID contributes to a cell under noise."],
      required = true,
    )
    var reachMaxFrequencyPerUser: Int by Delegates.notNull()
      private set
  }

  class EventDescriptorFlags {
    @CommandLine.Option(
      names = ["--trus-tee-v2-event-descriptor-set"],
      description =
        [
          "Serialized FileDescriptorSet of the event message type, for computations that carry no " +
            "event descriptor."
        ],
      required = true,
    )
    lateinit var descriptorSetFile: File
      private set

    @CommandLine.Option(
      names = ["--trus-tee-v2-event-message-type"],
      description = ["Full name of the event message type within the descriptor set."],
      required = true,
    )
    lateinit var eventMessageType: String
      private set
  }
}
