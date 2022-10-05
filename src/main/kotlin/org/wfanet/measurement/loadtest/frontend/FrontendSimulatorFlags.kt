// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.frontend

import java.io.File
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import kotlin.properties.Delegates
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.loadtest.KingdomPublicApiFlags
import picocli.CommandLine

class FrontendSimulatorFlags {

  /** The FrontendSimulator pod's own tls certificates. */
  @CommandLine.Mixin
  lateinit var tlsFlags: TlsFlags
    private set

  @CommandLine.Mixin
  lateinit var kingdomPublicApiFlags: KingdomPublicApiFlags
    private set

  @CommandLine.Option(
    names = ["--mc-resource-name"],
    description = ["The resource name of the measurement consumer."],
    required = true
  )
  lateinit var mcResourceName: String
    private set

  @CommandLine.Option(
    names = ["--mc-consent-signaling-cert-der-file"],
    description = ["The MC's consent signaling cert (DER format) file."],
    required = true
  )
  lateinit var mcCsCertDerFile: File
    private set

  @CommandLine.Option(
    names = ["--mc-consent-signaling-key-der-file"],
    description = ["The MC's consent signaling private key (DER format) file."],
    required = true
  )
  lateinit var mcCsPrivateKeyDerFile: File
    private set

  @CommandLine.Option(
    names = ["--mc-encryption-private-keyset"],
    description = ["The MC's encryption private Tink Keyset."],
    required = true
  )
  lateinit var mcEncryptionPrivateKeyset: File
    private set

  @set:CommandLine.Option(
    names = ["--output-differential-privacy-epsilon"],
    description = ["The common epsilon used for all output's differential privacy noises."],
    required = true
  )
  var outputDpEpsilon by Delegates.notNull<Double>()
    private set

  @set:CommandLine.Option(
    names = ["--output-differential-privacy-delta"],
    description = ["The common delta used for all output's differential privacy noises."],
    required = true
  )
  var outputDpDelta by Delegates.notNull<Double>()
    private set

  @CommandLine.Option(
    names = ["--run-id"],
    description = ["Unique identifier of the run, if not set, timestamp will be used."],
    required = false
  )
  var runId: String =
    DateTimeFormatter.ofPattern("yyyy-MM-ddHH-mm-ss-SSS")
      .withZone(ZoneOffset.UTC)
      .format(Instant.now())
    private set

  @CommandLine.Option(
    names = ["--api-authentication-key"],
    description = ["API authentication key for measurement consumer authentication."],
    required = true
  )
  lateinit var apiAuthenticationKey: String
    private set

  @CommandLine.Option(
    names = ["--result-polling-delay"],
    description = ["Duration to delay when polling for Measurement result"],
    defaultValue = "30s",
  )
  lateinit var resultPollingDelay: Duration
    private set
}
