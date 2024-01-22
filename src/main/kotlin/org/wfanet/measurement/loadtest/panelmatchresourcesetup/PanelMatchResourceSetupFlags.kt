// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.panelmatchresourcesetup

import java.io.File
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import org.wfanet.measurement.common.grpc.TlsFlags
import picocli.CommandLine

class PanelMatchResourceSetupFlags {
  /** The PanelMatchResourceSetup pod's own tls certificates. */
  @CommandLine.Mixin
  lateinit var tlsFlags: TlsFlags
    private set

  @CommandLine.Option(
    names = ["--kingdom-internal-api-target"],
    description = ["gRPC target (authority) of the Kingdom internal API server"],
    required = true,
  )
  lateinit var kingdomInternalApiTarget: String
    private set

  @CommandLine.Option(
    names = ["--kingdom-internal-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Kingdom internal API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --kingdom-internal-api-target.",
      ],
    required = false,
  )
  var kingdomInternalApiCertHost: String? = null
    private set

  @CommandLine.Option(
    names = ["--edp-display-name"],
    description = ["Event Data Provider (EDP) display name."],
    required = true,
  )
  lateinit var edpDisplayName: String
    private set

  @CommandLine.Option(
    names = ["--edp-cert-der-file"],
    description = ["EDP cert (DER format) file."],
    required = true,
  )
  lateinit var edpCertDerFile: File
    private set

  @CommandLine.Option(
    names = ["--edp-key-der-file"],
    description = ["EDP private key (DER format) file."],
    required = true,
  )
  lateinit var edpKeyDerFile: File
    private set

  @CommandLine.Option(
    names = ["--edp-encryption-public-keyset"],
    description = ["EDP encryption public key Tink Keyset file."],
    required = true,
  )
  lateinit var edpEncryptionPublicKeyset: File
    private set

  @CommandLine.Option(
    names = ["--exchange-workflow"],
    description = ["Exchange Workflow textproto file."],
    required = true,
  )
  lateinit var exchangeWorkflow: File
    private set

  @CommandLine.Option(
    names = ["--run-id"],
    description = ["Unique identifier of the run, if not set, timestamp will be used."],
    required = false,
  )
  var runId: String =
    DateTimeFormatter.ofPattern("yyyy-MM-ddHH-mm-ss-SSS")
      .withZone(ZoneOffset.UTC)
      .format(Instant.now())
    private set
}
