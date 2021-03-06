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

package org.wfanet.measurement.loadtest.resourcesetup

import java.io.File
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.loadtest.KingdomPublicApiFlags
import picocli.CommandLine

class ResourceSetupFlags {

  /** The ResourceSetup pod's own tls certificates. */
  @CommandLine.Mixin
  lateinit var tlsFlags: TlsFlags
    private set

  @CommandLine.Option(
    names = ["--duchy-consent-signaling-cert-der-files"],
    description =
      ["The map from external Duchy Id to its consent signaling cert (DER format) file."],
    required = true
  )
  lateinit var duchyCsCertDerFiles: Map<String, File>
    private set

  @CommandLine.Option(
    names = ["--edp-consent-signaling-cert-der-files"],
    description =
      ["The map from EDP display name to its consent signaling cert (DER format) file."],
    required = true
  )
  lateinit var edpCsCertDerFiles: Map<String, File>
    private set

  @CommandLine.Option(
    names = ["--edp-consent-signaling-key-der-files"],
    description =
      ["The map from EDP display name to its consent signaling private key (DER format) file."],
    required = true
  )
  lateinit var edpCsKeyDerFiles: Map<String, File>
    private set

  @CommandLine.Option(
    names = ["--edp-encryption-public-key-der-files"],
    description = ["The map from EDP display name to its encryption public key (DER format) file."],
    required = true
  )
  lateinit var edpEncryptionPublicKeyDerFiles: Map<String, File>
    private set

  @CommandLine.Option(
    names = ["--mc-consent-signaling-cert-der-files"],
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
  lateinit var mcCsKeyDerFiles: File
    private set

  @CommandLine.Option(
    names = ["--mc-encryption-public-key-der-file"],
    description = ["The MC's encryption public key (DER format) file."],
    required = true
  )
  lateinit var mcEncryptionPublicKeyDerFile: File
    private set

  @CommandLine.Mixin
  lateinit var kingdomPublicApiFlags: KingdomPublicApiFlags
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
}
