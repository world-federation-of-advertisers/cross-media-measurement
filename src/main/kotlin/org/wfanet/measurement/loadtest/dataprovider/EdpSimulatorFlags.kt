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

package org.wfanet.measurement.loadtest.dataprovider

import java.io.File
import java.time.Duration
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.loadtest.KingdomPublicApiFlags
import org.wfanet.measurement.loadtest.RequisitionFulfillmentServiceFlags
import picocli.CommandLine

class EdpSimulatorFlags {

  /** The EdpSimulator pod's own tls certificates. */
  @CommandLine.Mixin
  lateinit var tlsFlags: TlsFlags
    private set

  @CommandLine.Option(
    names = ["--data-provider-resource-name"],
    description = ["The public API resource name of this data provider."],
    required = true
  )
  lateinit var dataProviderResourceName: String
    private set

  @CommandLine.Option(
    names = ["--data-provider-display-name"],
    description = ["The display name of this data provider."],
    required = true
  )
  lateinit var dataProviderDisplayName: String
    private set

  @CommandLine.Option(
    names = ["--data-provider-encryption-private-keyset"],
    description = ["The EDP's encryption private Tink Keyset."],
    required = true
  )
  lateinit var edpEncryptionPrivateKeyset: File
    private set

  @CommandLine.Option(
    names = ["--data-provider-consent-signaling-private-key-der-file"],
    description = ["The EDP's consent signaling private key (DER format) file."],
    required = true
  )
  lateinit var edpCsPrivateKeyDerFile: File
    private set

  @CommandLine.Option(
    names = ["--data-provider-consent-signaling-certificate-der-file"],
    description = ["The EDP's consent signaling private key (DER format) file."],
    required = true
  )
  lateinit var edpCsCertificateDerFile: File
    private set

  @CommandLine.Option(
    names = ["--mc-resource-name"],
    description = ["The public API resource name of the Measurement Consumer."],
    required = true
  )
  lateinit var mcResourceName: String
    private set

  @CommandLine.Option(
    names = ["--throttler-minimum-interval"],
    description = ["Minimum throttle interval"],
    defaultValue = "2s"
  )
  lateinit var throttlerMinimumInterval: Duration
    private set

  @CommandLine.Mixin
  lateinit var kingdomPublicApiFlags: KingdomPublicApiFlags
    private set

  @CommandLine.Mixin
  lateinit var requisitionFulfillmentServiceFlags: RequisitionFulfillmentServiceFlags
    private set
}
