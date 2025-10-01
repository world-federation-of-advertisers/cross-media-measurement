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
import java.time.ZoneId
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
    required = true,
  )
  lateinit var dataProviderResourceName: String
    private set

  @CommandLine.Option(
    names = ["--data-provider-certificate-resource-name"],
    description = ["The public API resource name for data provider consent signaling."],
    required = true,
  )
  lateinit var dataProviderCertificateResourceName: String
    private set

  @CommandLine.Option(
    names = ["--data-provider-display-name"],
    description = ["The display name of this data provider."],
    required = true,
  )
  lateinit var dataProviderDisplayName: String
    private set

  @CommandLine.Option(
    names = ["--data-provider-encryption-private-keyset"],
    description = ["The EDP's encryption private Tink Keyset."],
    required = true,
  )
  lateinit var edpEncryptionPrivateKeyset: File
    private set

  @CommandLine.Option(
    names = ["--data-provider-consent-signaling-private-key-der-file"],
    description = ["The EDP's consent signaling private key (DER format) file."],
    required = true,
  )
  lateinit var edpCsPrivateKeyDerFile: File
    private set

  @CommandLine.Option(
    names = ["--data-provider-consent-signaling-certificate-der-file"],
    description = ["The EDP's consent signaling private key (DER format) file."],
    required = true,
  )
  lateinit var edpCsCertificateDerFile: File
    private set

  @CommandLine.Option(
    names = ["--mc-resource-name"],
    description = ["The public API resource name of the Measurement Consumer."],
    required = true,
  )
  lateinit var mcResourceName: String
    private set

  @CommandLine.Option(
    names = ["--throttler-minimum-interval"],
    description = ["Minimum throttle interval"],
    defaultValue = "2s",
  )
  lateinit var throttlerMinimumInterval: Duration
    private set

  @CommandLine.Mixin
  lateinit var kingdomPublicApiFlags: KingdomPublicApiFlags
    private set

  @CommandLine.ArgGroup(
    exclusive = false,
    multiplicity = "1..*",
    heading = "Configurations of the Duchies' RequisitionFullfillment servers",
  )
  lateinit var requisitionFulfillmentServiceFlags: List<RequisitionFulfillmentServiceFlags>
    private set

  @CommandLine.Option(
    names = ["--event-message-descriptor-set"],
    description =
      [
        "Serialized FileDescriptorSet for the event message and its dependencies.",
        "This can be specified multiple times.",
        "It need not be specified if the event message type is $TEST_EVENT_MESSAGE_TYPE.",
      ],
    required = false,
  )
  var eventMessageDescriptorSetFiles: List<File> = emptyList()
    private set

  @CommandLine.Option(
    names = ["--population-spec"],
    description = ["Path to SyntheticPopulationSpec message in text format."],
    required = true,
  )
  lateinit var populationSpecFile: File
    private set

  lateinit var syntheticDataTimeZone: ZoneId
    private set

  @CommandLine.Option(
    names = ["--synthetic-data-time-zone"],
    description = ["ID of time zone for synthetic data generation"],
    required = false,
    defaultValue = "UTC",
  )
  private fun setSyntheticDataTimeZone(zoneId: String) {
    syntheticDataTimeZone = ZoneId.of(zoneId)
  }

  @CommandLine.Option(
    names = ["--support-hmss"],
    description = ["Whether to support HMSS protocol requisitions."],
  )
  var supportHmss: Boolean = false
    private set

  @CommandLine.Option(
    names = ["--random-seed"],
    description = ["Random seed of differential privacy noisers for direct measurements"],
    required = false,
  )
  var randomSeed: Long? = null
    private set

  @CommandLine.Option(
    names = ["--log-sketch-details"],
    description =
      [
        "Whether to log Sketch content.",
        "WARNING: Sketch contains sensitive information if it is generated from real data.",
      ],
    required = false,
  )
  var logSketchDetails: Boolean = false
    private set

  @CommandLine.Option(
    names = ["--health-file"],
    description = ["File indicating whether the process is healthy"],
    required = false,
  )
  var healthFile: File? = null
    private set

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "0..1")
  var trusTeeParams: TrusTeeParams? = null
    private set

  class TrusTeeParams {
    @CommandLine.Option(
      names = ["--kms-kek-uri"],
      description = ["The uri of kms key encryption key."],
      required = true,
    )
    lateinit var kmsKekUri: String
      private set

    @CommandLine.Option(
      names = ["--workload-identity-provider"],
      description = ["The resource name of the workload identity provider."],
      required = true,
    )
    lateinit var workloadIdentityProvider: String
      private set

    @CommandLine.Option(
      names = ["--impersonated-service-account"],
      description = ["The name of the service account to impersonate."],
      required = true,
    )
    lateinit var impersonatedServiceAccount: String
      private set
  }

  companion object {
    const val TEST_EVENT_MESSAGE_TYPE =
      "wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
  }
}
