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

package org.wfanet.measurement.integration.common

import com.google.protobuf.TypeRegistry
import kotlinx.coroutines.runBlocking
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.parseTextProto
import picocli.CommandLine

/**
 * Seeds the high overlap synthetic dataset for one ModelLine.
 *
 * Run this when the dataset's specs change or an environment gains an EDP, not on every test run:
 * what it writes persists, and the data it produces is deterministic. Tests read the manifest it
 * leaves behind and refuse to run against a dataset that no longer matches their specs.
 */
@CommandLine.Command(
  name = "HighOverlapSeedingJob",
  description = ["Writes the high overlap synthetic dataset for a ModelLine."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
private fun run(@CommandLine.Mixin flags: HighOverlapSeedingFlags) {
  val config: ImpressionTestDataConfig =
    parseTextProto(
      ImpressionTestDataConfigs.resolveSpecPath(flags.impressionTestDataConfig),
      ImpressionTestDataConfig.getDefaultInstance(),
    )
  val provisioned: ImpressionTestDataConfig =
    if (flags.edpNames.isEmpty()) {
      config
    } else {
      val requested = flags.edpNames.toSet()
      val present = config.eventGroupsList.map { it.edpName }.toSet()
      val missing = requested - present
      require(missing.isEmpty()) { "--edp-name values absent from the dataset: $missing" }
      config
        .toBuilder()
        .clearEventGroups()
        .addAllEventGroups(config.eventGroupsList.filter { it.edpName in requested })
        .build()
    }

  // The population spec packs `Common` into an `Any`, so parsing needs its descriptor.
  val populationSpec: PopulationSpec =
    parseTextProto(
      ImpressionTestDataConfigs.resolveSpecPath(provisioned.populationSpecResourcePath),
      PopulationSpec.getDefaultInstance(),
      TypeRegistry.newBuilder().add(Common.getDescriptor()).build(),
    )

  val modelResources =
    HighOverlapModelResources(
      populationSpecProvider = { populationSpec },
      populationDataProvider = flags.populationDataProvider,
      modelLineName = flags.modelLine,
      earliestEventDateProvider = {
        HighOverlapDataset.datesByImpressionPath(provisioned).values.flatten().min()
      },
      kingdomPublicApiTarget = flags.kingdomPublicApiTarget,
      kingdomPublicApiCertHost = flags.kingdomPublicApiCertHost.ifEmpty { null },
    )

  val impressionWriter =
    HighOverlapImpressionWriter(
      configProvider = { provisioned },
      populationSpecProvider = { populationSpec },
      bucket = flags.storageBucket,
      edp7KekUri = flags.edp7KekUri,
      edpaMetaKekUri = flags.edpaMetaKekUri,
      edpaMetaAwsRoleArn = flags.edpaMetaAwsRoleArn,
      edpaMetaAwsRegion = flags.edpaMetaAwsRegion,
    )

  val seeder =
    HighOverlapSeeder(
      config = provisioned,
      populationSpec = populationSpec,
      bucket = flags.storageBucket,
      modelLine = flags.modelLine,
      measurementConsumer = flags.measurementConsumer,
      googleProjectId = flags.googleProjectId,
      modelResources = modelResources,
      impressionWriter = impressionWriter,
    )

  val manifest = runBlocking { seeder.seed() }
  println("Seeded ${manifest.seededEdpsList} for ${manifest.modelLine}")
  println("Fingerprint ${manifest.specFingerprint}")
}

private class HighOverlapSeedingFlags {
  @CommandLine.Option(
    names = ["--model-line"],
    description = ["ModelLine resource name the impressions are stamped with."],
    required = true,
  )
  lateinit var modelLine: String
    private set

  @CommandLine.Option(
    names = ["--storage-bucket"],
    description = ["Bucket the EventGroup blobs, impressions and manifest are written to."],
    required = true,
  )
  lateinit var storageBucket: String
    private set

  @CommandLine.Option(
    names = ["--google-project-id"],
    description = ["Google Cloud project owning --storage-bucket."],
    required = true,
  )
  lateinit var googleProjectId: String
    private set

  @CommandLine.Option(
    names = ["--measurement-consumer"],
    description = ["MeasurementConsumer the EventGroups are written for."],
    required = true,
  )
  lateinit var measurementConsumer: String
    private set

  @CommandLine.Option(
    names = ["--population-data-provider"],
    description = ["DataProvider that owns the high overlap Population."],
    required = true,
  )
  lateinit var populationDataProvider: String
    private set

  @CommandLine.Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target of the Kingdom public API server."],
    required = true,
  )
  lateinit var kingdomPublicApiTarget: String
    private set

  @CommandLine.Option(
    names = ["--kingdom-public-api-cert-host"],
    description = ["Expected hostname in the Kingdom public API server's TLS certificate."],
    defaultValue = "",
  )
  lateinit var kingdomPublicApiCertHost: String
    private set

  @CommandLine.Option(
    names = ["--impression-test-data-config"],
    description = ["Path to the ImpressionTestDataConfig describing the dataset."],
    required = true,
  )
  lateinit var impressionTestDataConfig: String
    private set

  @CommandLine.Option(
    names = ["--edp-name"],
    description =
      [
        "EDP to seed, repeatable. Defaults to every EDP in the dataset.",
        "Seeding a subset is what the manifest's seeded_edps records, so a test covering an EDP " +
          "that was left out fails rather than silently omitting it.",
      ],
  )
  var edpNames: List<String> = emptyList()
    private set

  @CommandLine.Option(
    names = ["--edp7-kek-uri"],
    description = ["Google Cloud KMS KEK URI shared by the GCP KMS EDPs."],
    defaultValue = "",
  )
  lateinit var edp7KekUri: String
    private set

  @CommandLine.Option(
    names = ["--edpa-meta-kek-uri"],
    description = ["AWS KMS KEK URI for edpa_meta."],
    defaultValue = "",
  )
  lateinit var edpaMetaKekUri: String
    private set

  @CommandLine.Option(
    names = ["--edpa-meta-aws-role-arn"],
    description = ["AWS role assumed via web-identity federation for edpa_meta."],
    defaultValue = "",
  )
  lateinit var edpaMetaAwsRoleArn: String
    private set

  @CommandLine.Option(
    names = ["--edpa-meta-aws-region"],
    description = ["AWS region of the edpa_meta KMS key."],
    defaultValue = "",
  )
  lateinit var edpaMetaAwsRegion: String
    private set
}

fun main(args: Array<String>) = commandLineMain(::run, args)
