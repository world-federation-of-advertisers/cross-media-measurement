/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.loadtest.edpaggregator.tools

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.QueryParameterValue
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import com.google.protobuf.TypeRegistry
import com.google.protobuf.timestamp
import com.google.type.interval
import io.grpc.ManagedChannel
import java.io.File
import java.time.Clock
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.logging.Logger
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.measurement.integration.k8s.testing.HighOverlapDatasetManifest
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.measurement.integration.k8s.testing.highOverlapDatasetManifest
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.aws.kms.AwsKmsClientFactory
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.tink.AwsWebIdentityCredentials
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.entityKey
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as eventGroupMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.integration.common.EventGroupConfig
import org.wfanet.measurement.integration.common.HighOverlapDataset
import org.wfanet.measurement.integration.common.ImpressionTestDataConfigs
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import picocli.CommandLine.ArgGroup
import picocli.CommandLine.Command
import picocli.CommandLine.Option

/**
 * Seeds the high overlap synthetic dataset for one ModelLine.
 *
 * Provisions the ModelLine's Population and rollout, uploads the EventGroups, writes the
 * impressions and exports them to BigQuery, writes the `done` blobs, and finally writes the
 * [HighOverlapDatasetManifest]. The manifest is written only once everything else has succeeded, so
 * a failed run leaves the previous one in place.
 *
 * What this writes persists and the dataset is deterministic, so seeding is needed only when the
 * specs change or an environment gains an EDP.
 */
@Command(
  name = "SeedHighOverlapDataset",
  description = ["Seeds the high overlap synthetic dataset for one ModelLine."],
)
class SeedHighOverlapDataset : Runnable {
  class AwsKmsFlags {
    @Option(
      names = ["--aws-kms-kek-uri"],
      description = ["AWS KMS KEK URI for EDPs encrypting with AWS KMS."],
      required = true,
    )
    lateinit var kekUri: String
      private set

    @Option(
      names = ["--aws-role-arn"],
      description = ["AWS role assumed via web-identity federation."],
      required = true,
    )
    lateinit var roleArn: String
      private set

    @Option(names = ["--aws-region"], description = ["AWS region of the KMS key."], required = true)
    lateinit var region: String
      private set

    @Option(
      names = ["--aws-web-identity-token-file"],
      description = ["Path to the web identity token file."],
      required = true,
    )
    lateinit var webIdentityTokenFile: String
      private set
  }

  @Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target of the Kingdom public API server."],
    required = true,
  )
  private lateinit var kingdomPublicApiTarget: String

  @Option(
    names = ["--kingdom-public-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Kingdom public API server's TLS certificate.",
        "If empty, standard TLS DNS-ID derivation is used.",
      ],
    defaultValue = "",
  )
  private lateinit var kingdomPublicApiCertHost: String

  @Option(
    names = ["--kingdom-trusted-certs-file"],
    description = ["PEM file of trusted root certificates for the Kingdom public API."],
    required = true,
  )
  private lateinit var kingdomTrustedCertsFile: File

  @Option(
    names = ["--pdp-tls-cert-file"],
    description = ["TLS client certificate of --population-data-provider."],
    required = true,
  )
  private lateinit var pdpTlsCertFile: File

  @Option(
    names = ["--pdp-tls-key-file"],
    description = ["TLS client private key of --population-data-provider."],
    required = true,
  )
  private lateinit var pdpTlsKeyFile: File

  @Option(
    names = ["--model-provider-tls-cert-file"],
    description = ["TLS client certificate of the ModelProvider that owns --model-line."],
    required = true,
  )
  private lateinit var modelProviderTlsCertFile: File

  @Option(
    names = ["--model-provider-tls-key-file"],
    description = ["TLS client private key of the ModelProvider that owns --model-line."],
    required = true,
  )
  private lateinit var modelProviderTlsKeyFile: File

  @Option(
    names = ["--population-data-provider"],
    description = ["Resource name of the DataProvider that owns the high overlap Population."],
    required = true,
  )
  private lateinit var populationDataProvider: String

  @Option(
    names = ["--model-line"],
    description = ["Resource name of the high overlap ModelLine."],
    required = true,
  )
  private lateinit var modelLine: String

  @Option(
    names = ["--measurement-consumer"],
    description = ["Resource name of the MeasurementConsumer the EventGroups belong to."],
    required = true,
  )
  private lateinit var measurementConsumer: String

  @Option(
    names = ["--storage-bucket"],
    description = ["Google Cloud Storage bucket the EDP Aggregator reads from."],
    required = true,
  )
  private lateinit var storageBucket: String

  @Option(
    names = ["--google-project-id"],
    description = ["Google Cloud project of the storage bucket and BigQuery dataset."],
    required = true,
  )
  private lateinit var googleProjectId: String

  @Option(
    names = ["--google-cloud-kms-kek-uri"],
    description = ["Google Cloud KMS KEK URI for EDPs encrypting with Google Cloud KMS."],
    required = true,
  )
  private lateinit var googleCloudKmsKekUri: String

  @ArgGroup(exclusive = false, multiplicity = "0..1", heading = "AWS KMS flags%n")
  private var awsKmsFlags: AwsKmsFlags? = null

  @Option(
    names = ["--bigquery-dataset"],
    description = ["BigQuery dataset of the impressions table."],
    defaultValue = "high_overlap",
  )
  private lateinit var bigQueryDataset: String

  @Option(
    names = ["--bigquery-table"],
    description = ["BigQuery table the impressions are exported to."],
    defaultValue = "impressions",
  )
  private lateinit var bigQueryTable: String

  private val modelLineId: String by lazy {
    requireNotNull(ModelLineKey.fromName(modelLine)) {
        "--model-line must be a ModelLine resource name: $modelLine"
      }
      .modelLineId
  }

  private val storage: Storage by lazy {
    StorageOptions.newBuilder().setProjectId(googleProjectId).build().service
  }

  override fun run() {
    val fullConfig: ImpressionTestDataConfig =
      parseTextProto(
        ImpressionTestDataConfigs.resolveSpecPath(HighOverlapDataset.CONFIG_RESOURCE_PATH),
        ImpressionTestDataConfig.getDefaultInstance(),
      )
    val populationSpec: PopulationSpec =
      parseTextProto(
        ImpressionTestDataConfigs.resolveSpecPath(fullConfig.populationSpecResourcePath),
        PopulationSpec.getDefaultInstance(),
        TypeRegistry.newBuilder().add(Common.getDescriptor()).build(),
      )
    val kekClientsByEdp: Map<String, HighOverlapImpressionsWriter.KekClient> = buildKekClients()
    val config: ImpressionTestDataConfig =
      fullConfig
        .toBuilder()
        .clearEventGroups()
        .addAllEventGroups(fullConfig.eventGroupsList.filter { it.edpName in kekClientsByEdp })
        .build()
    val seededEdps: Set<String> = config.eventGroupsList.map { it.edpName }.toSortedSet()
    val datesByImpressionPath: Map<String, Set<LocalDate>> =
      HighOverlapDataset.datesByImpressionPath(config)
    val specFingerprint: String =
      HighOverlapDataset.fingerprint(HighOverlapDataset.CONFIG_RESOURCE_PATH)
    logger.info("Seeding $seededEdps for $modelLine with specs $specFingerprint")

    provisionModelResources(populationSpec, datesByImpressionPath.values.flatten().min())

    val bigQuery: BigQuery =
      BigQueryOptions.newBuilder().setProjectId(googleProjectId).build().service
    val exporter =
      HighOverlapImpressionExporter(
        bigQuery = bigQuery,
        writeClient = BigQueryWriteClient.create(),
        projectId = googleProjectId,
        datasetId = bigQueryDataset,
        tableId = bigQueryTable,
        modelLine = modelLine,
        specFingerprint = specFingerprint,
      )
    runBlocking {
      uploadEventGroups(config)
      exporter.use {
        it.deleteExistingRows()
        HighOverlapImpressionsWriter(
            config = config,
            populationSpec = populationSpec,
            bucket = storageBucket,
            modelLine = modelLine,
            kmsClientsByEdp = kekClientsByEdp,
            exporter = it,
          )
          .write()
      }
      writeDoneBlobs(datesByImpressionPath)

      val exportedRowCount = exportedRowCount(bigQuery)
      check(exportedRowCount == exporter.rowCount) {
        "BigQuery holds $exportedRowCount row(s) for $modelLine, but ${exporter.rowCount} " +
          "impression(s) were written"
      }
      writeManifest(
        highOverlapDatasetManifest {
          this.specFingerprint = specFingerprint
          this.seededEdps += seededEdps
          modelLine = this@SeedHighOverlapDataset.modelLine
          generateTime = timestamp { seconds = Clock.systemUTC().instant().epochSecond }
          impressionRowCount = exporter.rowCount
        }
      )
    }
  }

  private fun buildKekClients(): Map<String, HighOverlapImpressionsWriter.KekClient> {
    // A single Google Cloud KMS KEK is shared by these EDPs to avoid a separate KMS for each.
    val googleCloudKmsClient: KmsClient = GcpKmsClient().withDefaultCredentials()
    val googleCloudKekClient =
      HighOverlapImpressionsWriter.KekClient(googleCloudKmsClient, googleCloudKmsKekUri)
    val awsKekClient: HighOverlapImpressionsWriter.KekClient? =
      awsKmsFlags?.let { flags ->
        val kmsClient: KmsClient =
          AwsKmsClientFactory()
            .getKmsClient(
              AwsWebIdentityCredentials(
                roleArn = flags.roleArn,
                webIdentityTokenFilePath = flags.webIdentityTokenFile,
                roleSessionName = AWS_ROLE_SESSION_NAME,
                region = flags.region,
              )
            )
        HighOverlapImpressionsWriter.KekClient(kmsClient, flags.kekUri)
      }
    return buildMap {
      for (edpName in GOOGLE_CLOUD_KMS_EDPS) {
        put(edpName, googleCloudKekClient)
      }
      if (awsKekClient != null) {
        for (edpName in AWS_KMS_EDPS) {
          put(edpName, awsKekClient)
        }
      }
    }
  }

  private fun provisionModelResources(
    populationSpec: PopulationSpec,
    earliestEventDate: LocalDate,
  ) {
    val pdpChannel: ManagedChannel = buildChannel(pdpTlsCertFile, pdpTlsKeyFile)
    val modelProviderChannel: ManagedChannel =
      buildChannel(modelProviderTlsCertFile, modelProviderTlsKeyFile)
    try {
      HighOverlapModelResources(
          populationSpec = populationSpec,
          populationDataProvider = populationDataProvider,
          modelLineName = modelLine,
          earliestEventDate = earliestEventDate,
          pdpChannel = pdpChannel,
          mpChannel = modelProviderChannel,
        )
        .provision()
    } finally {
      pdpChannel.shutdown()
      modelProviderChannel.shutdown()
    }
  }

  private fun buildChannel(certFile: File, privateKeyFile: File): ManagedChannel {
    val signingCerts = SigningCerts.fromPemFiles(certFile, privateKeyFile, kingdomTrustedCertsFile)
    return buildMutualTlsChannel(
      kingdomPublicApiTarget,
      signingCerts,
      kingdomPublicApiCertHost.ifEmpty { null },
    )
  }

  /** Uploads each EDP's high overlap EventGroups and waits for `EventGroupSync` to pick them up. */
  private suspend fun uploadEventGroups(config: ImpressionTestDataConfig) {
    val allEventGroups: List<EventGroup> =
      buildEventGroups(ImpressionTestDataConfigs.toEventGroupMap(config))
    for ((edpName, referenceIds) in HighOverlapDataset.eventGroupRefIdsByEdp(config)) {
      // EventGroupSync always writes its output to the map blob fixed per EDP in its config.
      val objectMapKey = "$edpName/event-groups-map/$edpName-event-group.binpb"
      val objectKey = "$edpName/event-groups/$edpName-high-overlap-event-group.binpb"
      storage.delete(storageBucket, objectMapKey)

      val blobUri = SelectedStorageClient.parseBlobUri("gs://$storageBucket/$objectKey")
      MesosRecordIoStorageClient(
          SelectedStorageClient(
            blobUri = blobUri,
            rootDirectory = null,
            projectId = googleProjectId,
          )
        )
        .writeBlob(
          objectKey,
          allEventGroups
            .filter { it.eventGroupReferenceId in referenceIds }
            .asFlow()
            .map { it.toByteString() },
        )

      withTimeout(EVENT_GROUP_SYNC_TIMEOUT) {
        while (storage.get(storageBucket, objectMapKey) == null) {
          logger.info("Waiting on EventGroupSync for $edpName...")
          delay(EVENT_GROUP_SYNC_POLLING_INTERVAL)
        }
      }
    }
    logger.info("High overlap EventGroup sync completed.")
  }

  private fun buildEventGroups(eventGroupMap: Map<String, EventGroupConfig>): List<EventGroup> {
    return eventGroupMap.flatMap { (referenceId, config) ->
      when (config) {
        // EventGroupSync filters its Kingdom listing by entity type, so an EventGroup without an
        // entity key would be re-created on every sync.
        is EventGroupConfig.LegacySpec ->
          error("high overlap event group $referenceId has no entity key")
        // The Kingdom enforces uniqueness on both reference ID and entity key, so there is one
        // EventGroup per entity key spanning every date spec.
        is EventGroupConfig.MultiEntityKey ->
          config.entityKeySpecs.map { entityKeySpec ->
            val dateRanges = entityKeySpec.spec.dateSpecsList.map { it.dateRange }
            val startTime =
              dateRanges.minOf { it.start.toLocalDate() }.atStartOfDay(ZoneOffset.UTC).toInstant()
            // Both bounds are exclusive: the date spec's and the interval's.
            val endTime =
              dateRanges
                .maxOf { it.endExclusive.toLocalDate() }
                .atStartOfDay(ZoneOffset.UTC)
                .toInstant()
            eventGroup {
              eventGroupReferenceId =
                "${entityKeySpec.entityKey.entityType}-${entityKeySpec.entityKey.entityId}"
              measurementConsumer = this@SeedHighOverlapDataset.measurementConsumer
              dataAvailabilityInterval = interval {
                this.startTime = timestamp { seconds = startTime.epochSecond }
                this.endTime = timestamp { seconds = endTime.epochSecond }
              }
              eventGroupMetadata = eventGroupMetadata {
                adMetadata = adMetadata {
                  campaignMetadata = campaignMetadata {
                    brand = "some-brand"
                    campaign = "some-campaign"
                  }
                }
                entityKeySpec.entityMetadata?.let { entityMetadata = it }
              }
              entityKey = entityKey {
                entityType = entityKeySpec.entityKey.entityType
                entityId = entityKeySpec.entityKey.entityId
              }
              // Every segment alternates media per stripe.
              mediaTypes += listOf(MediaType.VIDEO, MediaType.DISPLAY)
            }
          }
      }
    }
  }

  /**
   * Writes the `done` blob for each date of impressions, marking that date's impressions as
   * complete and available.
   */
  private suspend fun writeDoneBlobs(datesByImpressionPath: Map<String, Set<LocalDate>>) {
    val paths: List<String> =
      datesByImpressionPath.flatMap { (impressionPath, dates) ->
        dates.map { date ->
          "gs://$storageBucket/$impressionPath/model-line/$modelLineId/$date/done"
        }
      }
    logger.info("Creating ${paths.size} high overlap done blob(s)...")
    for (path in paths) {
      val doneBlobUri = SelectedStorageClient.parseBlobUri(path)
      val storageClient =
        SelectedStorageClient(
          blobUri = doneBlobUri,
          rootDirectory = null,
          projectId = googleProjectId,
        )
      storageClient.getBlob(doneBlobUri.key)?.delete()
      storageClient.writeBlob(doneBlobUri.key, emptyFlow())
    }
  }

  /** Number of rows BigQuery holds for [modelLine]. */
  private fun exportedRowCount(bigQuery: BigQuery): Long {
    val query =
      QueryJobConfiguration.newBuilder(
          "SELECT COUNT(*) AS row_count FROM `$bigQueryDataset.$bigQueryTable` " +
            "WHERE model_line = @model_line"
        )
        .addNamedParameter("model_line", QueryParameterValue.string(modelLine))
        .build()
    return bigQuery.query(query).iterateAll().first().get("row_count").longValue
  }

  private suspend fun writeManifest(manifest: HighOverlapDatasetManifest) {
    val key = HighOverlapDataset.manifestBlobKey(modelLineId)
    val blobUri = SelectedStorageClient.parseBlobUri("gs://$storageBucket/$key")
    val storageClient =
      SelectedStorageClient(blobUri = blobUri, rootDirectory = null, projectId = googleProjectId)
    storageClient.getBlob(key)?.delete()
    storageClient.writeBlob(key, listOf(manifest.toByteString()).asFlow())
    logger.info("Wrote dataset manifest ${manifest.specFingerprint} for $modelLine")
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /** EDPs whose impressions are encrypted with a Google Cloud KMS KEK. */
    private val GOOGLE_CLOUD_KMS_EDPS: Set<String> =
      setOf("edp7", "edpa_video_pub", "edpa_retail_pub")

    /** EDPs whose impressions are encrypted with an AWS KMS KEK. */
    private val AWS_KMS_EDPS: Set<String> = setOf("edpa_meta")

    private const val AWS_ROLE_SESSION_NAME = "seed-high-overlap-dataset"

    private val EVENT_GROUP_SYNC_TIMEOUT = 5.minutes
    private val EVENT_GROUP_SYNC_POLLING_INTERVAL = 5.seconds
  }
}

fun main(args: Array<String>) = commandLineMain(SeedHighOverlapDataset(), args)
