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

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.crypto.tink.KmsClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.edpaggregator.BaseTeeAppRunner
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.rawimpressions.LabelerInputMapper
import org.wfanet.measurement.edpaggregator.rawimpressions.ParquetDigestedEvent
import org.wfanet.measurement.edpaggregator.runBlockingWithTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams.StorageParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.storage.ParquetEncryptionConfig
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient
import picocli.CommandLine

/**
 * CLI entry point for the [VidLabelerApp] Phase-2 TEE container.
 *
 * Pulls EDPA mTLS material from Secret Manager, builds per-`DataProvider` [KmsClient]s from the
 * EDPA-level `event-data-provider-configs.textproto` via Workload Identity Federation, opens a
 * mutual-TLS channel to the Secure Computation control plane for `WorkItem` / `WorkItemAttempt`
 * reads and a mutual-TLS channel to the EDP Aggregator metadata-storage public API for the
 * `VidLabelingJob`, `RawImpressionUploadModelLine`, `RawImpressionUploadFile`, and `RankIndexBlob`
 * services, subscribes to the Phase-2 Pub/Sub queue, wires the production storage / model /
 * converter seams, and hands everything to [VidLabelerApp.run].
 */
@CommandLine.Command(name = "vid_labeler_app_runner")
class VidLabelerAppRunner : BaseTeeAppRunner() {

  private val getStorageConfig: (StorageParams) -> StorageConfig = { storageParams ->
    StorageConfig(projectId = storageParams.gcsProjectId)
  }

  override fun run() {
    saveCommonEdpaCerts()
    val kmsClients: Map<String, KmsClient> = buildKmsClientsMap()

    val pubSubClient = DefaultGooglePubSubClient()
    val queueSubscriber = createQueueSubscriber(pubSubClient)

    val secureComputationPublicChannel = buildSecureComputationPublicChannel()
    val workItemsClient = WorkItemsGrpcKt.WorkItemsCoroutineStub(secureComputationPublicChannel)
    val workItemAttemptsClient =
      WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub(secureComputationPublicChannel)

    val metadataStorageChannel = buildMetadataStoragePublicChannel()
    val vidLabelingJobsClient = VidLabelingJobServiceCoroutineStub(metadataStorageChannel)
    val rawImpressionUploadModelLinesClient =
      RawImpressionUploadModelLineServiceCoroutineStub(metadataStorageChannel)
    val rawImpressionUploadFilesClient =
      RawImpressionUploadFileServiceCoroutineStub(metadataStorageChannel)
    val rankIndexBlobsClient = RankIndexBlobServiceCoroutineStub(metadataStorageChannel)

    val vidLabelerApp =
      VidLabelerApp(
        subscriptionId = subscriptionId,
        queueSubscriber = queueSubscriber,
        parser = WorkItem.parser(),
        workItemsClient = workItemsClient,
        workItemAttemptsClient = workItemAttemptsClient,
        kmsClients = kmsClients,
        getStorageConfig = getStorageConfig,
        vidLabelingJobsStub = vidLabelingJobsClient,
        rawImpressionUploadModelLinesStub = rawImpressionUploadModelLinesClient,
        rankIndexBlobsStub = rankIndexBlobsClient,
        rawImpressionUploadFilesStub = rawImpressionUploadFilesClient,
        buildParquetStorageClient = { cfg, kms ->
          ParquetStorageClient(
            conf = productionConfiguration(requireNotNull(cfg.projectId)),
            // RawImpressionSource hands ParquetStorageClient absolute gs:// URIs, so the root is
            // only the FileSystem selector; the read never resolves against it.
            rootPath = Path(GCS_ROOT_URI),
            encryptionConfig = ParquetEncryptionConfig(kmsProvider = { kms }),
          )
        },
        buildVidRankMapStorageClient = { cfg -> buildSelectedStorageClient(cfg, GCS_ROOT_URI) },
        loadAssigner = { modelBlobUri ->
          val blobUri = SelectedStorageClient.parseBlobUri(modelBlobUri)
          val modelBlob =
            SelectedStorageClient(blobUri, /* rootDirectory= */ null, googleProjectId)
              .getBlob(blobUri.key) ?: error("Compiled-model blob not found: $modelBlobUri")
          VirtualPeopleVidAssigner.fromCompiledNodeBlob(modelBlob.read().flatten())
        },
        impressionConverter = ParquetImpressionConverter,
      )

    runBlockingWithTelemetry { vidLabelerApp.run() }
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(VidLabelerAppRunner(), args)

    /** Root URI selecting the GCS-backed [SelectedStorageClient] for absolute `gs://` blob URIs. */
    private const val GCS_ROOT_URI = "gs://"

    /**
     * Hadoop [Configuration] selecting the GCS connector with Compute-Engine (VM SA) auth, for the
     * `ParquetStorageClient` reading raw impressions.
     */
    private fun productionConfiguration(projectId: String): Configuration =
      Configuration().apply {
        set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        set("fs.gs.auth.type", "COMPUTE_ENGINE")
        set("fs.gs.project.id", projectId)
      }

    /**
     * Builds a [StorageClient] for [rootUri], passing the optional [StorageConfig.projectId]
     * through to the underlying GCS client.
     */
    private fun buildSelectedStorageClient(cfg: StorageConfig, rootUri: String): StorageClient =
      SelectedStorageClient(
        SelectedStorageClient.parseBlobUri(rootUri),
        cfg.rootDirectory,
        cfg.projectId,
      )
  }
}

/**
 * Production [ImpressionConverter]: projects a raw-impression Parquet row into a [LabelerInput] via
 * the model line's `labeler_input_field_mapping`, and derives the labeling-relevant fields the
 * current schema + mapping pin down (`event_time_micros` from `LabelerInput.timestamp_usec`).
 *
 * TODO(world-federation-of-advertisers/cross-media-measurement#3913): once the raw-impression
 *   Parquet schema (#3954) is finalized, source `event_group_reference_id`, the `event` template
 *   payload (via `event_template_field_mapping`), and `entity_keys` (from `EventGroup` metadata)
 *   from the row + footer. Until then `convert` throws when it reaches the schema-dependent fields,
 *   since [ConvertedImpression] requires a non-empty `entity_keys` and there is no schema to read
 *   them from. This is the single documented residual of the Phase-2 wiring.
 */
object ParquetImpressionConverter : ImpressionConverter {
  override fun convert(
    event: ParquetDigestedEvent,
    config: VidLabelerParams.ModelLineConfig,
  ): ConvertedImpression? {
    // Real: project the LabelerInput via the per-model-line field mapping.
    val labelerInput = LabelerInputMapper(config.labelerInputFieldMappingMap).project(event.row)
    // Real: event time is LabelerInput.timestamp_usec (mapped via labeler_input_field_mapping).
    val eventTimeMicros: Long = labelerInput.timestampUsec

    // TODO(world-federation-of-advertisers/cross-media-measurement#3913): derive
    //   eventGroupReferenceId, the event template payload, and entityKeys from the finalized
    //   raw-impression schema (event_template_field_mapping + EventGroup metadata). entityKeys is
    //   REQUIRED by ConvertedImpression, so this throws until the schema lands.
    val entityKeys: List<org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression.EntityKey> =
      TODO(
        "Source entityKeys from EventGroup metadata once the raw-impression Parquet schema is " +
          "finalized (world-federation-of-advertisers/cross-media-measurement#3913)"
      )

    return ConvertedImpression(
      labelerInput = labelerInput,
      eventTimeMicros = eventTimeMicros,
      eventGroupReferenceId =
        TODO(
          "Source eventGroupReferenceId once the raw-impression Parquet schema is finalized " +
            "(world-federation-of-advertisers/cross-media-measurement#3913)"
        ),
      event =
        TODO(
          "Build the event template payload from event_template_field_mapping once the " +
            "raw-impression Parquet schema is finalized " +
            "(world-federation-of-advertisers/cross-media-measurement#3913)"
        ),
      entityKeys = entityKeys,
    )
  }
}
