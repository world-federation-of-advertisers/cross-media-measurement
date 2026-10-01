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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling

import com.google.cloud.functions.HttpFunction
import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.EnvVars
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfig
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigs
import org.wfanet.measurement.config.securecomputation.DataWatcherConfig
import org.wfanet.measurement.edpaggregator.service.UploadHealingOperationKey
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.vidlabeling.RawImpressionUploadManifestClassifier
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.CorrectionManifestReader
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.DoneBlobReplayer
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.EvictUploader
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.GrpcHealingOperationStore
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.RawImpressionUploadCorrectionPlanner
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.VidLabelingHealingController
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.VidLabelingHealingControllerEventSink
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperationServiceGrpcKt as InternalUploadHealingOperationServiceGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs

/**
 * Scheduled Cloud Function for automatic VID-labeling correction progression.
 *
 * `HEALING_INTERNAL_API_TARGET` selects the controller-only persistence endpoint, and
 * `HEALING_INTERNAL_API_CERT_HOST` optionally overrides its TLS authority.
 */
class VidLabelingHealingControllerFunction(
  private val runController: suspend () -> Unit = { controller.run() }
) : HttpFunction {
  override fun service(request: HttpRequest, response: HttpResponse) {
    try {
      runBlocking { runController() }
      response.setStatusCode(204)
    } finally {
      EdpaTelemetry.flush()
    }
  }

  companion object {
    init {
      EdpaTelemetry.ensureInitialized()
    }

    private val storage: Storage by lazy { StorageOptions.getDefaultInstance().service }
    private val grpcTelemetry by lazy { GrpcTelemetry.create(Instrumentation.openTelemetry) }
    private val rawApiTarget by lazy { EnvVars.checkNotNullOrEmpty("RAW_IMPRESSION_UPLOAD_TARGET") }
    private val rawApiCertHost by lazy { System.getenv("RAW_IMPRESSION_UPLOAD_CERT_HOST") }
    private val healingInternalApiTarget by lazy {
      EnvVars.checkNotNullOrEmpty("HEALING_INTERNAL_API_TARGET")
    }
    private val healingInternalApiCertHost by lazy {
      System.getenv("HEALING_INTERNAL_API_CERT_HOST")
    }
    private val controlPlaneTarget by lazy { EnvVars.checkNotNullOrEmpty("CONTROL_PLANE_TARGET") }
    private val controlPlaneCertHost by lazy { System.getenv("CONTROL_PLANE_CERT_HOST") }
    private val retention by lazy {
      Duration.ofDays(System.getenv("HEALING_RETENTION_DAYS")?.toLong() ?: 90L)
    }
    private val stallTimeout by lazy {
      Duration.ofMinutes(System.getenv("HEALING_STALL_MINUTES")?.toLong() ?: 60L)
    }

    private val vidLabelingConfigs: List<VidLabelingConfig> by lazy {
      runBlocking {
          EdpAggregatorConfig.getConfigAsProtoMessage(
            EnvVars.checkNotNullOrEmpty("CONFIG_BLOB_KEY"),
            VidLabelingConfigs.getDefaultInstance(),
          )
        }
        .configsList
        .also { require(it.isNotEmpty()) { "VidLabelingConfigs must not be empty" } }
    }
    private val dataWatcherConfig: DataWatcherConfig by lazy {
      runBlocking {
        EdpAggregatorConfig.getConfigAsProtoMessage(
          EnvVars.checkNotNullOrEmpty("DATA_WATCHER_CONFIG_BLOB_KEY"),
          DataWatcherConfig.getDefaultInstance(),
        )
      }
    }

    private val controller: VidLabelingHealingController by lazy {
      val firstConfig = vidLabelingConfigs.first()
      require(
        vidLabelingConfigs.all {
          it.rawImpressionMetadataStorageConnection ==
            firstConfig.rawImpressionMetadataStorageConnection
        }
      ) {
        "Healing controller requires one shared RawImpressionMetadata endpoint"
      }
      val rawChannel =
        VidLabelingFunctionHelpers.createInstrumentedChannel(
          firstConfig.rawImpressionMetadataStorageConnection,
          rawApiTarget,
          rawApiCertHost,
          grpcTelemetry,
        )
      val internalChannel =
        VidLabelingFunctionHelpers.createInstrumentedChannel(
          firstConfig.rawImpressionMetadataStorageConnection,
          healingInternalApiTarget,
          healingInternalApiCertHost,
          grpcTelemetry,
        )
      val uploads = RawImpressionUploadServiceCoroutineStub(rawChannel)
      val files = RawImpressionUploadFileServiceCoroutineStub(rawChannel)
      val modelLines = RawImpressionUploadModelLineServiceCoroutineStub(rawChannel)
      val ranks = RankIndexBlobServiceCoroutineStub(rawChannel)
      val impressionMetadata = ImpressionMetadataServiceCoroutineStub(rawChannel)
      val candidates = RawImpressionUploadCorrectionCandidateServiceCoroutineStub(rawChannel)
      val operations = UploadHealingOperationServiceCoroutineStub(rawChannel)
      val operationStore =
        GrpcHealingOperationStore(
          operations,
          InternalUploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(
            internalChannel
          ),
        )
      val watchers =
        vidLabelingConfigs.associate { config ->
          val controlPlaneChannel =
            VidLabelingFunctionHelpers.createInstrumentedChannel(
              config.controlPlaneConnection,
              controlPlaneTarget,
              controlPlaneCertHost,
              grpcTelemetry,
            )
          config.dataProvider to
            DataWatcher(
              WorkItemsCoroutineStub(controlPlaneChannel),
              dataWatcherConfig.watchedPathsList,
            )
        }
      val evictors =
        vidLabelingConfigs.associate { config ->
          config.dataProvider to
            EvictUploader(
              uploads,
              modelLines,
              ranks,
              files,
              impressionMetadata,
              labeledOutputPrefix(config),
              ::deleteBlob,
            )
        }
      return@lazy VidLabelingHealingController(
        vidLabelingConfigs.map {
          VidLabelingHealingController.DataProviderConfig(
            it.dataProvider,
            labeledOutputPrefix(it),
            retention,
            stallTimeout,
          )
        },
        candidates,
        operationStore,
        uploads,
        files,
        modelLines,
        ranks,
        plannerFactory = { config ->
          RawImpressionUploadCorrectionPlanner(
            planCorrection = { owners, cutoff, operationId ->
              evictors.getValue(config.name).planCorrection(owners, cutoff, operationId)
            }
          )
        },
        evictionExecutorFactory = { evictors.getValue(it.name) },
        manifestReader = GcsCorrectionManifestReader(storage),
        doneBlobReplayerFactory = { config ->
          DataWatcherDoneBlobReplayer(watchers.getValue(config.name))
        },
        eventSink = HealingMetrics(),
      )
    }

    private fun labeledOutputPrefix(config: VidLabelingConfig): String {
      require(config.vidLabeledImpressionsStorageParams.hasGcs())
      return "gs://${config.vidLabeledImpressionsStorageParams.gcs.bucketName}/" +
        config.edpImpressionPath.trim('/')
    }

    private fun deleteBlob(uri: String): Boolean {
      val parsed = GcsUri.parse(uri)
      return storage.delete(BlobId.of(parsed.bucket, parsed.key))
    }
  }
}

internal class GcsCorrectionManifestReader(private val storage: Storage) :
  CorrectionManifestReader {
  override suspend fun read(
    doneBlobUri: String,
    doneBlobGeneration: Long,
  ): Collection<RawImpressionUploadManifestClassifier.File> {
    val done = GcsUri.parse(doneBlobUri)
    val current = checkNotNull(storage.get(done.bucket, done.key)) { "$doneBlobUri does not exist" }
    check(current.generation == doneBlobGeneration) { "$doneBlobUri generation changed" }
    val prefix =
      done.key.substringBeforeLast('/', missingDelimiterValue = "").let {
        if (it.isEmpty()) "" else "$it/"
      }
    val manifest =
      storage
        .list(done.bucket, Storage.BlobListOption.prefix(prefix))
        .iterateAll()
        .asSequence()
        .filterNot { it.name.substringAfterLast('/').equals("done", ignoreCase = true) }
        .map {
          RawImpressionUploadManifestClassifier.File(
            "gs://${done.bucket}/${it.name}",
            it.generation,
          )
        }
        .toList()
    val rechecked =
      checkNotNull(storage.get(done.bucket, done.key)) { "$doneBlobUri does not exist" }
    check(rechecked.generation == doneBlobGeneration) { "$doneBlobUri generation changed" }
    return manifest
  }
}

internal class DataWatcherDoneBlobReplayer(private val dataWatcher: DataWatcher) :
  DoneBlobReplayer {
  override suspend fun replay(request: DoneBlobReplayer.Request) {
    val operationId =
      requireNotNull(UploadHealingOperationKey.fromName(request.uploadHealingOperation))
        .uploadHealingOperationId
    dataWatcher.receivePath(
      request.doneBlobUri,
      mapOf(
        DataWatcher.GENERATION_METADATA_KEY to request.doneBlobGeneration.toString(),
        WatchedBlobs.OVERRIDE_MODEL_LINES_KEY to request.cmmsModelLines.joinToString(","),
        WatchedBlobs.RECOVERY_SOURCE_UPLOAD_KEY to request.sourceRawImpressionUpload,
        WatchedBlobs.EVICTION_OPERATION_ID_KEY to operationId,
      ),
    )
  }
}

private class HealingMetrics : VidLabelingHealingControllerEventSink {
  private val counters: Map<VidLabelingHealingControllerEventSink.Event.Type, LongCounter> =
    VidLabelingHealingControllerEventSink.Event.Type.entries.associateWith { type ->
      Instrumentation.meter
        .counterBuilder("edpa.vid_labeling.healing.${type.name.lowercase()}")
        .setDescription("VID-labeling healing controller ${type.name.lowercase()} events")
        .build()
    }

  override fun record(event: VidLabelingHealingControllerEventSink.Event) {
    counters.getValue(event.type).add(1L, Attributes.of(DATA_PROVIDER, event.dataProvider))
  }

  companion object {
    private val DATA_PROVIDER = AttributeKey.stringKey("data_provider")
  }
}

internal data class GcsUri(val bucket: String, val key: String) {
  companion object {
    fun parse(uri: String): GcsUri {
      require(uri.startsWith("gs://")) { "Expected a gs:// URI" }
      val value = uri.removePrefix("gs://")
      val separator = value.indexOf('/')
      require(separator > 0 && separator < value.lastIndex) { "Malformed GCS URI" }
      return GcsUri(value.substring(0, separator), value.substring(separator + 1))
    }
  }
}
