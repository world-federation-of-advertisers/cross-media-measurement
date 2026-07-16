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

import io.grpc.Channel
import io.grpc.ClientInterceptors
import io.grpc.ManagedChannel
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.io.File
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.config.edpaggregator.TransportLayerSecurityParams as ConfigTransportLayerSecurityParams
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfig
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.subpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.transportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams

/** Channel cache key using TLS params, target, and optional hostname override. */
data class ChannelKey(
  val tls: ConfigTransportLayerSecurityParams,
  val target: String,
  val hostName: String?,
)

/**
 * Shared helpers for the VID labeling Cloud Functions.
 *
 * Holds the gRPC channel cache and the [VidLabelerParams]/[SubpoolAssignerParams] template builders
 * used identically by [VidLabelingDispatcherFunction] and [VidLabelingMonitorFunction].
 */
object VidLabelingFunctionHelpers {
  private val logger: Logger = Logger.getLogger(this::class.java.name)
  private const val DEFAULT_CHANNEL_SHUTDOWN_DURATION_SECONDS: Long = 3L
  private val channelShutdownDuration =
    Duration.ofSeconds(
      System.getenv("CHANNEL_SHUTDOWN_DURATION_SECONDS")?.toLong()
        ?: DEFAULT_CHANNEL_SHUTDOWN_DURATION_SECONDS
    )

  private val channelCache = ConcurrentHashMap<ChannelKey, ManagedChannel>()

  private fun createPublicChannel(
    connectionParams: ConfigTransportLayerSecurityParams,
    target: String,
    hostName: String?,
  ): ManagedChannel {
    val signingCerts =
      SigningCerts.fromPemFiles(
        certificateFile = checkNotNull(File(connectionParams.certFilePath)),
        privateKeyFile = checkNotNull(File(connectionParams.privateKeyFilePath)),
        trustedCertCollectionFile = checkNotNull(File(connectionParams.certCollectionFilePath)),
      )
    return buildMutualTlsChannel(target, signingCerts, hostName)
      .withShutdownTimeout(channelShutdownDuration)
  }

  private fun getOrCreateChannel(
    connectionParams: ConfigTransportLayerSecurityParams,
    target: String,
    hostName: String?,
  ): ManagedChannel {
    val channelKey = ChannelKey(connectionParams, target, hostName)
    return channelCache.computeIfAbsent(channelKey) {
      logger.info("Creating new channel for $target")
      createPublicChannel(connectionParams, target, hostName)
    }
  }

  fun createInstrumentedChannel(
    connectionParams: ConfigTransportLayerSecurityParams,
    target: String,
    hostName: String?,
    grpcTelemetry: GrpcTelemetry,
  ): Channel {
    val channel = getOrCreateChannel(connectionParams, target, hostName)
    return ClientInterceptors.intercept(channel, grpcTelemetry.newClientInterceptor())
  }

  fun convertModelLineConfigs(
    configModelLines: Map<String, VidLabelingConfig.ModelLineConfig>
  ): Map<String, VidLabelerParams.ModelLineConfig> {
    return configModelLines.mapValues { (_, configModelLine) ->
      VidLabelerParamsKt.modelLineConfig {
        labelerInputFieldMapping.addAll(configModelLine.labelerInputFieldMappingList)
        eventTemplateFieldMapping.putAll(configModelLine.eventTemplateFieldMappingMap)
        eventTemplateDescriptorBlobUri = configModelLine.eventTemplateDescriptorBlobUri
        eventTemplateType = configModelLine.eventTemplateType
        requiredEntityKeyFieldMapping.putAll(configModelLine.requiredEntityKeyFieldMappingMap)
        optionalEntityKeyFieldMapping.putAll(configModelLine.optionalEntityKeyFieldMappingMap)
      }
    }
  }

  fun buildVidLabelerParamsTemplate(config: VidLabelingConfig): VidLabelerParams {
    require(config.rawImpressionsStorageParams.hasGcs()) {
      "VidLabelingConfig raw_impressions_storage_params must use GCS"
    }
    require(config.vidLabeledImpressionsStorageParams.hasGcs()) {
      "VidLabelingConfig vid_labeled_impressions_storage_params must use GCS"
    }
    require(config.edpImpressionPath.isNotEmpty()) {
      "VidLabelingConfig.edp_impression_path is required"
    }

    return vidLabelerParams {
      dataProvider = config.dataProvider
      vidLabeledImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = config.vidLabeledImpressionsStorageParams.gcs.projectId
          // Per-EDP folder segment so each EDP's labeled output lives under its own
          // folder: gs://<bucket>/<edp_impression_path>/model-line/<id>/<date>/. Required,
          // and must match this EDP's DataAvailabilitySyncConfig edp_impression_path so the
          // writer and the registrar agree on the location.
          impressionsBlobPrefix =
            "gs://${config.vidLabeledImpressionsStorageParams.gcs.bucketName}" +
              "/${config.edpImpressionPath}"
        }
      rawImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = config.rawImpressionsStorageParams.gcs.projectId
          impressionsBlobPrefix = "gs://${config.rawImpressionsStorageParams.gcs.bucketName}"
        }
      vidRepoConnection = transportLayerSecurityParams {
        clientCertResourcePath = config.vidRepoConnection.certFilePath
        clientPrivateKeyResourcePath = config.vidRepoConnection.privateKeyFilePath
      }
      // The compiled model lives in its own Cloud Storage project. Optional on VidLabelingConfig
      // (only EDPs that actually label need it); when set, thread it onto every WorkItem so the
      // TEE reads the model from its own project on both the memoized and non-memoized paths.
      if (config.modelStorageParams.hasGcs()) {
        modelStorageParams =
          VidLabelerParamsKt.storageParams {
            gcsProjectId = config.modelStorageParams.gcs.projectId
            impressionsBlobPrefix = "gs://${config.modelStorageParams.gcs.bucketName}"
          }
      }
    }
  }

  /**
   * Builds the template [SubpoolAssignerParams] carrying the storage + connection fields shared by
   * every memoized Phase-0 WorkItem. The per-shard fields (model line, shard index, active window,
   * pool assignment job) are filled in by the sequencer.
   */
  fun buildSubpoolAssignerParamsTemplate(config: VidLabelingConfig): SubpoolAssignerParams {
    require(config.rawImpressionsStorageParams.hasGcs()) {
      "VidLabelingConfig raw_impressions_storage_params must use GCS"
    }
    require(config.vidLabeledImpressionsStorageParams.hasGcs()) {
      "VidLabelingConfig vid_labeled_impressions_storage_params must use GCS"
    }
    // vid_rank_map/subpool_map storage are consumed only by the memoized Phase-0 path and are
    // therefore optional in VidLabelingConfig; validate them only when set. An EDP whose model
    // lines are all non-memoized may omit them, and this template is then never consumed.
    if (config.hasVidRankMapStorageParams()) {
      require(config.vidRankMapStorageParams.hasGcs()) {
        "VidLabelingConfig vid_rank_map_storage_params must use GCS"
      }
    }
    if (config.hasSubpoolMapStorageParams()) {
      require(config.subpoolMapStorageParams.hasGcs()) {
        "VidLabelingConfig subpool_map_storage_params must use GCS"
      }
    }
    if (config.hasModelStorageParams()) {
      require(config.modelStorageParams.hasGcs()) {
        "VidLabelingConfig model_storage_params must use GCS"
      }
    }

    return subpoolAssignerParams {
      dataProvider = config.dataProvider
      rawImpressionStorageParams =
        SubpoolAssignerParamsKt.storageParams {
          gcsProjectId = config.rawImpressionsStorageParams.gcs.projectId
          blobPrefix = "gs://${config.rawImpressionsStorageParams.gcs.bucketName}"
        }
      vidLabeledImpressionsStorageParams =
        SubpoolAssignerParamsKt.storageParams {
          gcsProjectId = config.vidLabeledImpressionsStorageParams.gcs.projectId
          blobPrefix = "gs://${config.vidLabeledImpressionsStorageParams.gcs.bucketName}"
        }
      if (config.hasVidRankMapStorageParams()) {
        vidRankMapStorageParams =
          SubpoolAssignerParamsKt.storageParams {
            gcsProjectId = config.vidRankMapStorageParams.gcs.projectId
            blobPrefix = "gs://${config.vidRankMapStorageParams.gcs.bucketName}"
          }
      }
      if (config.hasSubpoolMapStorageParams()) {
        subpoolMapStorageParams =
          SubpoolAssignerParamsKt.storageParams {
            gcsProjectId = config.subpoolMapStorageParams.gcs.projectId
            blobPrefix = "gs://${config.subpoolMapStorageParams.gcs.bucketName}"
          }
      }
      if (config.hasModelStorageParams()) {
        modelStorageParams =
          SubpoolAssignerParamsKt.storageParams {
            gcsProjectId = config.modelStorageParams.gcs.projectId
            blobPrefix = "gs://${config.modelStorageParams.gcs.bucketName}"
          }
      }
      rawImpressionMetadataStorageConnection = transportLayerSecurityParams {
        clientCertResourcePath = config.rawImpressionMetadataStorageConnection.certFilePath
        clientPrivateKeyResourcePath =
          config.rawImpressionMetadataStorageConnection.privateKeyFilePath
      }
      // Forward the bin-packing threshold onto the memoized Phase-0 path so the Phase-1 ranker's
      // last-job-out fan-out bin-packs identically to the non-memoized dispatcher. REQUIRED on
      // SubpoolAssignerParams; SubpoolAssignerApp validates it > 0.
      maxFileBatchSizeBytes = config.maxFileBatchSizeBytes
    }
  }
}
