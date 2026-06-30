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

package org.wfanet.measurement.edpaggregator

import com.google.crypto.tink.KmsClient
import com.google.protobuf.ByteString
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.ParquetEncryptionConfig
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Base [BaseTeeAppRunner] for the VID Labeling phase TEE apps (SubpoolAssigner / VidRankBuilder /
 * VidLabeler). Holds the storage-client, Parquet, compiled-model, and per-EDP KEK wiring shared by
 * the phase runners, keyed on common types ([StorageConfig], [ByteString], dataProvider) so each
 * phase's own *Params.StorageParams mapping stays in its runner.
 */
abstract class BaseVidLabelingTeeAppRunner(
  private val storageRootUri: String = "gs://",
  private val hadoopConfigurationFor: (StorageConfig) -> Configuration,
) : BaseTeeAppRunner() {

  /** The per-EDP KEK URI ("the EDP's KMS key") from [EventDataProviderConfig.kms_config]. */
  protected fun kekUriFor(dataProvider: String): String {
    val edpConfig =
      edpsConfig.eventDataProviderConfigList.firstOrNull { it.dataProvider == dataProvider }
    require(edpConfig != null) { "No EventDataProviderConfig for $dataProvider" }
    val kekUri = edpConfig.kmsConfig.kekUri
    require(kekUri.isNotEmpty()) { "kms_config.kek_uri must be set for $dataProvider" }
    return kekUri
  }

  /**
   * A bucket-rooted GCS [SelectedStorageClient] for [cfg] (resolves absolute gs:// URIs). The
   * concrete type also satisfies `ConditionalOperationStorageClient` consumers (e.g. the
   * subpool-map / rank-map stores).
   */
  protected fun buildStorageClient(cfg: StorageConfig): SelectedStorageClient =
    SelectedStorageClient(
      SelectedStorageClient.parseBlobUri(storageRootUri),
      cfg.rootDirectory,
      cfg.projectId,
    )

  /** A [ParquetStorageClient] for raw-impression reads, PME-decrypting via [kms]. */
  protected fun buildParquetStorageClient(
    cfg: StorageConfig,
    kms: KmsClient,
  ): ParquetStorageClient =
    ParquetStorageClient(
      conf = hadoopConfigurationFor(cfg),
      // RawImpressionSource hands absolute gs:// URIs, so the root is only the FileSystem selector.
      rootPath = Path(storageRootUri),
      encryptionConfig = ParquetEncryptionConfig(kmsProvider = { kms }),
    )

  /**
   * Reads the compiled-model blob bytes at [modelBlobUri] from the model's own GCS project given by
   * [cfg] (the compiled model lives in a project separate from the impression/map storage).
   */
  protected suspend fun readCompiledModelBlob(
    cfg: StorageConfig,
    modelBlobUri: String,
  ): ByteString {
    val blobUri = SelectedStorageClient.parseBlobUri(modelBlobUri)
    val blob =
      SelectedStorageClient(blobUri, /* rootDirectory= */ null, cfg.projectId).getBlob(blobUri.key)
        ?: error("Compiled-model blob not found: $modelBlobUri")
    return blob.read().flatten()
  }

  /** A [StorageConfig] from the GCS project id (root resolves per absolute gs:// URI). */
  protected fun storageConfig(gcsProjectId: String): StorageConfig =
    StorageConfig(projectId = gcsProjectId)
}

/**
 * Production GCS Hadoop [Configuration] for [ParquetStorageClient], wiring the GCS connector for
 * [projectId]. Each phase runner passes this as its `hadoopConfigurationFor`; tests can instead
 * inject a `file://`-shaped [Configuration] without subclassing.
 */
fun gcsHadoopConfiguration(projectId: String): Configuration =
  Configuration().apply {
    set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    set("fs.gs.auth.type", "COMPUTE_ENGINE")
    set("fs.gs.project.id", projectId)
  }
