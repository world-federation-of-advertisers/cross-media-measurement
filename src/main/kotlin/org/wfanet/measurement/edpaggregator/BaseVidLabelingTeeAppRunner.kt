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
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
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
  private val hadoopConfigurationFor: (StorageConfig) -> Configuration,
) : BaseTeeAppRunner() {

  init {
    // Register Tink AEAD key templates (e.g. AES128_GCM) so the phase apps can generate DEKs to
    // encrypt their outputs (subpool map / rank map / labeled impressions). Unlike the results
    // fulfiller (which only unwraps a pre-generated DEK), the VID-labeling phases generate new
    // keysets, so the registry must be populated. register() is idempotent.
    AeadConfig.register()
    StreamingAeadConfig.register()
  }

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
   * The per-EDP KEK URI, or null when [dataProvider] has no configured KEK URI. EDPs that do not use
   * the VID Labeling pipeline (e.g. AWS/direct-path EDPs) legitimately leave `kms_config.kek_uri`
   * unset in the shared all-EDP config and never produce pipeline work, so callers skip them rather
   * than eagerly requiring a KEK URI that will never be used.
   */
  protected fun kekUriForOrNull(dataProvider: String): String? {
    val edpConfig =
      edpsConfig.eventDataProviderConfigList.firstOrNull { it.dataProvider == dataProvider }
        ?: return null
    val kekUri = edpConfig.kmsConfig.kekUri
    return if (kekUri.isNotEmpty()) kekUri else null
  }

  /**
   * A bucket-rooted, multi-key [ConditionalOperationStorageClient] for the store at
   * [StorageConfig.blobPrefix] (its `gs://` / `file://` scheme selects the backend).
   *
   * The subpool-map and rank-index stores are multi-key: one client serves many relative keys under
   * one bucket. This returns the [SelectedStorageClient.underlyingClient] (a bucket-rooted
   * `GcsStorageClient` / `FileSystemStorageClient`), NOT the [SelectedStorageClient] itself — the
   * latter is single-blob (it asserts the requested key equals its one constructor key), so rooting
   * it at a bare `gs://` would reject every real key. Callers write/read relative keys under the
   * prefix's bucket.
   */
  protected fun buildStorageClient(cfg: StorageConfig): ConditionalOperationStorageClient {
    val blobPrefix =
      requireNotNull(cfg.blobPrefix) {
        "StorageConfig.blobPrefix must be set to build a multi-key storage client"
      }
    return SelectedStorageClient(
        SelectedStorageClient.parseBlobUri(blobPrefix),
        cfg.rootDirectory,
        cfg.projectId,
      )
      .underlyingClient
  }

  /** A [ParquetStorageClient] for raw-impression reads, PME-decrypting via [kms]. */
  protected fun buildParquetStorageClient(
    cfg: StorageConfig,
    kms: KmsClient,
  ): ParquetStorageClient =
    ParquetStorageClient(
      conf = hadoopConfigurationFor(cfg),
      // RawImpressionSource hands absolute gs:// URIs, so the root only selects the FileSystem;
      // ParquetStorageClient still requires a bucket in the root, so use the configured
      // raw-impression bucket (cfg.blobPrefix, e.g. "gs://<bucket>") rather than a bare "gs://".
      rootPath =
        Path(
          requireNotNull(cfg.blobPrefix) {
            "StorageConfig.blobPrefix must be set to root the raw-impression ParquetStorageClient"
          }
        ),
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

