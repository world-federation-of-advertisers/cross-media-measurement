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
import java.util.logging.Logger
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionSource
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow

/**
 * Labels one raw-impression upload's shard with VIDs.
 *
 * Driven by [VidLabelerApp] per work item. Loads the compiled model for each model line once (via
 * [vidModelLoader]) to build a [VidAssigner] per line, then streams this VM's shard of the upload
 * through [rawImpressionSource], handing each input file to a [VidLabelingSink] that labels its
 * events and writes the encrypted labeled output.
 *
 * The [rawImpressionSource] is constructed by the app (it owns the shard index/total, storage, and
 * discovery stub); the [modelLineSpecs] are likewise pre-resolved by the app from the `ModelLine`
 * resources. This class owns the labeling job itself: model-line resolution, loading the assigners,
 * and driving the stream.
 *
 * @param rawImpressionSource the per-shard raw-impression reader.
 * @param modelLineSpecs model lines available to label with, each resolved to its compiled-model
 *   blob URI and [ActiveWindow].
 * @param overrideModelLines if non-empty, only these model lines are used (validated against
 *   [modelLineSpecs]).
 * @param vidModelLoader loads + caches the compiled model per model blob URI.
 * @param impressionConverter converts Parquet rows into [ConvertedImpression]s (schema seam).
 * @param encryptKmsClient encrypt/decrypt KMS client for the labeled output.
 * @param encryptKekUri KEK URI for generating per-output DEKs.
 * @param outputStorageParams GCS project + blob prefix for labeled output.
 * @param storageConfig storage configuration built from [outputStorageParams].
 */
class VidLabeler(
  private val rawImpressionSource: RawImpressionSource,
  private val modelLineSpecs: List<ModelLineSpec>,
  private val overrideModelLines: List<String>,
  private val vidModelLoader: VidModelLoader,
  private val impressionConverter: ImpressionConverter,
  private val encryptKmsClient: KmsClient,
  private val encryptKekUri: String,
  private val outputStorageParams: VidLabelerParams.StorageParams,
  private val storageConfig: StorageConfig,
) {

  // TODO(world-federation-of-advertisers/cross-media-measurement#4010): Add vid_labeling_job,
  // raw_impression_upload_files, and model_lines fields to VidLabelerParams for the
  // VidLabelerApp wiring PR.

  /** Labels this VM's shard of the upload, writing encrypted labeled output per input file. */
  suspend fun label() {
    val specs = resolveModelLineSpecs()
    require(specs.isNotEmpty()) { "No model lines to label with" }

    val contexts =
      specs.map { spec ->
        ModelLineContext(
          modelLine = spec.modelLine,
          activeWindow = spec.activeWindow,
          assigner = vidModelLoader.getAssigner(spec.modelBlobUri),
          config = spec.config,
          rankIndex = spec.rankIndex,
        )
      }
    logger.info("Labeling shard with ${contexts.size} model line(s)")

    // TODO(world-federation-of-advertisers/cross-media-measurement#4010): Switch to file-batching
    // contract. RawImpressionSource currently shards by fingerprint; once VidLabelerApp is wired
    // up, pass VidLabelingJob.raw_impression_upload_files directly and remove fingerprint sharding.
    rawImpressionSource.streamBlobs { blobUri ->
      // The converter resolves each input file's entity keys (and legacy event group reference id)
      // from the dispatcher-provided per-file map, keyed by the input blob URI.
      VidLabelingSink(
        inputBlobUri = blobUri,
        modelLineContexts = contexts,
        impressionConverter = impressionConverter,
        encryptKmsClient = encryptKmsClient,
        encryptKekUri = encryptKekUri,
        outputStorageParams = outputStorageParams,
        storageConfig = storageConfig,
      )
    }
    logger.info("Finished labeling shard")
  }

  /** Model lines to label with: [overrideModelLines] if set, else all of [modelLineSpecs]. */
  private fun resolveModelLineSpecs(): List<ModelLineSpec> {
    if (overrideModelLines.isEmpty()) return modelLineSpecs
    val specsByModelLine = modelLineSpecs.associateBy { it.modelLine }
    return overrideModelLines.map { modelLine ->
      requireNotNull(specsByModelLine[modelLine]) {
        "Override model line $modelLine not found in model line specs"
      }
    }
  }

  companion object {
    private val logger = Logger.getLogger(VidLabeler::class.java.name)
  }
}

/**
 * A model line resolved to everything [VidLabeler] needs to label with it.
 *
 * @property modelLine model line resource name.
 * @property modelBlobUri URI of the compiled-model blob, loaded via [VidModelLoader].
 * @property activeWindow the model line's active interval, for event-time filtering.
 * @property config the model line's field-mapping configuration.
 * @property rankIndex the memoized rank index for this model line, or `null` for the non-memoized
 *   (hash-only) path. When set, each impression's rank is looked up and attached to the
 *   `LabelerInput` before labeling.
 */
data class ModelLineSpec(
  val modelLine: String,
  val modelBlobUri: String,
  val activeWindow: ActiveWindow,
  val config: VidLabelerParams.ModelLineConfig,
  val rankIndex: MemoizedRankIndex? = null,
)
