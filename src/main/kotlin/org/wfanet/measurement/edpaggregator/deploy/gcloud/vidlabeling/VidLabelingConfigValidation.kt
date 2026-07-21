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

import java.time.Duration
import org.wfanet.measurement.common.toDuration
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfig

/**
 * LabelerInput field path the Phase-2 TEE requires: the raw event-id column that feeds
 * `EventIdDigestExtractor` (`VidLabelerApp` and `SubpoolAssignerApp` both `require` it at runtime).
 */
private const val EVENT_ID_FIELD_PATH = "event_id.id"

/**
 * Fails fast (at Cloud Function boot / first tick) if any per-model-line config in [config] is
 * missing a value the TEE `require()`s at label time. Several `VidLabelingConfig.ModelLineConfig`
 * fields are OPTIONAL at the proto level but required at runtime; without this pre-flight check a
 * missing value is stamped empty onto every WorkItem and only fails at Phase-2 — after Phase-0
 * sharding and Phase-1 ranking on the memoized path, then Pub/Sub retries and a DLQ FAILED cascade.
 */
fun requireValidModelLineConfigs(config: VidLabelingConfig) {
  for ((modelLine, modelLineConfig) in config.modelLineConfigsMap) {
    require(
      modelLineConfig.labelerInputFieldMappingList.any { it.fieldPath == EVENT_ID_FIELD_PATH }
    ) {
      "labeler_input_field_mapping must map '$EVENT_ID_FIELD_PATH' for model line $modelLine on " +
        config.dataProvider
    }
    require(modelLineConfig.eventTemplateDescriptorBlobUri.isNotEmpty()) {
      "event_template_descriptor_blob_uri missing for model line $modelLine on ${config.dataProvider}"
    }
    require(modelLineConfig.eventTemplateType.isNotEmpty()) {
      "event_template_type missing for model line $modelLine on ${config.dataProvider}"
    }
    require(
      modelLineConfig.requiredEntityKeyFieldMappingMap.isNotEmpty() ||
        modelLineConfig.optionalEntityKeyFieldMappingMap.isNotEmpty()
    ) {
      "entity_key_field_mapping (required or optional) must have at least one entry for model " +
        "line $modelLine on ${config.dataProvider}"
    }
  }
}

/**
 * Minimum permitted [VidLabelingConfig.getStalenessThreshold].
 *
 * The Monitor treats a model line whose updateTime has been static past the staleness threshold as
 * stuck and republishes its recovery WorkItem. Too short a value risks racing a legitimate but slow
 * last-out (a large fan-out can take far longer than a few minutes), making the Monitor mistake
 * in-flight work for stuck work and run a duplicate last-out. A one-day floor keeps recovery
 * attempts safely spaced (and makes spacing successive recovery attempts days apart viable).
 */
val MINIMUM_STALENESS_THRESHOLD: Duration = Duration.ofHours(24)

/**
 * Fails fast (at Cloud Function boot / first tick) if [config]'s staleness_threshold is unset or
 * below [MINIMUM_STALENESS_THRESHOLD], so a misconfiguration surfaces immediately instead of
 * silently letting the Monitor race an in-flight last-out.
 */
fun requireValidStalenessThreshold(config: VidLabelingConfig) {
  require(config.hasStalenessThreshold()) {
    "staleness_threshold must be set for data provider: ${config.dataProvider}"
  }
  val stalenessThreshold: Duration = config.stalenessThreshold.toDuration()
  require(stalenessThreshold >= MINIMUM_STALENESS_THRESHOLD) {
    "staleness_threshold ($stalenessThreshold) must be at least $MINIMUM_STALENESS_THRESHOLD for " +
      "data provider: ${config.dataProvider}; a shorter threshold risks recovering an in-flight " +
      "last-out as if it were stuck"
  }
}
