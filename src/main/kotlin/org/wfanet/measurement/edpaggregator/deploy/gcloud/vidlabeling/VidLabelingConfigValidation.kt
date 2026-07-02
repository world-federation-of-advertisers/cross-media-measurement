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
    require(modelLineConfig.labelerInputFieldMappingMap.containsKey(EVENT_ID_FIELD_PATH)) {
      "labeler_input_field_mapping must map '$EVENT_ID_FIELD_PATH' for model line $modelLine on " +
        config.dataProvider
    }
  }
}
