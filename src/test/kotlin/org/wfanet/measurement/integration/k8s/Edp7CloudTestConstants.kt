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

package org.wfanet.measurement.integration.k8s

import java.time.LocalDate

/**
 * edp7's pipelined date(s): the date(s) whose edp7 impressions are produced by the deployed VID
 * Labeling pipeline rather than by pre-labeled test data. Single source of truth shared by
 * [SeedRawImpressionsRule], [WriteReusedLabeledImpressionsRule], and CreateDoneBlobs so the set
 * cannot drift across the three.
 *
 * Kept to one date deliberately: when several raw `done` blobs drop at once the dispatcher
 * processes the first and defers the rest, and nothing re-triggers the deferred ones (monitor
 * re-dispatch is unimplemented; uploads arrive sequentially in production). One upload still
 * exercises Phase 0/1/2 end to end.
 */
internal object Edp7PipelinedDates {
  val DATES: Set<LocalDate> = setOf(LocalDate.parse("2021-03-21"))
}

/**
 * edp7's storage KEK URI per GCP project (KMS resource names, not secrets) - the key the deployed
 * VidLabeler TEE uses to decrypt/re-encrypt edp7's raw impressions. Single source of truth shared
 * by [SeedRawImpressionsRule] and [WriteReusedLabeledImpressionsRule].
 *
 * All three test envs (dev / head / qa) deliberately share one KEK URI in the dev EDP project: edp7
 * is the same EDP everywhere, with `kms_config.service_account = primus-sa@halo-cmm-dev-edp` in
 * dev/head/qa alike (see the per-env `EDPA_EDPS_CONFIG` variable). This is not a staging shortcut;
 * if a future change splits edp7 per env, update [BY_PROJECT]. Kept here rather than a per-env
 * GitHub variable because the env-var limit is reached; the `EDP7_KEK_URI` env still overrides at
 * the call sites.
 */
internal object Edp7StorageKek {
  private const val URI =
    "gcp-kms://projects/halo-cmm-dev-edp/locations/global/keyRings/" +
      "halo-cmm-dev-edp-enc-kr/cryptoKeys/halo-cmm-dev-edp-enc-key-"
  val BY_PROJECT: Map<String, String> =
    mapOf("halo-cmm-dev" to URI, "halo-cmm-head" to URI, "halo-cmm-qa" to URI)
}
