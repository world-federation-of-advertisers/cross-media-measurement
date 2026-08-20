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

package org.wfanet.measurement.edpaggregator.rawimpressions

import org.apache.hadoop.conf.Configuration

/**
 * Production GCS Hadoop [Configuration] wiring the GCS connector (`fs.gs.impl`) for [projectId].
 * Shared by the TEE app runners and the dispatcher Cloud Function so the connector settings stay in
 * a single place.
 */
fun gcsHadoopConfiguration(projectId: String): Configuration =
  Configuration().apply {
    set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    set("fs.gs.auth.type", "COMPUTE_ENGINE")
    set("fs.gs.project.id", projectId)
  }
