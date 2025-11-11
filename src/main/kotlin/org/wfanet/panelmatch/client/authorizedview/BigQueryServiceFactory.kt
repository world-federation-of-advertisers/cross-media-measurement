// Copyright 2025 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.panelmatch.client.authorizedview

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import java.util.concurrent.ConcurrentHashMap
import org.wfanet.panelmatch.common.loggerFor

/** Factory that creates and caches one [BigQuery] service instance per project ID. */
open class BigQueryServiceFactory {
  private val projectIdToService = ConcurrentHashMap<String, BigQuery>()

  /** Returns the cached [BigQuery] service for [projectId], creating one if necessary. */
  fun getService(projectId: String): BigQuery {
    return projectIdToService.computeIfAbsent(projectId) { pid ->
      logger.info("Creating new BigQuery service for project: $pid")
      BigQueryOptions.newBuilder().setProjectId(pid).build().service
    }
  }

  companion object {
    private val logger by loggerFor()
  }
}
