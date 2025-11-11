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

/**
 * Factory for creating and caching BigQuery service instances.
 *
 * This factory maintains a thread-safe cache of BigQuery services indexed by project ID, ensuring
 * that only one service instance is created per project.
 *
 * BigQuery service instances are thread-safe and designed to be reused across multiple operations,
 * so caching them reduces connection overhead and improves performance.
 */
class BigQueryServiceFactory {
  private val projectIdToService = ConcurrentHashMap<String, BigQuery>()

  /**
   * Gets or creates a BigQuery service for the specified project.
   *
   * If a service already exists for the project ID, returns the cached instance. Otherwise, creates
   * a new service, caches it, and returns it.
   *
   * This method is thread-safe and ensures that only one service is created per project, even when
   * called concurrently.
   *
   * @param projectId The Google Cloud project ID
   * @return A BigQuery service instance for the specified project
   */
  fun getService(projectId: String): BigQuery {
    return projectIdToService.computeIfAbsent(projectId) { pid ->
      logger.info("Creating new BigQuery service for project: $pid")
      BigQueryOptions.newBuilder().setProjectId(pid).build().service
    }
  }

  /** Returns the number of cached BigQuery services. Useful for monitoring and debugging. */
  val cachedServiceCount: Int
    get() = projectIdToService.size

  /**
   * Returns the set of project IDs for which services are cached. Useful for monitoring and
   * debugging.
   */
  val cachedProjectIds: Set<String>
    get() = projectIdToService.keys.toSet()

  companion object {
    private val logger by loggerFor()
  }
}
