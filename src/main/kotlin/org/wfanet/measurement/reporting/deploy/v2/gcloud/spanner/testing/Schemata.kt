/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing

import java.nio.file.Path
import org.wfanet.measurement.common.getJarResourcePath

object Schemata {
  private const val REPORTING_SPANNER_RESOURCE_PREFIX = "reporting/spanner"

  private fun getResourcePath(fileName: String): Path {
    val resourceName = "$REPORTING_SPANNER_RESOURCE_PREFIX/$fileName"
    val classLoader: ClassLoader = Thread.currentThread().contextClassLoader
    return requireNotNull(classLoader.getJarResourcePath(resourceName)) {
      "Resource $resourceName not found"
    }
  }

  val REPORTING_CHANGELOG_PATH: Path = getResourcePath("changelog.yaml")
}
