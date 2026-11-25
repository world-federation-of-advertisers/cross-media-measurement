// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common

object EventGroupHierarchyPath {
  private const val MAX_INDEX_KEY_BYTES = 8 * 1024

  /** Checks the full `EventGroupsByEntityTypePath` key, including its base-table key suffix. */
  fun fitsIndexKey(entityType: String, entityPath: String): Boolean {
    // DataProviderId, MeasurementConsumerId, and EventGroupId are INT64 columns.
    val keyBytes =
      3 * Long.SIZE_BYTES +
        entityType.toByteArray(Charsets.UTF_8).size +
        entityPath.toByteArray(Charsets.UTF_8).size
    return keyBytes <= MAX_INDEX_KEY_BYTES
  }
}
