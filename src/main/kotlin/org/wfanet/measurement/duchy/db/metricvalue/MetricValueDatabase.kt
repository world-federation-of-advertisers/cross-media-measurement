// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.db.metricvalue

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.duchy.MetricValue

/** Database interface for `MetricValue` resources. */
interface MetricValueDatabase {
  /**
   * Inserts the specified [MetricValue] into the database.
   *
   * @param metricValue the [MetricValue] to insert. The
   *     [externalId][MetricValue.getExternalId] field will be ignored.
   * @return the inserted [MetricValue] with a new
   *     [externalId][MetricValue.getExternalId] assigned.
   */
  suspend fun insertMetricValue(metricValue: MetricValue): MetricValue

  /**
   * Returns the [MetricValue] corresponding to the specified lookup key, or
   * `null` if not found.
   */
  suspend fun getMetricValue(externalId: ExternalId): MetricValue?

  /**
   * Returns the [MetricValue] corresponding to the specified lookup key, or
   * `null` if not found.
   */
  suspend fun getMetricValue(resourceKey: MetricValue.ResourceKey): MetricValue?

  /**
   * Returns the [blobStorageKey][MetricValue.getBlobStorageKey] corresponding
   * to the [MetricValue] with the specified lookup key, or `null` if the
   * [MetricValue] isn't found.
   */
  suspend fun getBlobStorageKey(resourceKey: MetricValue.ResourceKey): String?
}
