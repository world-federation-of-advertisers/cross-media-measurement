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

package org.wfanet.measurement.duchy.db.metricvalue.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.duchy.db.metricvalue.MetricValueDatabase
import org.wfanet.measurement.internal.duchy.MetricValue

/** Abstract base class for [MetricValueDatabase] tests. */
abstract class AbstractMetricValueDatabaseTest<T : MetricValueDatabase> {
  protected lateinit var metricValueDb: T
  protected lateinit var fixedIdGenerator: FixedIdGenerator

  @Test fun `insertMetricValue inserts MetricValue`() = runBlocking {
    val metricValue = insertTestMetricValue()

    assertThat(metricValue).isEqualTo(testMetricValue)
  }

  @Test fun `getMetricValue returns null for non-existant external ID`() = runBlocking {
    val metricValue = metricValueDb.getMetricValue(ExternalId(testMetricValue.externalId))

    assertThat(metricValue).isNull()
  }

  @Test fun `getMetricValue returns null for non-existant resource key`() = runBlocking {
    val metricValue = metricValueDb.getMetricValue(testMetricValue.resourceKey)

    assertThat(metricValue).isNull()
  }

  @Test fun `getMetricValue with external ID returns MetricValue`() = runBlocking {
    insertTestMetricValue()

    val metricValue = metricValueDb.getMetricValue(ExternalId(testMetricValue.externalId))

    assertThat(metricValue).isEqualTo(testMetricValue)
  }

  @Test fun `getMetricValue with resource key returns MetricValue`() = runBlocking {
    insertTestMetricValue()

    val metricValue = metricValueDb.getMetricValue(testMetricValue.resourceKey)

    assertThat(metricValue).isEqualTo(testMetricValue)
  }

  @Test fun `getBlobStorageKey returns null for non-existant resource key`() = runBlocking {
    val blobKey = metricValueDb.getBlobStorageKey(testMetricValue.resourceKey)

    assertThat(blobKey).isNull()
  }

  @Test fun `getBlobStorageKey returns blob storage key`() = runBlocking {
    insertTestMetricValue()

    val blobKey = metricValueDb.getBlobStorageKey(testMetricValue.resourceKey)

    assertThat(blobKey).isEqualTo(testMetricValue.blobStorageKey)
  }

  private suspend fun insertTestMetricValue(): MetricValue {
    fixedIdGenerator.externalId = ExternalId(testMetricValue.externalId)
    return metricValueDb.insertMetricValue(testMetricValue.toBuilder().clearExternalId().build())
  }

  companion object {
    val testMetricValue: MetricValue = MetricValue.newBuilder().apply {
      externalId = 987654321L
      resourceKeyBuilder.apply {
        dataProviderResourceId = "data-provider-id"
        campaignResourceId = "campaign-id"
        metricRequisitionResourceId = "requisition-id"
      }
      blobStorageKey = "blob-key"
      blobFingerprint = ByteString.copyFrom(ByteArray(32))
    }.build()
  }
}
