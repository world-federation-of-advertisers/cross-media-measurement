// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.toHexString
import org.wfanet.measurement.duchy.db.metricvalue.MetricValueDatabase
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.getBytesAsByteString
import org.wfanet.measurement.internal.duchy.MetricValue

/** Metadata for `MetricValues` table. */
private object MetricValuesTable {
  const val TABLE_NAME = "MetricValues"
  val columns = Columns
  val indexes = Indexes

  object Columns {
    const val METRIC_VALUE_ID = "MetricValueId"
    const val EXTERNAL_METRIC_VALUE_ID = "ExternalMetricValueId"
    const val DATA_PROVIDER_RESOURCE_ID = "DataProviderResourceId"
    const val CAMPAIGN_RESOURCE_ID = "CampaignResourceId"
    const val METRIC_REQUISITION_RESOURCE_ID = "MetricRequisitionResourceId"
    const val BLOB_STORAGE_KEY = "BlobStorageKey"
    const val BLOB_FINGERPRINT = "BlobFingerprint"

    val all =
      listOf(
        METRIC_VALUE_ID,
        EXTERNAL_METRIC_VALUE_ID,
        DATA_PROVIDER_RESOURCE_ID,
        CAMPAIGN_RESOURCE_ID,
        METRIC_REQUISITION_RESOURCE_ID,
        BLOB_STORAGE_KEY,
        BLOB_FINGERPRINT
      )
  }

  object Indexes {
    const val METRIC_VALUES_BY_EXTERNAL_ID = "MetricValuesByExternalId"
    const val METRIC_VALUES_BY_RESOURCE_KEY = "MetricValuesByResourceKey"
  }
}

/** Google Cloud Spanner implementation of [MetricValueDatabase]. */
class SpannerMetricValueDatabase(
  private val dbClient: AsyncDatabaseClient,
  private val idGenerator: IdGenerator
) : MetricValueDatabase {

  override suspend fun insertMetricValue(metricValue: MetricValue): MetricValue {
    val resourceKey = metricValue.resourceKey
    require(resourceKey.dataProviderResourceId.isNotEmpty())
    require(resourceKey.campaignResourceId.isNotEmpty())
    require(resourceKey.metricRequisitionResourceId.isNotEmpty())
    require(metricValue.blobStorageKey.isNotEmpty())
    require(metricValue.blobFingerprint.size() == 32) {
      val fingerprint = metricValue.blobFingerprint
      val hexFingerprint = fingerprint.toByteArray().toHexString()
      "blobFingerprint size ${fingerprint.size()} is not 32: $hexFingerprint"
    }

    val id = idGenerator.generateInternalId()
    val externalId = idGenerator.generateExternalId()
    val insertMutation = with(MetricValuesTable) {
      Mutation.newInsertBuilder(TABLE_NAME)
        .set(columns.METRIC_VALUE_ID).to(id.value)
        .set(columns.EXTERNAL_METRIC_VALUE_ID).to(externalId.value)
        .set(columns.DATA_PROVIDER_RESOURCE_ID).to(resourceKey.dataProviderResourceId)
        .set(columns.CAMPAIGN_RESOURCE_ID).to(resourceKey.campaignResourceId)
        .set(columns.METRIC_REQUISITION_RESOURCE_ID).to(resourceKey.metricRequisitionResourceId)
        .set(columns.BLOB_STORAGE_KEY).to(metricValue.blobStorageKey)
        .set(columns.BLOB_FINGERPRINT).to(metricValue.blobFingerprint.toGcloudByteArray())
        .build()
    }

    dbClient.readWriteTransaction().execute { txn ->
      txn.buffer(insertMutation)
    }

    return metricValue.toBuilder().setExternalId(externalId.value).build()
  }

  override suspend fun getMetricValue(externalId: ExternalId): MetricValue? {
    val sql = with(MetricValuesTable) {
      """
      SELECT * FROM $TABLE_NAME@{FORCE_INDEX=${indexes.METRIC_VALUES_BY_EXTERNAL_ID}}
      WHERE ${columns.EXTERNAL_METRIC_VALUE_ID} = @externalId
      """.trimIndent()
    }
    val query = Statement.newBuilder(sql)
      .bind("externalId").to(externalId.value)
      .build()

    return dbClient.singleUse().executeQuery(query).singleOrNull()?.toMetricValue()
  }

  override suspend fun getMetricValue(resourceKey: MetricValue.ResourceKey): MetricValue? {
    val sql = with(MetricValuesTable) {
      """
      SELECT * FROM $TABLE_NAME@{FORCE_INDEX=${indexes.METRIC_VALUES_BY_RESOURCE_KEY}}
      WHERE
        ${columns.DATA_PROVIDER_RESOURCE_ID} = @dataProviderResourceId
        AND ${columns.CAMPAIGN_RESOURCE_ID} = @campaignResourceId
        AND ${columns.METRIC_REQUISITION_RESOURCE_ID} = @metricRequisitionResourceId
      """.trimIndent()
    }
    val query = with(resourceKey) {
      Statement.newBuilder(sql)
        .bind("dataProviderResourceId").to(dataProviderResourceId)
        .bind("campaignResourceId").to(campaignResourceId)
        .bind("metricRequisitionResourceId").to(metricRequisitionResourceId)
        .build()
    }

    return dbClient.singleUse().executeQuery(query).singleOrNull()?.toMetricValue()
  }

  override suspend fun getBlobStorageKey(resourceKey: MetricValue.ResourceKey): String? =
    with(MetricValuesTable) {
      val indexRow: Struct? = dbClient.singleUse().readRowUsingIndex(
        TABLE_NAME,
        indexes.METRIC_VALUES_BY_RESOURCE_KEY,
        resourceKey.toSpannerKey(),
        columns.BLOB_STORAGE_KEY
      )
      indexRow?.getString(columns.BLOB_STORAGE_KEY)
    }
}

private fun Struct.toMetricValue(): MetricValue = with(MetricValuesTable.Columns) {
  MetricValue.newBuilder().apply {
    externalId = getLong(EXTERNAL_METRIC_VALUE_ID)
    resourceKeyBuilder.apply {
      dataProviderResourceId = getString(DATA_PROVIDER_RESOURCE_ID)
      campaignResourceId = getString(CAMPAIGN_RESOURCE_ID)
      metricRequisitionResourceId = getString(METRIC_REQUISITION_RESOURCE_ID)
    }
    blobStorageKey = getString(BLOB_STORAGE_KEY)
    blobFingerprint = getBytesAsByteString(BLOB_FINGERPRINT)
  }.build()
}

private fun MetricValue.ResourceKey.toSpannerKey(): Key {
  return Key.of(dataProviderResourceId, campaignResourceId, metricRequisitionResourceId)
}
