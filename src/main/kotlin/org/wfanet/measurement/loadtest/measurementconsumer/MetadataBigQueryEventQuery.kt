/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.measurementconsumer

import com.google.cloud.bigquery.BigQuery
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessage
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.consent.client.measurementconsumer.decryptMetadata
import org.wfanet.measurement.loadtest.dataprovider.BigQueryEventQuery

/** [BigQueryEventQuery] that reads publisher IDs from [EventGroup] metadata. */
class MetadataBigQueryEventQuery(
  bigQuery: BigQuery,
  datasetName: String,
  tableName: String,
  private val mcPrivateKey: PrivateKeyHandle,
) : BigQueryEventQuery(bigQuery, datasetName, tableName) {
  override fun getPublisherId(eventGroup: EventGroup): Int {
    val metadata = decryptMetadata(eventGroup.encryptedMetadata, mcPrivateKey)
    return metadata.metadata.unpack(TestMetadataMessage::class.java).publisherId
  }
}
