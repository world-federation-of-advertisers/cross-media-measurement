// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import com.google.cloud.spanner.Statement
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.StreamEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupMetadataDescriptorReader

class StreamEventGroupMetadataDescriptors(
  requestFilter: StreamEventGroupMetadataDescriptorsRequest.Filter
) : SimpleSpannerQuery<EventGroupMetadataDescriptorReader.Result>() {
  override val reader =
    EventGroupMetadataDescriptorReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause(
        "ORDER BY ExternalDataProviderId ASC, ExternalEventGroupMetadataDescriptorId ASC"
      )
    }

  private fun Statement.Builder.appendWhereClause(
    filter: StreamEventGroupMetadataDescriptorsRequest.Filter
  ) {
    val conjuncts = mutableListOf<String>()
    if (filter.externalEventGroupMetadataDescriptorIdsList.isNotEmpty()) {
      conjuncts.add(
        "ExternalEventGroupMetadataDescriptorId IN " +
          "UNNEST(@$EXTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR_IDS)"
      )
      bind(EXTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR_IDS)
        .toInt64Array(filter.externalEventGroupMetadataDescriptorIdsList.map { it.toLong() })
    }
    if (filter.externalDataProviderId != 0L) {
      conjuncts.add("ExternalDataProviderId = @$EXTERNAL_DATA_PROVIDER_ID")
      bind(EXTERNAL_DATA_PROVIDER_ID to filter.externalDataProviderId)
    }

    if (conjuncts.isEmpty()) {
      return
    }

    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  companion object {
    const val EXTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR_IDS =
      "externalEventGroupMetadataDescriptorIds"
    const val EXTERNAL_DATA_PROVIDER_ID = "externalDataProviderId"
    const val EXTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR_ID = "externalEventGroupMetadataDescriptorId"
  }
}
