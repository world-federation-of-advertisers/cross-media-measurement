/*
 * Copyright 2023 Google LLC
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader

class StreamDataProviders(externalDataProviderIds: Iterable<ExternalId>) :
  SimpleSpannerQuery<DataProviderReader.Result>() {
  override val reader =
    DataProviderReader().fillStatementBuilder {
      appendClause(
        """
        WHERE ExternalDataProviderId in UNNEST(@externalDataProviderIds)
      """
          .trimIndent()
      )
      bind("externalDataProviderIds").toInt64Array(externalDataProviderIds.map { it.value })
    }
}
