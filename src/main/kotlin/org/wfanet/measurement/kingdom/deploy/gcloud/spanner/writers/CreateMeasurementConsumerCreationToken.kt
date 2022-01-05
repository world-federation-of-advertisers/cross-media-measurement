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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set

/** Creates a measurement consumer creation token in the database. */
class CreateMeasurementConsumerCreationToken() : SimpleSpannerWriter<Long>() {

  override suspend fun TransactionScope.runTransaction(): Long {
    val internalMeasurementConsumerCreationTokenId = idGenerator.generateInternalId()
    val measurementConsumerCreationToken = idGenerator.generateExternalId()
    val measurementConsumerCreationTokenHash = hashSha256(measurementConsumerCreationToken.value)

    transactionContext.bufferInsertMutation("MeasurementConsumerCreationTokens") {
      set("MeasurementConsumerCreationTokenId" to internalMeasurementConsumerCreationTokenId)
      set(
        "MeasurementConsumerCreationTokenHash" to
          measurementConsumerCreationTokenHash.toGcloudByteArray()
      )
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
    }

    return measurementConsumerCreationToken.value
  }
}
