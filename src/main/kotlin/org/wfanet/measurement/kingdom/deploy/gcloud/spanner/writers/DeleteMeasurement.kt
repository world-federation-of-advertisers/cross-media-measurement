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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.protobuf.Empty
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.DeleteMeasurementRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementNotFoundByMeasurementConsumerException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader

class DeleteMeasurement(
  private val request: DeleteMeasurementRequest,
) : SimpleSpannerWriter<Empty>() {

  override suspend fun TransactionScope.runTransaction(): Empty {
    val externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId)
    val externalMeasurementId = ExternalId(request.externalMeasurementId)

    val result =
      MeasurementReader(Measurement.View.DEFAULT)
        .readByExternalIds(transactionContext, externalMeasurementConsumerId, externalMeasurementId)
        ?: throw MeasurementNotFoundByMeasurementConsumerException(
          externalMeasurementConsumerId,
          externalMeasurementId
        ) {
          "Measurement with external MeasurementConsumer ID $externalMeasurementConsumerId and " +
            "external Measurement ID $externalMeasurementId not found"
        }

    transactionContext.buffer(
      Mutation.delete(
        "Measurements",
        KeySet.singleKey(Key.of(result.measurementConsumerId.value, result.measurementId.value))
      )
    )
    return Empty.getDefaultInstance()
  }
}
