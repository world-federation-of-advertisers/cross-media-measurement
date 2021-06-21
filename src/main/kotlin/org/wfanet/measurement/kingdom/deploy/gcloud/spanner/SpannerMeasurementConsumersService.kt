<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 55087518 (added server path)
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner
<<<<<<< HEAD
=======
package org.wfanet.measurement.kingdom.service.internal
>>>>>>> 7e49eb6d (moved services)
=======
>>>>>>> 55087518 (added server path)

import io.grpc.Status
import java.time.Clock
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.GetMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateMeasurementConsumer

class SpannerMeasurementConsumersService(
  val clock: Clock,
  val idGenerator: IdGenerator,
  val client: AsyncDatabaseClient
) : MeasurementConsumersCoroutineImplBase() {
  override suspend fun createMeasurementConsumer(
    request: MeasurementConsumer
  ): MeasurementConsumer {
    return CreateMeasurementConsumer(request).execute(client, idGenerator, clock)
  }
  override suspend fun getMeasurementConsumer(
    request: GetMeasurementConsumerRequest
  ): MeasurementConsumer {
    val measurementConsumer =
      MeasurementConsumerReader()
        .readExternalIdOrNull(client.singleUse(), ExternalId(request.externalMeasurementConsumerId))
        ?.measurementConsumer
    if (measurementConsumer == null) {
      failGrpc(Status.FAILED_PRECONDITION) {
        "No MeasurementConsumer with externalId ${request.externalMeasurementConsumerId}"
      }
    }
    return measurementConsumer
  }
}
