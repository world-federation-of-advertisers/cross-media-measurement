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

package org.wfanet.measurement.reporting.deploy.v2.postgres

import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumer
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.CreateMeasurementConsumer
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerAlreadyExistsException

class PostgresMeasurementConsumersService(
  private val idGenerator: IdGenerator,
  private val client: DatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase(coroutineContext) {
  override suspend fun createMeasurementConsumer(
    request: MeasurementConsumer
  ): MeasurementConsumer {
    try {
      return CreateMeasurementConsumer(request).execute(client, idGenerator)
    } catch (e: MeasurementConsumerAlreadyExistsException) {
      throw e.asStatusRuntimeException(
        Status.Code.ALREADY_EXISTS,
        "Measurement Consumer already exists",
      )
    }
  }
}
