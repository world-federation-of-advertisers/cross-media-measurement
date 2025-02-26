/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.SpannerException
import io.grpc.Status
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumer
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertMeasurementConsumer
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.measurementConsumerExists

class SpannerMeasurementConsumersService (
  private val spannerClient: AsyncDatabaseClient,
  private val idGenerator: IdGenerator = IdGenerator.Default,
  ) : MeasurementConsumersCoroutineImplBase() {
  override suspend fun createMeasurementConsumer(request: MeasurementConsumer): MeasurementConsumer {
    val transactionRunner = spannerClient.readWriteTransaction()
    try {
      transactionRunner.run { txn ->
        val measurementConsumerId = idGenerator.generateNewId { id -> txn.measurementConsumerExists(id) }
        txn.insertMeasurementConsumer(
          measurementConsumerId = measurementConsumerId,
          measurementConsumer = request,
        )
      }
      transactionRunner.close()
    } catch (e: SpannerException) {
      if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
        throw StatusRuntimeException(Status.ALREADY_EXISTS)
      } else {
        throw e
      }
    }

    return request
  }
}
