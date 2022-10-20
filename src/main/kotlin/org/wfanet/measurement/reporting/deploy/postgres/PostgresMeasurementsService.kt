// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.postgres

import io.grpc.Status
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.reporting.GetMeasurementRequest
import org.wfanet.measurement.internal.reporting.Measurement
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.SetMeasurementFailureRequest
import org.wfanet.measurement.internal.reporting.SetMeasurementResultRequest
import org.wfanet.measurement.reporting.deploy.postgres.readers.MeasurementReader
import org.wfanet.measurement.reporting.deploy.postgres.writers.CreateMeasurement
import org.wfanet.measurement.reporting.deploy.postgres.writers.SetMeasurementFailure
import org.wfanet.measurement.reporting.deploy.postgres.writers.SetMeasurementResult
import org.wfanet.measurement.reporting.service.internal.MeasurementAlreadyExistsException
import org.wfanet.measurement.reporting.service.internal.MeasurementNotFoundException
import org.wfanet.measurement.reporting.service.internal.MeasurementStateInvalidException

class PostgresMeasurementsService(
  private val idGenerator: IdGenerator,
  private val client: DatabaseClient,
) : MeasurementsCoroutineImplBase() {
  override suspend fun createMeasurement(request: Measurement): Measurement {
    return try {
      CreateMeasurement(request).execute(client, idGenerator)
    } catch (e: MeasurementAlreadyExistsException) {
      e.throwStatusRuntimeException(Status.ALREADY_EXISTS) { "Measurement already exists." }
    }
  }

  override suspend fun getMeasurement(request: GetMeasurementRequest): Measurement {
    val measurementResult =
      SerializableErrors.retrying {
        MeasurementReader()
          .readMeasurementByReferenceIds(
            client.singleUse(),
            request.measurementConsumerReferenceId,
            request.measurementReferenceId
          )
      }
        ?: MeasurementNotFoundException().throwStatusRuntimeException(Status.NOT_FOUND) {
          "Measurement not found."
        }

    return measurementResult.measurement
  }

  override suspend fun setMeasurementResult(request: SetMeasurementResultRequest): Measurement {
    return try {
      SetMeasurementResult(request).execute(client, idGenerator)
    } catch (e: MeasurementNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "Measurement not found." }
    } catch (e: MeasurementStateInvalidException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
        "Measurement has already been updated."
      }
    }
  }

  override suspend fun setMeasurementFailure(request: SetMeasurementFailureRequest): Measurement {
    return try {
      SetMeasurementFailure(request).execute(client, idGenerator)
    } catch (e: MeasurementNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "Measurement not found." }
    } catch (e: MeasurementStateInvalidException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
        "Measurement has already been updated."
      }
    }
  }
}
