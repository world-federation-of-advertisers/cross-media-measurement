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

import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.AddMeasurementConsumerOwnerRequest
import org.wfanet.measurement.internal.kingdom.CreateMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.GetMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RemoveMeasurementConsumerOwnerRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.AccountActivationStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.AccountNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.PermissionDeniedException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.AddMeasurementConsumerOwner
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateMeasurementConsumer
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.RemoveMeasurementConsumerOwner

class SpannerMeasurementConsumersService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : MeasurementConsumersCoroutineImplBase(coroutineContext) {
  override suspend fun createMeasurementConsumer(
    request: CreateMeasurementConsumerRequest
  ): MeasurementConsumer {
    val measurementConsumer = request.measurementConsumer
    grpcRequire(
      measurementConsumer.details.apiVersion.isNotEmpty() &&
        !measurementConsumer.details.publicKey.isEmpty &&
        !measurementConsumer.details.publicKeySignature.isEmpty
    ) {
      "Details field of MeasurementConsumer is missing fields."
    }
    try {
      return CreateMeasurementConsumer(
          measurementConsumer,
          ExternalId(request.externalAccountId),
          request.measurementConsumerCreationTokenHash,
        )
        .execute(client, idGenerator)
    } catch (e: PermissionDeniedException) {
      throw e.asStatusRuntimeException(
        Status.Code.PERMISSION_DENIED,
        "Measurement Consumer creation token is not valid.",
      )
    } catch (e: AccountNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Account not found.")
    } catch (e: AccountActivationStateIllegalException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Account has not been activated yet.",
      )
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  override suspend fun getMeasurementConsumer(
    request: GetMeasurementConsumerRequest
  ): MeasurementConsumer {
    val externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId)
    return MeasurementConsumerReader()
      .readByExternalMeasurementConsumerId(client.singleUse(), externalMeasurementConsumerId)
      ?.measurementConsumer
      ?: throw MeasurementConsumerNotFoundException(externalMeasurementConsumerId)
        .asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found")
  }

  override suspend fun addMeasurementConsumerOwner(
    request: AddMeasurementConsumerOwnerRequest
  ): MeasurementConsumer {
    try {
      return AddMeasurementConsumerOwner(
          externalAccountId = ExternalId(request.externalAccountId),
          externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId),
        )
        .execute(client, idGenerator)
    } catch (e: AccountNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Account not found.")
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found.")
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  override suspend fun removeMeasurementConsumerOwner(
    request: RemoveMeasurementConsumerOwnerRequest
  ): MeasurementConsumer {
    try {
      return RemoveMeasurementConsumerOwner(
          externalAccountId = ExternalId(request.externalAccountId),
          externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId),
        )
        .execute(client, idGenerator)
    } catch (e: AccountNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Account not found.")
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found.")
    } catch (e: PermissionDeniedException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Account doesn't own MeasurementConsumer.",
      )
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }
}
