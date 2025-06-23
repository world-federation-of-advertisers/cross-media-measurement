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

import com.google.protobuf.Empty
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.BatchCancelMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.BatchCancelMeasurementsResponse
import org.wfanet.measurement.internal.kingdom.BatchCreateMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.BatchCreateMeasurementsResponse
import org.wfanet.measurement.internal.kingdom.BatchDeleteMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.BatchGetMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.BatchGetMeasurementsResponse
import org.wfanet.measurement.internal.kingdom.CancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.CreateMeasurementRequest
import org.wfanet.measurement.internal.kingdom.GetMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.GetMeasurementRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.SetMeasurementResultRequest
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.batchCreateMeasurementsResponse
import org.wfanet.measurement.internal.kingdom.batchGetMeasurementsResponse
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertificateIsInvalidException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotActiveException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementEtagMismatchException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementNotFoundByComputationException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementNotFoundByMeasurementConsumerException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamMeasurements
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.BatchCancelMeasurements
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.BatchDeleteMeasurements
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CancelMeasurement
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateMeasurements
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.SetMeasurementResult

private const val MAX_BATCH_DELETE = 1000
private const val MAX_BATCH_CANCEL = 1000

class SpannerMeasurementsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : MeasurementsCoroutineImplBase(coroutineContext) {

  override suspend fun createMeasurement(request: CreateMeasurementRequest): Measurement {
    validateCreateMeasurementRequest(request)
    try {
      return CreateMeasurements(listOf(request)).execute(client, idGenerator).first()
    } catch (e: CertificateIsInvalidException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Certificate is invalid.")
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found.")
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "DataProvider not found.")
    } catch (e: DuchyNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Duchy not found.")
    } catch (e: CertificateNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Certificate not found.")
    } catch (e: DuchyNotActiveException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Inactive required duchy.")
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  private fun validateCreateMeasurementRequest(request: CreateMeasurementRequest) {
    grpcRequire(request.measurement.externalMeasurementConsumerCertificateId != 0L) {
      "external_measurement_consumer_certificate_id unspecified"
    }
    grpcRequire(request.measurement.details.apiVersion.isNotEmpty()) { "api_version unspecified" }
    grpcRequire(!request.measurement.details.measurementSpec.isEmpty) {
      "measurement_spec unspecified"
    }
    grpcRequire(!request.measurement.details.measurementSpecSignature.isEmpty) {
      "measurement_spec_signature unspecified"
    }
    for ((externalDataProviderId, dataProvider) in request.measurement.dataProvidersMap) {
      grpcRequire(!dataProvider.dataProviderPublicKey.isEmpty) {
        "data_provider_public_key unspecified for ${ExternalId(externalDataProviderId)}"
      }
      grpcRequire(!dataProvider.encryptedRequisitionSpec.isEmpty) {
        "encrypted_requisition_spec unspecified for ${ExternalId(externalDataProviderId)}"
      }
      grpcRequire(!dataProvider.nonceHash.isEmpty) {
        "nonce_hash unspecified for ${ExternalId(externalDataProviderId)}"
      }
    }
  }

  override suspend fun getMeasurement(request: GetMeasurementRequest): Measurement {
    return MeasurementReader(Measurement.View.DEFAULT)
      .readByExternalIds(
        client.singleUse(),
        ExternalId(request.externalMeasurementConsumerId),
        ExternalId(request.externalMeasurementId),
      )
      ?.measurement
      ?: throw MeasurementNotFoundByMeasurementConsumerException(
          ExternalId(request.externalMeasurementConsumerId),
          ExternalId(request.externalMeasurementId),
        )
        .asStatusRuntimeException(Status.Code.NOT_FOUND, "Measurement not found")
  }

  override suspend fun getMeasurementByComputationId(
    request: GetMeasurementByComputationIdRequest
  ): Measurement {
    return MeasurementReader(Measurement.View.COMPUTATION)
      .readByExternalComputationId(client.singleUse(), ExternalId(request.externalComputationId))
      ?.measurement
      ?: throw MeasurementNotFoundByComputationException(ExternalId(request.externalComputationId))
        .asStatusRuntimeException(Status.Code.NOT_FOUND, "Measurement not found")
  }

  override fun streamMeasurements(request: StreamMeasurementsRequest): Flow<Measurement> {
    return StreamMeasurements(request.measurementView, request.filter, request.limit)
      .execute(client.singleUse())
      .map { it.measurement }
  }

  override suspend fun setMeasurementResult(request: SetMeasurementResultRequest): Measurement {
    grpcRequire(request.publicApiVersion.isNotEmpty()) { "public_api_version not specified" }
    try {
      return SetMeasurementResult(request).execute(client, idGenerator)
    } catch (e: MeasurementNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Measurement not found.")
    } catch (e: DuchyNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Duchy not found.")
    } catch (e: DuchyCertificateNotFoundException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Aggregator's Certificate not found.",
      )
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  override suspend fun cancelMeasurement(request: CancelMeasurementRequest): Measurement {
    with(request) {
      grpcRequire(externalMeasurementConsumerId != 0L) {
        "external_measurement_consumer_id not specified"
      }
      grpcRequire(externalMeasurementId != 0L) { "external_measurement_id not specified" }
    }

    val externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId)
    val externalMeasurementId = ExternalId(request.externalMeasurementId)

    try {
      return CancelMeasurement(externalMeasurementConsumerId, externalMeasurementId)
        .execute(client, idGenerator)
    } catch (e: MeasurementNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Measurement not found.")
    } catch (e: MeasurementStateIllegalException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Measurement state illegal.",
      )
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  override suspend fun batchDeleteMeasurements(request: BatchDeleteMeasurementsRequest): Empty {
    validateBatchDeleteMeasurementsRequest(request)
    try {
      return BatchDeleteMeasurements(request).execute(client, idGenerator)
    } catch (e: MeasurementNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Measurement not found.")
    } catch (e: MeasurementEtagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED, "Measurement etag mismatch.")
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  private fun validateBatchDeleteMeasurementsRequest(request: BatchDeleteMeasurementsRequest) {
    grpcRequire(request.requestsList.size <= MAX_BATCH_DELETE) {
      "number of requested Measurements exceeds limit: $MAX_BATCH_DELETE"
    }
    for (measurementDeleteRequest in request.requestsList) {
      grpcRequire(measurementDeleteRequest.externalMeasurementConsumerId != 0L) {
        "external_measurement_consumer_id not specified"
      }
      grpcRequire(measurementDeleteRequest.externalMeasurementId != 0L) {
        "external_measurement_id not specified"
      }
    }
  }

  override suspend fun batchCancelMeasurements(
    request: BatchCancelMeasurementsRequest
  ): BatchCancelMeasurementsResponse {
    validateBatchCancelMeasurementsRequest(request)
    try {
      return BatchCancelMeasurements(request).execute(client, idGenerator)
    } catch (e: MeasurementNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Measurement not found.")
    } catch (e: MeasurementEtagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED, "Measurement etag mismatch.")
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  private fun validateBatchCancelMeasurementsRequest(request: BatchCancelMeasurementsRequest) {
    grpcRequire(request.requestsList.size <= MAX_BATCH_CANCEL) {
      "number of requested Measurements exceeds limit: $MAX_BATCH_CANCEL"
    }
    for (measurementCancelRequest in request.requestsList) {
      grpcRequire(measurementCancelRequest.externalMeasurementConsumerId != 0L) {
        "external_measurement_consumer_id not specified"
      }
      grpcRequire(measurementCancelRequest.externalMeasurementId != 0L) {
        "external_measurement_id not specified"
      }
    }
  }

  override suspend fun batchCreateMeasurements(
    request: BatchCreateMeasurementsRequest
  ): BatchCreateMeasurementsResponse {
    for (createMeasurementRequest in request.requestsList) {
      grpcRequire(
        request.externalMeasurementConsumerId ==
          createMeasurementRequest.measurement.externalMeasurementConsumerId
      ) {
        "Child request external_measurement_consumer_id does not match parent request external_measurement_consumer_id."
      }
      validateCreateMeasurementRequest(createMeasurementRequest)
    }

    try {
      return batchCreateMeasurementsResponse {
        measurements += CreateMeasurements(request.requestsList).execute(client, idGenerator)
      }
    } catch (e: CertificateIsInvalidException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Certificate is invalid.")
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found.")
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "DataProvider not found.")
    } catch (e: DuchyNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Duchy not found.")
    } catch (e: CertificateNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Certificate not found.")
    } catch (e: DuchyNotActiveException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Inactive required duchy.")
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  override suspend fun batchGetMeasurements(
    request: BatchGetMeasurementsRequest
  ): BatchGetMeasurementsResponse {
    val externalMeasurementIds = request.externalMeasurementIdsList.distinct()
    val results: List<MeasurementReader.Result> =
      MeasurementReader(Measurement.View.DEFAULT)
        .readByExternalIds(
          client.singleUse(),
          ExternalId(request.externalMeasurementConsumerId),
          externalMeasurementIds.map { ExternalId(it) },
        )

    if (results.size < externalMeasurementIds.size) {
      throw Status.NOT_FOUND.withDescription("Measurement not found").asRuntimeException()
    }

    val measurementsMap =
      results.associate { result -> result.measurement.externalMeasurementId to result.measurement }

    return batchGetMeasurementsResponse {
      measurements += request.externalMeasurementIdsList.map { measurementsMap.getValue(it) }
    }
  }
}
