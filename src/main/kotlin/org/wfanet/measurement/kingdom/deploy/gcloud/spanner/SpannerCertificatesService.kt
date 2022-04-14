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
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.GetCertificateRequest
import org.wfanet.measurement.internal.kingdom.ReleaseCertificateHoldRequest
import org.wfanet.measurement.internal.kingdom.RevokeCertificateRequest
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.BaseSpannerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateCertificate
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ReleaseCertificateHold
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.RevokeCertificate

class SpannerCertificatesService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : CertificatesCoroutineImplBase() {
  override suspend fun createCertificate(request: Certificate): Certificate {
    grpcRequire(request.parentCase != Certificate.ParentCase.PARENT_NOT_SET) {
      "Certificate is missing parent field"
    }
    // TODO(world-federation-of-advertisers/cross-media-measurement#178) : Update fail conditions
    // accordingly.
    try {
      return CreateCertificate(request).execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "MeasurementConsumer not found" }
        ErrorCode.DATA_PROVIDER_NOT_FOUND -> failGrpc(Status.NOT_FOUND) { "DataProvider not found" }
        ErrorCode.MODEL_PROVIDER_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "ModelProvider not found" }
        ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS ->
          failGrpc(Status.ALREADY_EXISTS) {
            "Certificate with the same subject key identifier (SKID) already exists."
          }
        ErrorCode.DUCHY_NOT_FOUND -> failGrpc(Status.NOT_FOUND) { "Duchy not found" }
        ErrorCode.API_KEY_NOT_FOUND,
        ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        ErrorCode.DUPLICATE_ACCOUNT_IDENTITY,
        ErrorCode.ACCOUNT_NOT_FOUND,
        ErrorCode.PERMISSION_DENIED,
        ErrorCode.CERTIFICATE_NOT_FOUND,
        ErrorCode.CERTIFICATE_IS_INVALID,
        ErrorCode.MEASUREMENT_NOT_FOUND,
        ErrorCode.MEASUREMENT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND,
        ErrorCode.REQUISITION_NOT_FOUND,
        ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        ErrorCode.REQUISITION_STATE_ILLEGAL,
        ErrorCode.EVENT_GROUP_INVALID_ARGS,
        ErrorCode.EVENT_GROUP_NOT_FOUND,
        ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND,
        ErrorCode.UNKNOWN_ERROR,
        ErrorCode.UNRECOGNIZED -> throw e
      }
    }
  }

  override suspend fun getCertificate(request: GetCertificateRequest): Certificate {
    val externalCertificateId = ExternalId(request.externalCertificateId)
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    val reader: BaseSpannerReader<CertificateReader.Result> =
      when (request.parentCase) {
        GetCertificateRequest.ParentCase.EXTERNAL_DATA_PROVIDER_ID ->
          CertificateReader(CertificateReader.ParentType.DATA_PROVIDER)
            .bindWhereClause(ExternalId(request.externalDataProviderId), externalCertificateId)
        GetCertificateRequest.ParentCase.EXTERNAL_MEASUREMENT_CONSUMER_ID ->
          CertificateReader(CertificateReader.ParentType.MEASUREMENT_CONSUMER)
            .bindWhereClause(
              ExternalId(request.externalMeasurementConsumerId),
              externalCertificateId
            )
        GetCertificateRequest.ParentCase.EXTERNAL_DUCHY_ID -> {
          val duchyId =
            InternalId(
              grpcRequireNotNull(DuchyIds.getInternalId(request.externalDuchyId)) {
                "Duchy with external ID ${request.externalDuchyId} not found"
              }
            )
          CertificateReader(CertificateReader.ParentType.DUCHY)
            .bindWhereClause(duchyId, externalCertificateId)
        }
        GetCertificateRequest.ParentCase.PARENT_NOT_SET ->
          throw Status.INVALID_ARGUMENT.withDescription("parent not specified").asRuntimeException()
      }

    val certificateResult =
      reader.execute(client.singleUse()).singleOrNull()
        ?: failGrpc(Status.NOT_FOUND) { "Certificate not found" }

    return certificateResult.certificate
  }

  override suspend fun revokeCertificate(request: RevokeCertificateRequest): Certificate {
    grpcRequire(request.parentCase != RevokeCertificateRequest.ParentCase.PARENT_NOT_SET) {
      "RevokeCertificateRequest is missing parent field"
    }
    // TODO(world-federation-of-advertisers/cross-media-measurement#178) : Update fail conditions
    // accordingly.
    try {
      return RevokeCertificate(request).execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        ErrorCode.CERTIFICATE_NOT_FOUND -> failGrpc(Status.NOT_FOUND) { "Certificate not found" }
        ErrorCode.DUCHY_NOT_FOUND -> failGrpc(Status.NOT_FOUND) { "Duchy not found" }
        ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        ErrorCode.DUPLICATE_ACCOUNT_IDENTITY,
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
        ErrorCode.API_KEY_NOT_FOUND,
        ErrorCode.ACCOUNT_NOT_FOUND,
        ErrorCode.PERMISSION_DENIED,
        ErrorCode.DATA_PROVIDER_NOT_FOUND,
        ErrorCode.MODEL_PROVIDER_NOT_FOUND,
        ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        ErrorCode.MEASUREMENT_NOT_FOUND,
        ErrorCode.MEASUREMENT_STATE_ILLEGAL,
        ErrorCode.CERTIFICATE_IS_INVALID,
        ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND,
        ErrorCode.REQUISITION_NOT_FOUND,
        ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        ErrorCode.REQUISITION_STATE_ILLEGAL,
        ErrorCode.EVENT_GROUP_INVALID_ARGS,
        ErrorCode.EVENT_GROUP_NOT_FOUND,
        ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND,
        ErrorCode.UNKNOWN_ERROR,
        ErrorCode.UNRECOGNIZED -> throw e
      }
    }
  }

  override suspend fun releaseCertificateHold(request: ReleaseCertificateHoldRequest): Certificate {
    grpcRequire(request.parentCase != ReleaseCertificateHoldRequest.ParentCase.PARENT_NOT_SET) {
      "ReleaseCertificateHoldRequest is missing parent field"
    }
    // TODO(world-federation-of-advertisers/cross-media-measurement#178) : Update fail conditions
    // accordingly.
    try {
      return ReleaseCertificateHold(request).execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        ErrorCode.CERTIFICATE_NOT_FOUND -> failGrpc(Status.NOT_FOUND) { "Certificate not found" }
        ErrorCode.DUCHY_NOT_FOUND -> failGrpc(Status.NOT_FOUND) { "Duchy not found" }
        ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL ->
          failGrpc(Status.FAILED_PRECONDITION) { "Certificate is in wrong State." }
        ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        ErrorCode.DUPLICATE_ACCOUNT_IDENTITY,
        ErrorCode.ACCOUNT_NOT_FOUND,
        ErrorCode.API_KEY_NOT_FOUND,
        ErrorCode.PERMISSION_DENIED,
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
        ErrorCode.DATA_PROVIDER_NOT_FOUND,
        ErrorCode.MODEL_PROVIDER_NOT_FOUND,
        ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        ErrorCode.MEASUREMENT_NOT_FOUND,
        ErrorCode.CERTIFICATE_IS_INVALID,
        ErrorCode.MEASUREMENT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND,
        ErrorCode.REQUISITION_NOT_FOUND,
        ErrorCode.REQUISITION_STATE_ILLEGAL,
        ErrorCode.EVENT_GROUP_INVALID_ARGS,
        ErrorCode.EVENT_GROUP_NOT_FOUND,
        ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND,
        ErrorCode.UNKNOWN_ERROR,
        ErrorCode.UNRECOGNIZED -> throw e
      }
    }
  }
}
