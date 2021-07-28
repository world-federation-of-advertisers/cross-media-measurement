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
import java.time.Clock
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetCertificateRequest
import org.wfanet.measurement.internal.kingdom.ReleaseCertificateHoldRequest
import org.wfanet.measurement.internal.kingdom.RevokeCertificateRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateCertificate

class SpannerCertificatesService(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : CertificatesCoroutineImplBase() {
  override suspend fun createCertificate(request: Certificate): Certificate {

    grpcRequire(request.parentCase != Certificate.ParentCase.PARENT_NOT_SET) {
      "Certificate is missing parent field"
    }

    try {
      return CreateCertificate(request).execute(client, idGenerator, clock)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND ->
          failGrpc(Status.INVALID_ARGUMENT) { "MeasurementConsumer not found" }
        KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND ->
          failGrpc(Status.INVALID_ARGUMENT) { "DataProvider not found" }
        KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS ->
          failGrpc(Status.ALREADY_EXISTS) {
            "Certificate with the same subject key identifier (SKID) already exists."
          }
      }
    }
  }

  override suspend fun getCertificate(request: GetCertificateRequest): Certificate {

    grpcRequire(request.parentCase != GetCertificateRequest.ParentCase.PARENT_NOT_SET) {
      "GetCertificateRequest is missing parent field"
    }

    return CertificateReader(request)
      .readExternalIdOrNull(client.singleUse(), ExternalId(request.externalCertificateId))
      ?.certificate
      ?: failGrpc(Status.NOT_FOUND) { "Certificate not found" }
  }

  override suspend fun revokeCertificate(request: RevokeCertificateRequest): Certificate {
    TODO("not implemented yet")
  }

  override suspend fun releaseCertificateHold(request: ReleaseCertificateHoldRequest): Certificate {
    TODO("not implemented yet")
  }
}
