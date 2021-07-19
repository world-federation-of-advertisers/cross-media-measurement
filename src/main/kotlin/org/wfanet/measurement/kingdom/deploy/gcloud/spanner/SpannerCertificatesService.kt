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
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetCertificateRequest
import org.wfanet.measurement.internal.kingdom.ReleaseCertificateHoldRequest
import org.wfanet.measurement.internal.kingdom.RevokeCertificateRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader.Owner
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateCertificate

class SpannerCertificatesService(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : CertificatesCoroutineImplBase() {

  private fun getInternalResourceName(request: GetCertificateRequest): Owner {
    if (request.hasExternalMeasurementConsumerId()) {
      return Owner.MEASUREMENT_CONSUMER
    }
    if (request.hasExternalDataProviderId()) {
      return Owner.DATA_PROVIDER
    } else {
      return Owner.DUCHY
    }
  }

  override suspend fun createCertificate(request: Certificate): Certificate {
    return CreateCertificate(request).execute(client, idGenerator, clock)
  }

  override suspend fun getCertificate(request: GetCertificateRequest): Certificate {
    return CertificateReader(getInternalResourceName(request))
      .readExternalIdOrNull(client.singleUse(), ExternalId(request.externalCertificateId))
      ?.certificate
      ?: failGrpc(Status.NOT_FOUND) {
        "No Certificate with externalId ${request.externalCertificateId}"
      }
  }
  override suspend fun revokeCertificate(request: RevokeCertificateRequest): Certificate {
    TODO("not implemented yet")
  }

  override suspend fun releaseCertificateHold(request: ReleaseCertificateHoldRequest): Certificate {
    TODO("not implemented yet")
  }
}
