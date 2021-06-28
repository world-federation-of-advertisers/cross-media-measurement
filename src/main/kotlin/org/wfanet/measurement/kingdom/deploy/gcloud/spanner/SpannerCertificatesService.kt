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

import java.time.Clock
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetCertificateRequest
import org.wfanet.measurement.internal.kingdom.ReleaseCertificateHoldRequest
import org.wfanet.measurement.internal.kingdom.RevokeCertificateRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateCertificate

class SpannerCertificatesService(
  val clock: Clock,
  val idGenerator: IdGenerator,
  val client: AsyncDatabaseClient
) : CertificatesCoroutineImplBase() {

  override suspend fun createCertificate(request: Certificate): Certificate {
    return CreateCertificate(request).execute(client, idGenerator, clock)
  }

  override suspend fun getCertificate(request: GetCertificateRequest): Certificate {
    val certificate =
      CertificateReader()
        .readExternalIdOrNull(client.singleUse(), ExternalId(request.externalcertificateId))
        ?.certificate
    if (measurementConsumer == null) {
      failGrpc(Status.FAILED_PRECONDITION) {
        "No Certificate with externalId ${request.externalcertificateId}"
      }
    }
    return certificate
  }
  override suspend fun revokeCertificate(request: RevokeCertificateRequest): Certificate {
    TODO("not implemented yet")
  }

  override suspend fun releaseCertificateHold(request: ReleaseCertificateHoldRequest): Certificate {
    TODO("not implemented yet")
  }
}
