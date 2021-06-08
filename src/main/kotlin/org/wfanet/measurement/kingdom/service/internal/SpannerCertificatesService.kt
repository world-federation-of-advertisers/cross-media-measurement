package org.wfanet.measurement.kingdom.service.internal

import java.time.Clock
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetCertificateRequest
import org.wfanet.measurement.internal.kingdom.ReleaseCertificateHoldRequest
import org.wfanet.measurement.internal.kingdom.RevokeCertificateRequest

class SpannerCertificatesService(
  clock: Clock,
  idGenerator: IdGenerator,
  client: AsyncDatabaseClient
) : CertificatesCoroutineImplBase() {

  override suspend fun createCertificate(request: Certificate): Certificate {
    TODO("not implemented yet")
  }

  override suspend fun getCertificate(request: GetCertificateRequest): Certificate {
    TODO("not implemented yet")
  }
  override suspend fun revokeCertificate(request: RevokeCertificateRequest): Certificate {
    TODO("not implemented yet")
  }

  override suspend fun releaseCertificateHold(request: ReleaseCertificateHoldRequest): Certificate {
    TODO("not implemented yet")
  }
}
