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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers


import com.google.cloud.spanner.Mutation
<<<<<<< HEAD
<<<<<<< HEAD
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.insertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.Certificate

class CreateCertificate(private val certificate: Certificate) :
  SpannerWriter<Certificate, Certificate>() {
  override suspend fun TransactionScope.runTransaction(): Certificate {
    certificate.toInsertMutation(idGenerator.generateInternalId()).bufferTo(transactionContext)
    return certificate
  }

  override fun ResultScope<Certificate>.buildResult(): Certificate {
    return checkNotNull(transactionResult)
  }
}

fun Certificate.toInsertMutation(internalId: InternalId): Mutation {
  return insertMutation("Certificates") {
    set("CertificateId" to internalId.value)
    set("SubjectKeyIdentifier" to subjectKeyIdentifier.toGcloudByteArray())
    set("NotValidBefore" to notValidBefore.toGcloudTimestamp())
    set("NotValidAfter" to notValidAfter.toGcloudTimestamp())
    set("RevocationState" to revocationState)
    set("CertificateDetails" to details)
    setJson("CertificateDetailsJson" to details)
  }
}
=======
=======
import org.wfanet.measurement.common.identity.InternalId
>>>>>>> 8b0bd33b (building)
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.kingdom.Certificate

class CreateCertificate(private val certificate: Certificate) :
  SpannerWriter<ExternalId, Certificate>() {
  override suspend fun TransactionScope.runTransaction(): ExternalId {
    val internalId = idGenerator.generateInternalId()
    val externalId = idGenerator.generateExternalId()
    certificate.toInsertMutation(internalId, externalId).bufferTo(transactionContext)
    return externalId
  }

  override fun ResultScope<ExternalId>.buildResult(): Certificate {
    val externalCertificateId = checkNotNull(transactionResult).value
    return Certificate.newBuilder().setExternalCertificateId(externalCertificateId).build()
  }
}

<<<<<<< HEAD
<<<<<<< HEAD




>>>>>>> f58fef48 (initial commit)
=======
protected fun Certificate.toInsertMutation(
=======
fun Certificate.toInsertMutation(
>>>>>>> 8b0bd33b (building)
  internalId: InternalId,
  externalId: ExternalId
): Mutation {
  return Mutation.newInsertBuilder("Certificates")
    .set("CertificateId")
    .to(internalId.value)
    .set("SubjectKeyIdentifier")
    .to(subjectKeyIdentifier.toGcloudByteArray())
    .set("NotValidBefore")
    .to(notValidBefore.toGcloudTimestamp())
    .set("NotValidAfter")
    .to(notValidAfter.toGcloudTimestamp())
    .set("RevocationState")
    .toProtoEnum(revocationState)
    .set("CertificateDetails")
    .toProtoBytes(details)
    .set("CertificateDetailsJson")
    .toProtoJson(details)
    .build()
}
>>>>>>> bf3c1bb3 (getting there)
