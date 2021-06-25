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

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TimestampBound
import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

@RunWith(JUnit4::class)
class CreateCertificateTest : KingdomDatabaseTestBase() {

  @Test
  fun success() =
    runBlocking<Unit> {
      val certificate = Certificate.newBuilder().build()
      val usedIdGenerator = FixedIdGenerator()
      CreateCertificate(certificate).execute(databaseClient, usedIdGenerator)

      val expectedInternalCertificateId = usedIdGenerator.generateInternalId()

      val certificates =
        databaseClient
          .singleUse(TimestampBound.strong())
          .executeQuery(Statement.of("SELECT * FROM Certificates"))
          .toList()

      assertThat(certificates)
        .containsExactly(
          Struct.newBuilder()
            .set("CertificateId")
            .to(expectedInternalCertificateId.value)
            .set("SubjectKeyIdentifier")
            .to(certificate.subjectKeyIdentifier.toGcloudByteArray())
            .set("NotValidBefore")
            .to(certificate.notValidBefore.toGcloudTimestamp())
            .set("NotValidAfter")
            .to(certificate.notValidAfter.toGcloudTimestamp())
            .set("RevocationState")
            .toProtoEnum(certificate.revocationState)
            .set("CertificateDetails")
            .toProtoBytes(certificate.details)
            .set("CertificateDetailsJson")
            .toProtoJson(certificate.details)
            .build()
        )
    }
}
