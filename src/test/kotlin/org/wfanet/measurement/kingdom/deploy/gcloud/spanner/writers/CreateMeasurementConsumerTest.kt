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
import org.wfanet.measurement.common.identity.testing.DeterministicIdGenerator
import org.wfanet.measurement.common.identity.testing.copy
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

private const val ID_SEED = 1L

@RunWith(JUnit4::class)
class CreateMeasurementConsumerTest : KingdomDatabaseTestBase() {

  @Test
  fun success() =
    runBlocking<Unit> {
      val measurementConsumer = MeasurementConsumer.newBuilder().build()
      val usedIdGenerator = DeterministicIdGenerator(ID_SEED)
      CreateMeasurementConsumer(measurementConsumer).execute(databaseClient, usedIdGenerator.copy())

      val expectedInternalCertificateId = usedIdGenerator.generateInternalId()

      val expectedInternalMeasurementConsumerId = usedIdGenerator.generateInternalId()
      val expectedExternalMeasurementConsumerId = usedIdGenerator.generateExternalId()

      val expectedExternalMeasurementConsumerCertificateId = usedIdGenerator.generateExternalId()

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
            .to(measurementConsumer.preferredCertificate.subjectKeyIdentifier.toGcloudByteArray())
            .set("NotValidBefore")
            .to(measurementConsumer.preferredCertificate.notValidBefore.toGcloudTimestamp())
            .set("NotValidAfter")
            .to(measurementConsumer.preferredCertificate.notValidAfter.toGcloudTimestamp())
            .set("RevocationState")
            .toProtoEnum(measurementConsumer.preferredCertificate.revocationState)
            .set("CertificateDetails")
            .toProtoBytes(measurementConsumer.preferredCertificate.details)
            .set("CertificateDetailsJson")
            .toProtoJson(measurementConsumer.preferredCertificate.details)
            .build()
        )

      val measurementConsumers =
        databaseClient
          .singleUse(TimestampBound.strong())
          .executeQuery(Statement.of("SELECT * FROM MeasurementConsumers"))
          .toList()

      assertThat(measurementConsumers)
        .containsExactly(
          Struct.newBuilder()
            .set("MeasurementConsumerId")
            .to(expectedInternalMeasurementConsumerId.value)
            .set("PublicKeyCertificateId")
            .to(expectedInternalCertificateId.value)
            .set("ExternalMeasurementConsumerId")
            .to(expectedExternalMeasurementConsumerId.value)
            .set("MeasurementConsumerDetails")
            .toProtoBytes(measurementConsumer.details)
            .set("MeasurementConsumerDetailsJson")
            .toProtoJson(measurementConsumer.details)
            .build()
        )

    val measurementConsumerCertificates =
        databaseClient
          .singleUse(TimestampBound.strong())
          .executeQuery(Statement.of("SELECT * FROM MeasurementConsumerCertificates"))
          .toList()

      assertThat(measurementConsumerCertificates)
        .containsExactly(
          Struct.newBuilder()
            .set("MeasurementConsumerId")
            .to(expectedInternalMeasurementConsumerId.value)
            .set("CertificateId")
            .to(expectedInternalCertificateId.value)
            .set("ExternalMeasurementConsumerCertificateId")
            .to(expectedExternalMeasurementConsumerCertificateId.value)
            .build()
        )
    }
}
