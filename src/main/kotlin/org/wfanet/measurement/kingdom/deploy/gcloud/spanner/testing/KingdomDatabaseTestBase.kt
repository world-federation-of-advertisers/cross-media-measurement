// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Mutation
import java.time.Instant
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.testing.UsingSpannerEmulator

abstract class KingdomDatabaseTestBase : UsingSpannerEmulator(KINGDOM_SCHEMA) {
  private suspend fun write(mutation: Mutation) = databaseClient.write(mutation)

  protected suspend fun insertMeasurementConsumer(
    measurementConsumerId: Long,
    externalMeasurementConsumerId: Long,
    publicKeyCertificateId: Long
  ) {
    write(
      Mutation.newInsertBuilder("MeasurementConsumers")
        .set("MeasurementConsumerId")
        .to(measurementConsumerId)
        .set("PublicKeyCertificateId")
        .to(publicKeyCertificateId)
        .set("ExternalMeasurementConsumerId")
        .to(externalMeasurementConsumerId)
        .set("MeasurementConsumerDetails")
        .to(ByteArray.copyFrom(""))
        .set("MeasurementConsumerDetailsJson")
        .to("irrelevant-measurement-consumer-details-json")
        .build()
    )
  }
  protected suspend fun insertMeasurementConsumerCertificate(
    certificateId: Long,
    notValidBefore: Instant = Instant.EPOCH,
    notValidAfter: Instant = Instant.EPOCH
  ) {
    write(
      Mutation.newInsertBuilder("Certificates")
        .set("CertificateId")
        .to(certificateId)
        .set("SubjectKeyIdentifier")
        .to(ByteArray.copyFrom(""))
        .set("CertificateDetails")
        .to(ByteArray.copyFrom(""))
        .set("CertificateDetailsJson")
        .to("irrelevant-certificate-details-json")
        .set("RevocationState")
        .to(0L)
        .set("NotValidBefore")
        .to(notValidBefore.toGcloudTimestamp())
        .set("NotValidAfter")
        .to(notValidAfter.toGcloudTimestamp())
        .build()
    )
  }
}
