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

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.SpannerException
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.insertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader

/**
 * Creates a measurement in the database.
 *
 * Throw KingdomInternalException with code CERT_SUBJECT_KEY_ID_ALREADY_EXISTS when executed if
 * subjectKeyIdentifier of [measurement] collides with a measurement already in the database.
 */
class CreateMeasurement(private val measurement: Measurement) :
  SpannerWriter<Measurement, Measurement>() {


  override suspend fun TransactionScope.runTransaction(): Measurement {
    val measurementId = idGenerator.generateInternalId()
    val externalMapId = idGenerator.generateExternalId()
    measurement.toInsertMutation(measurementId).bufferTo(transactionContext)
    return measurement.toBuilder().setExternalMeasurementId(externalMapId.value).build()
  }

  override fun ResultScope<Measurement>.buildResult(): Measurement {
    return checkNotNull(transactionResult)
  }


  override suspend fun handleSpannerException(e: SpannerException): Measurement? {
    when (e.errorCode) {
      ErrorCode.ALREADY_EXISTS ->
        throw KingdomInternalException(
          KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS
        )
      else -> throw e
    }
  }
}

fun Measurement.toInsertMutation(internalId: InternalId): Mutation {
  return insertMutation("Measurements") {
    set("MeasurementId" to internalId.value)
    set("SubjectKeyIdentifier" to subjectKeyIdentifier.toGcloudByteArray())
    set("NotValidBefore" to notValidBefore.toGcloudTimestamp())
    set("NotValidAfter" to notValidAfter.toGcloudTimestamp())
    set("RevocationState" to revocationState)
    set("MeasurementDetails" to details)
    setJson("MeasurementDetailsJson" to details)
  }
}
