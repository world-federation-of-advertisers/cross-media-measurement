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

import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader

suspend fun checkValidCertificate(
  measurementConsumerCertificateId: Long,
  measurementConsumerId: Long,
  transactionContext: AsyncDatabaseClient.TransactionContext
): InternalId? {
  val reader =
    CertificateReader(CertificateReader.ParentType.MEASUREMENT_CONSUMER)
      .bindWhereClause(
        ExternalId(measurementConsumerId),
        ExternalId(measurementConsumerCertificateId)
      )

  return reader.execute(transactionContext).singleOrNull()?.let {
    if (!it.isValid) {
      throw KingdomInternalException(KingdomInternalException.Code.CERTIFICATE_IS_INVALID)
    } else it.certificateId
  }
    ?: throw KingdomInternalException(KingdomInternalException.Code.CERTIFICATE_NOT_FOUND)
}
