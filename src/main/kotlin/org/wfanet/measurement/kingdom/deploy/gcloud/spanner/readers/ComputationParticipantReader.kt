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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.internal.kingdom.ComputationParticipant

class ComputationParticipantReader() : SpannerReader<ComputationParticipantReader.Result>() {

  data class Result(val computaitonParticipant: ComputationParticipant)

  override val baseSql: String =
    """
    SELECT
      ComputationParticipants.MeasurementConsumerId,
      ComputationParticipants.MeasurementId,
      ComputationParticipants.DuchyId,
      ComputationParticipants.CertificateId,
      ComputationParticipants.State,
      ComputationParticipants.ParticipantDetails,
      ComputationParticipants.ParticipantDetailsJson
    FROM ComputationParticipants
    """.trimIndent()

  override val externalIdColumn: String
    get() = error("This isn't supported.")

  suspend fun readInternalIdsOrNull(
    readContext: AsyncDatabaseClient.ReadContext,
    measurementId: Long,
    measurementConsumerId: Long,
    duchyId: Long
  ): Result? {
    return withBuilder {
        appendClause("WHERE measurementId = @measurementId")
        appendClause("AND measurementConsumerId = @measurementConsumerId")
        appendClause("AND duchyId = @duchyId")
        bind("measurementId").to(measurementId)
        bind("measurementConsumerId").to(measurementConsumerId)
        bind("duchyId").to(duchyId)

        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }

  override suspend fun translate(struct: Struct): Result =
    Result(ComputationParticipant.newBuilder().build())
}
