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

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.computationParticipant
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException

private val SELECT_EXPRESSIONS =
  listOf(
    "ComputationParticipants.MeasurementConsumerId",
    "ComputationParticipants.MeasurementId",
    "ComputationParticipants.DuchyId",
    "ComputationParticipants.CertificateId",
    "ComputationParticipants.State",
    "ComputationParticipants.ParticipantDetails",
    "ComputationParticipants.ParticipantDetailsJson",
    "ComputationParticipants.UpdateTime",
    "Measurements.ExternalMeasurementId",
    "Measurements.ExternalComputationId",
    "MeasurementConsumers.ExternalMeasurementConsumerId",
    "DuchyCertificates.ExternalDuchyCertificateId"
  )

private val FROM_CLAUSE =
  """
  FROM ComputationParticipants
    JOIN MeasurementConsumers USING (MeasurementConsumerId)
    JOIN MEASUREMENTS USING(MeasurementConsumerId, MeasurementId)
    LEFT JOIN DuchyCertificates ON DuchyCertificates.DuchyId = ComputationParticipants.DuchyId 
         AND DuchyCertificates.CertificateId = ComputationParticipants.CertificateId
  """.trimIndent()

class ComputationParticipantReader() : BaseSpannerReader<ComputationParticipantReader.Result>() {

  data class Result(
    val computationParticipant: ComputationParticipant,
    val measurementId: Long,
    val measurementConsumerId: Long
  )
  override val builder: Statement.Builder = initBuilder()

  /** Fills [builder], returning this [ComputationParticipantReader] for chaining. */
  fun fillStatementBuilder(fill: Statement.Builder.() -> Unit): ComputationParticipantReader {
    builder.fill()
    return this
  }

  suspend fun readWithIdsOrNull(
    readContext: AsyncDatabaseClient.ReadContext,
    externalComputationId: ExternalId,
    duchyId: InternalId
  ): Result? {
    return fillStatementBuilder {
        appendClause("WHERE Measurements.externalComputationId = @externalComputationId")
        appendClause("AND ComputationParticipants.duchyId = @duchyId")
        bind("externalComputationId").to(externalComputationId.value)
        bind("duchyId").to(duchyId.value)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }

  override suspend fun translate(struct: Struct): Result =
    Result(
      buildComputationParticipant(struct),
      struct.getLong("MeasurementId"),
      struct.getLong("MeasurementConsumerId")
    )

  private fun buildComputationParticipant(struct: Struct): ComputationParticipant =
  // TOOO(@uakyol) : Also populate failure_log_entry and api_version.
  computationParticipant {
    externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
    externalMeasurementId = struct.getLong("ExternalMeasurementId")
    externalDuchyId = checkNotNull(DuchyIds.getExternalId(struct.getLong("DuchyId")))
    externalComputationId = struct.getLong("ExternalComputationId")
    if (!struct.isNull("ExternalDuchyCertificateId")) {
      externalDuchyCertificateId = struct.getLong("ExternalDuchyCertificateId")
    }
    updateTime = struct.getTimestamp("UpdateTime").toProto()
    state = struct.getProtoEnum("State", ComputationParticipant.State::forNumber)
    details = struct.getProtoMessage("ParticipantDetails", ComputationParticipant.Details.parser())
  }
  private fun initBuilder(): Statement.Builder {
    val sqlBuilder = StringBuilder("SELECT\n")
    SELECT_EXPRESSIONS.joinTo(sqlBuilder, ", ")
    return Statement.newBuilder(sqlBuilder.toString()).appendClause(FROM_CLAUSE)
  }
}

suspend fun readComputationParticipantState(
  readContext: AsyncDatabaseClient.ReadContext,
  measurementConsumerId: InternalId,
  measurementId: InternalId,
  duchyId: InternalId
): ComputationParticipant.State {
  val column = "State"
  return readContext.readRow(
      "ComputationParticipants",
      Key.of(measurementConsumerId.value, measurementId.value, duchyId.value),
      listOf(column)
    )
    ?.let { struct -> struct.getProtoEnum(column, ComputationParticipant.State::forNumber) }
    ?: throw KingdomInternalException(
      KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND
    ) { "ComputationParticipant not found $duchyId" }
}

suspend fun computationParticipantsInState(
  readContext: AsyncDatabaseClient.ReadContext,
  duchyIds: List<InternalId>,
  measurementConsumerId: InternalId,
  measurementId: InternalId,
  state: ComputationParticipant.State
): Boolean {

  val wrongState =
    duchyIds
      .asFlow()
      .map {
        readComputationParticipantState(readContext, measurementConsumerId, measurementId, it)
      }
      .firstOrNull { it != state }

  return wrongState == null
}
