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

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.gcloud.spanner.updateMutation
import org.wfanet.measurement.internal.kingdom.SetParticipantRequisitionParamsRequest
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ComputationParticipantReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader

class SetParticipantRequisitionParams(private val request: SetParticipantRequisitionParamsRequest) :
  SimpleSpannerWriter<ComputationParticipant>() {

  override suspend fun TransactionScope.runTransaction(): ComputationParticipant {

    val measurementResult =
      MeasurementReader(Measurement.View.DEFAULT)
        .readExternalIdOrNull(transactionContext, ExternalId(request.externalComputationId))
        ?: throw KingdomInternalException(
          KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND
        ) { "Measurement with external computation ID ${request.externalComputationId} not found" }

    // val measurementIds = readMeasurementIds(ExternalId(request.externalComputationId))
    val duchyId =
      DuchyIds.getInternalId(request.externalDuchyId)
        ?: throw KingdomInternalException(KingdomInternalException.Code.DUCHY_NOT_FOUND)
    val duchyCertificateId = 5L
    // readDuchyCertificateId(ExternalId(request.externalDuchyCertificateId))

    val computaitonParticipant =
      ComputationParticipantReader()
        .readInternalIdsOrNull(
          transactionContext,
          measurementResult.measurementId,
          measurementResult.measurementConsumerId,
          duchyId
        )?.computaitonParticipant
        ?: throw KingdomInternalException(
          KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND
        ) {
          "ComputationParticipant for external computation ID ${request.externalComputationId} " +
            "and external duchy Id ${request.externalDuchyId} not found"
        }

    if (computaitonParticipant.state != ComputationParticipant.State.CREATED) {
      throw KingdomInternalException(
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_IN_UNEXPECTED_STATE
      ) {
        "ComputationParticipant for external computation Id ${request.externalComputationId} " +
          "and external duchy Id ${request.externalDuchyId} has the wrong state. " +
          "It should have been in state CREATED but was in state ${computaitonParticipant.state}"
      }
    }

    val participantDetails =
      computaitonParticipant
        .details
        .toBuilder()
        .apply { liquidLegionsV2 = request.liquidLegionsV2 }
        .build()

    updateMutation("ComputationParticipants") {
        set("MeasurementId" to measurementResult.measurementId)
        set("MeasurementConsumerId" to measurementResult.measurementConsumerId)
        set("DuchyId" to duchyId)
      }
      .bufferTo(transactionContext)
    return ComputationParticipant.newBuilder().build()

    // set("CertificateId" to duchyCertificateId)
    // set("UpdateTime" to Value.COMMIT_TIMESTAMP)
    // set("State" to ComputationParticipant.State.REQUISITION_PARAMS_SET)
    // set("ParticipantDetails" to participantDetails)
    // setJson("ParticipantDetailsJson" to participantDetails)

    // if (allComputationParticipantsInState(
    //     measurementIds.measurementId,
    //     measurementIds.measurementConsumerId,
    //     ComputationParticipant.State.REQUISITION_PARAMS_SET
    //   )
    // ) {
    //   updateMeasurementState(
    //     measurementIds.measurementId,
    //     measurementIds.measurementConsumerId,
    //     Measurement.State.PENDING_REQUISITION_FULFILLMENT
    //   )
    // }
  }

  // private suspend fun TransactionScope.readMeasurementIds(
  //   externalComputationId: ExternalId
  // ): MeasurementIds {
  //   val columns = Arrays.asList("MeasurementId", "MeasurementConsumerId")
  //   return transactionContext.readRowUsingIndex(
  //       "Measurements",
  //       "MeasurementsByExternalComputationId",
  //       Key.of(externalComputationId.value),
  //       columns
  //     )
  //     ?.let { struct ->
  //       MeasurementIds(
  //         InternalId(struct.getLong(columns.get(0))),
  //         InternalId(struct.getLong(columns.get(1)))
  //       )
  //     }
  //     ?: throw KingdomInternalException(KingdomInternalException.Code.MEASUREMENT_NOT_FOUND) {
  //       "Measurement with external computation ID $externalComputationId not found"
  //     }
  // }
}
