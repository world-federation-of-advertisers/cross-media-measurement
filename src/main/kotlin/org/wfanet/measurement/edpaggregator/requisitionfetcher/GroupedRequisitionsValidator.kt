/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.requisitionfetcher

import com.google.protobuf.InvalidProtocolBufferException
import java.security.GeneralSecurityException
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions

/**
 * Validates a requisition. Only refuses requisitions with permanently fatal errors.
 *
 * @param requisitionsClient The gRPC client used to interact with requisitions.
 * @param throttler used to throttle gRPC requests.
 * @param privateEncryptionKey The DataProvider's decryption key used for decrypting requisition
 *   data.
 */
class GroupedRequisitionsValidator(
  private val requisitionsClient: RequisitionsCoroutineStub,
  private val throttler: Throttler,
  private val privateEncryptionKey: PrivateKeyHandle,
) {
  fun validateMeasurementSpec(requisition: Requisition): MeasurementSpec? {
    val measurementSpec: MeasurementSpec? =
      try {
        requisition.measurementSpec.unpack()
      } catch (e: InvalidProtocolBufferException) {
        logger.info("Unable to parse measurement spec for ${requisition.name}: ${e.message}")
        refuseRequisition(
          requisition.name,
          refusal {
            justification = Refusal.Justification.SPEC_INVALID
            message = "Unable to parse MeasurementSpec"
          },
        )
        null
      }
    return measurementSpec
  }

  fun validateRequisitionSpec(requisition: Requisition): RequisitionSpec? {
    val requisitionSpec: RequisitionSpec? =
      try {
        decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey).unpack()
      } catch (e: GeneralSecurityException) {
        logger.info("RequisitionSpec decryption failed for ${requisition.name}: ${e.message}")
        refuseRequisition(
          requisition.name,
          refusal {
            justification = Refusal.Justification.CONSENT_SIGNAL_INVALID
            message = "Unable to decrypt RequisitionSpec"
          },
        )
        null
      } catch (e: InvalidProtocolBufferException) {
        logger.info("Unable to parse requisition spec for ${requisition.name}: ${e.message}")
        refuseRequisition(
          requisition.name,
          refusal {
            justification = Refusal.Justification.SPEC_INVALID
            message = "Unable to parse RequisitionSpec"
          },
        )
        null
      }
    return requisitionSpec
  }

  fun validateModelLines(groupedRequisitions: List<GroupedRequisitions>, reportId: String): Boolean {
    val modelLine = groupedRequisitions.first().modelLine
    val foundInvalidModelLine = groupedRequisitions.firstOrNull { it.modelLine != modelLine } != null
    if (foundInvalidModelLine) {
      logger.info("Report $reportId cannot contain multiple model lines")
      groupedRequisitions.forEach {
        val requisition =
          it.requisitionsList.single().requisition.unpack(Requisition::class.java)
        runBlocking {
          refuseRequisition(
            requisition.name,
            refusal {
              justification = Requisition.Refusal.Justification.UNFULFILLABLE
              message = "Report $reportId cannot contain multiple model lines"
            },
          )
        }
      }
      return false
    }
    return true
  }

  private fun refuseRequisition(name: String, refusal: Refusal) = runBlocking {
    throttler.onReady {
      requisitionsClient.refuseRequisition(
        refuseRequisitionRequest {
          this.name = name
          this.refusal = refusal
        }
      )
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
