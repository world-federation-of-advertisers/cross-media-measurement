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
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions

/**
 * Validates a requisition. Only refuses requisitions with permanently fatal errors.
 *
 * @param throttler used to throttle gRPC requests.
 * @param privateEncryptionKey The DataProvider's decryption key used for decrypting requisition
 *   data.
 */
// TODO(@marcopremier): Update constructor not to use callback but simply throwing an exception
// if the requisition is not valid
class RequisitionsValidator(
  private val privateEncryptionKey: PrivateKeyHandle,
  private val fatalRequisitionErrorPredicate: (requisition: Requisition, Refusal) -> Unit,
) {
  fun validateMeasurementSpec(requisition: Requisition): MeasurementSpec? {
    val measurementSpec: MeasurementSpec? =
      try {
        requisition.measurementSpec.unpack()
      } catch (e: InvalidProtocolBufferException) {
        logger.severe("Unable to parse measurement spec for ${requisition.name}: ${e.message}")
        fatalRequisitionErrorPredicate(
          requisition,
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
        logger.severe("RequisitionSpec decryption failed for ${requisition.name}: ${e.message}")
        fatalRequisitionErrorPredicate(
          requisition,
          refusal {
            justification = Refusal.Justification.CONSENT_SIGNAL_INVALID
            message = "Unable to decrypt RequisitionSpec"
          },
        )
        null
      } catch (e: InvalidProtocolBufferException) {
        logger.severe("Unable to parse requisition spec for ${requisition.name}: ${e.message}")
        fatalRequisitionErrorPredicate(
          requisition,
          refusal {
            justification = Refusal.Justification.SPEC_INVALID
            message = "Unable to parse RequisitionSpec"
          },
        )
        null
      }
    return requisitionSpec
  }

  fun validateModelLines(
    groupedRequisitions: List<GroupedRequisitions>,
    reportId: String,
  ): Boolean {
    val modelLine = groupedRequisitions.first().modelLine
    val foundInvalidModelLine =
      groupedRequisitions.firstOrNull { it.modelLine != modelLine } != null
    if (foundInvalidModelLine) {
      logger.severe("Report $reportId cannot contain multiple model lines")
      groupedRequisitions.forEach {
        it.requisitionsList.forEach { it ->
          val requisition = it.requisition.unpack(Requisition::class.java)
          fatalRequisitionErrorPredicate(
            requisition,
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

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
