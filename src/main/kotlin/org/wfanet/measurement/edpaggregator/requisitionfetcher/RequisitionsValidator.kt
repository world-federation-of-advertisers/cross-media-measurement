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

class InvalidRequisitionException(
  val requisitions: List<Requisition>,
  val refusal: Refusal,
  cause: Exception? = null,
) : Exception("Invalid requisition: ${refusal.justification}: ${refusal.message}", cause)

/**
 * Validates a [Requisition], ensuring it is well-formed and ready for processing.
 *
 * Only requisitions with **permanently fatal errors** (e.g., invalid or unparseable
 * [MeasurementSpec], or missing `reportId`) are refused.
 *
 * @param throttler Used to throttle outgoing gRPC requests.
 * @param privateEncryptionKey The DataProvider’s private key used to decrypt requisition data.
 */
class RequisitionsValidator(private val privateEncryptionKey: PrivateKeyHandle) {
  fun validateMeasurementSpec(requisition: Requisition): MeasurementSpec {
    return try {
      val measurementSpec: MeasurementSpec = requisition.measurementSpec.unpack()

      if (
        requisition.measurementSpec.unpack<MeasurementSpec>().reportingMetadata.report.isBlank()
      ) {
        throw InvalidRequisitionException(
          listOf(requisition),
          refusal {
            justification = Refusal.Justification.SPEC_INVALID
            message = "Unable to parse MeasurementSpec"
          },
        )
      }

      measurementSpec
    } catch (e: InvalidProtocolBufferException) {
      logger.severe("Unable to parse measurement spec for ${requisition.name}: ${e.message}")
      throw InvalidRequisitionException(
        listOf(requisition),
        refusal {
          justification = Refusal.Justification.SPEC_INVALID
          message = "Unable to parse MeasurementSpec"
        },
        e,
      )
    }
  }

  fun getRequisitionSpec(requisition: Requisition): RequisitionSpec {
    return decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
      .unpack()
  }

  fun validateRequisitionSpec(requisition: Requisition): RequisitionSpec {
    return try {
      getRequisitionSpec(requisition)
    } catch (e: GeneralSecurityException) {
      logger.severe("RequisitionSpec decryption failed for ${requisition.name}: ${e.message}")
      throw InvalidRequisitionException(
        listOf(requisition),
        refusal {
          justification = Refusal.Justification.CONSENT_SIGNAL_INVALID
          message = "Unable to decrypt RequisitionSpec"
        },
        e,
      )
    } catch (e: InvalidProtocolBufferException) {
      logger.severe("Unable to parse requisition spec for ${requisition.name}: ${e.message}")
      throw InvalidRequisitionException(
        listOf(requisition),
        refusal {
          justification = Refusal.Justification.SPEC_INVALID
          message = "Unable to parse RequisitionSpec"
        },
        e,
      )
    }
  }

  fun validateModelLines(groupedRequisitions: List<GroupedRequisitions>, reportId: String) {
    val modelLine = groupedRequisitions.first().modelLine
    val foundInvalidModelLine =
      groupedRequisitions.firstOrNull { it.modelLine != modelLine } != null
    if (foundInvalidModelLine) {
      logger.severe("Report $reportId cannot contain multiple model lines")
      val invalidRequisitions = mutableListOf<Requisition>()
      groupedRequisitions.forEach {
        it.requisitionsList.forEach { it ->
          val requisition = it.requisition.unpack(Requisition::class.java)
          invalidRequisitions.add(requisition)
        }
      }
      throw InvalidRequisitionException(
        invalidRequisitions.toList(),
        refusal {
          justification = Requisition.Refusal.Justification.UNFULFILLABLE
          message = "Report $reportId cannot contain multiple model lines"
        },
      )
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
