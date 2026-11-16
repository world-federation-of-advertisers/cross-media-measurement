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
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.edpaggregator.RequisitionSpecs
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions.EventGroupDetails

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
 * @param privateEncryptionKey The DataProvider’s private key used to decrypt requisition data.
 * @param allowMixedEventGroupSelectors Whether configured mapping permits mixed selector forms.
 */
class RequisitionsValidator(
  private val privateEncryptionKey: PrivateKeyHandle,
  private val allowMixedEventGroupSelectors: Boolean,
) {
  fun validateMeasurementSpec(requisition: Requisition): MeasurementSpec {
    return try {
      val measurementSpec: MeasurementSpec = RequisitionSpecs.readMeasurementSpec(requisition)

      if (measurementSpec.reportingMetadata.report.isBlank()) {
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
    } catch (e: IllegalArgumentException) {
      logger.severe("Invalid measurement spec for ${requisition.name}: ${e.message}")
      throw InvalidRequisitionException(
        listOf(requisition),
        refusal {
          justification = Refusal.Justification.SPEC_INVALID
          message = "Invalid MeasurementSpec"
        },
        e,
      )
    }
  }

  fun getRequisitionSpec(requisition: Requisition): RequisitionSpec {
    return RequisitionSpecs.readRequisitionSpec(requisition, privateEncryptionKey)
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
    } catch (e: IllegalArgumentException) {
      logger.severe("Invalid requisition spec for ${requisition.name}: ${e.message}")
      throw InvalidRequisitionException(
        listOf(requisition),
        refusal {
          justification = Refusal.Justification.SPEC_INVALID
          message = "Invalid RequisitionSpec"
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

  /** Requires a nonblank selector for each event group and, unless enabled, one selector form. */
  fun validateEventGroupSelectors(eventGroupMap: Map<String, EventGroupDetails>) {
    require(eventGroupMap.isNotEmpty()) { "eventGroupMap must not be empty" }

    // The Kingdom may populate entity_type without an entity_id.
    val hasMeaningfulEntityKey: (EventGroupDetails) -> Boolean = {
      it.hasEntityKey() && it.entityKey.entityId.isNotBlank()
    }
    if (
      !allowMixedEventGroupSelectors &&
        eventGroupMap.values.any(hasMeaningfulEntityKey) &&
        !eventGroupMap.values.all(hasMeaningfulEntityKey)
    ) {
      throw InconsistentEventGroupSelectorsException(
        "Inconsistent selectors: if any event group has entity_key with entity_id, all must"
      )
    }
    if (
      eventGroupMap.values.any { !hasMeaningfulEntityKey(it) && it.eventGroupReferenceId.isBlank() }
    ) {
      throw InconsistentEventGroupSelectorsException(
        "Every event group must have a nonblank entity_key.entity_id or event_group_reference_id"
      )
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

/** Thrown when event group selectors (entity_key vs event_group_reference_id) are inconsistent. */
class InconsistentEventGroupSelectorsException(message: String) : Exception(message)
