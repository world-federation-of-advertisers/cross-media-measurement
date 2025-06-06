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

import com.google.protobuf.Any
import com.google.protobuf.InvalidProtocolBufferException
import com.google.type.Interval
import io.grpc.StatusException
import java.security.GeneralSecurityException
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.requisitionEntry
import org.wfanet.measurement.edpaggregator.v1alpha.groupedRequisitions
import java.util.UUID

/**
 * An interface to group a list of requisitions.
 *
 * This class provides functionality to categorize a collection of [Requisition] objects into
 * groups, facilitating efficient execution.
 *
 * @param privateEncryptionKey The DataProvider's decryption key used for decrypting requisition
 *   data.
 * @param eventGroupsClient The gRPC client used to interact with event groups.
 *
 * TODO(#2373): Everything that the [ResultsFulfiller] verifies should be pre-verified here also.
 */
abstract class RequisitionGrouper(
  private val privateEncryptionKey: PrivateKeyHandle,
  private val eventGroupsClient: EventGroupsCoroutineStub,
  private val requisitionsClient: RequisitionsCoroutineStub,
  private val throttler: Throttler,
) {

  /**
   * Groups a list of disparate [Requisition] objects for execution.
   *
   * This method takes in a list of [Requisition] objects, maps them to their respective groups, and
   * then combines these groups into a single list of [GroupedRequisitions].
   *
   * @param requisitions A list of [Requisition] objects to be grouped.
   * @return A list of [GroupedRequisitions] containing the categorized [Requisition] objects.
   */
  fun groupRequisitions(requisitions: List<Requisition>): List<GroupedRequisitions> {
    println("~~~~~~~~~~~~~~~ AA: ${requisitions}")
    val mappedRequisitions = requisitions.mapNotNull { mapRequisition(it) }
    println("~~~~~~~~~~~~~~~ AAA")
    return combineGroupedRequisitions(mappedRequisitions)
  }

  /** Function to be implemented to combine [GroupedRequisition]s for optimal execution. */
  protected abstract fun combineGroupedRequisitions(
    groupedRequisitions: List<GroupedRequisitions>
  ): List<GroupedRequisitions>

  /* Maps a single [Requisition] to a single [GroupedRequisition]. */
  private fun mapRequisition(requisition: Requisition): GroupedRequisitions? {
    println("~~~~~~~~~~~~~~~~~~` map requi1")
    val measurementSpec: MeasurementSpec? =
      try {
        requisition.measurementSpec.unpack()
      } catch (e: InvalidProtocolBufferException) {
        logger.info("Unable to parse measurement spec for ${requisition.name}: ${e.message}")
        runBlocking {
          refuseRequisition(
            requisition.name,
            refusal {
              justification = Refusal.Justification.SPEC_INVALID
              message = "Unable to parse MeasurementSpec"
            },
          )
        }
        return null
      }
    println("~~~~~~~~~~~~~~~~~~` map requi2")
    if (measurementSpec == null) {
      logger.info("Measurement Spec is null for ${requisition.name}")
      runBlocking {
        refuseRequisition(
          requisition.name,
          refusal {
            justification = Refusal.Justification.SPEC_INVALID
            message = "MeasurementSpec is null"
          },
        )
      }
      return null
    }
    println("~~~~~~~~~~~~~~~~~~` map requi3: ${requisition}")
    val requisitionSpec: RequisitionSpec? =
      try {
        decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey).unpack()
      } catch (e: GeneralSecurityException) {
        logger.info("RequisitionSpec decryption failed for ${requisition.name}: ${e.message}")
        runBlocking {
          refuseRequisition(
            requisition.name,
            refusal {
              justification = Refusal.Justification.CONSENT_SIGNAL_INVALID
              message = "Unable to decrypt RequisitionSpec"
            },
          )
        }
        return null
      } catch (e: InvalidProtocolBufferException) {
        logger.info("Unable to parse requisition spec for ${requisition.name}: ${e.message}")
        runBlocking {
          refuseRequisition(
            requisition.name,
            refusal {
              justification = Refusal.Justification.SPEC_INVALID
              message = "Unable to parse RequisitionSpec"
            },
          )
        }
        return null
      }
    println("~~~~~~~~~~~~~~~~~~` map requi4")
    if (requisitionSpec == null) {
      logger.info("Requisition Spec is null for ${requisition.name}")
      runBlocking {
        refuseRequisition(
          requisition.name,
          refusal {
            justification = Refusal.Justification.SPEC_INVALID
            message = "RequisitionSpec is null"
          },
        )
      }
      return null
    }
    println("~~~~~~~~~~~~~~~~~~` map requi5")
    val eventGroupMap = mutableMapOf<String, String>()
    val collectionIntervalsMap = mutableMapOf<String, Interval>()
    for (eventGroupEntry in requisitionSpec!!.events.eventGroupsList) {
      val eventGroupName = eventGroupEntry.key
      val eventGroup = runBlocking { getEventGroup(eventGroupName) }
      if (eventGroup == null) {
        logger.info("Unable to fetch event group $eventGroupName for ${requisition.name}")
        runBlocking {
          refuseRequisition(
            requisition.name,
            refusal {
              justification = Refusal.Justification.SPEC_INVALID
              message = "Found duplicate event groups within a requisition spec"
            },
          )
        }
        return null
      }
      println("~~~~~~~~~~~~~~~~~~` map requi6")
      val eventGroupReferenceId = eventGroup!!.eventGroupReferenceId
      eventGroupMap[eventGroupName] = eventGroupReferenceId
      if (eventGroupName in collectionIntervalsMap) {
        logger.info(
          "Identical event group names not allowed in same requisition: ${requisition.name}"
        )
        runBlocking {
          refuseRequisition(
            requisition.name,
            refusal {
              justification = Refusal.Justification.UNFULFILLABLE
              message = "Found duplicate event groups within a requisition spec"
            },
          )
        }
        return null
      }
      collectionIntervalsMap[eventGroupReferenceId] = eventGroupEntry.value.collectionInterval
    }
    println("~~~~~~~~~~~~~~~~~~` map requi7")
    return groupedRequisitions {
      id = UUID.randomUUID().toString()
      modelLine = measurementSpec!!.modelLine
      this.requisitions += requisitionEntry { this.requisition = Any.pack(requisition) }
      this.eventGroupMap +=
        eventGroupMap.toList().map {
          eventGroupMapEntry {
            eventGroup = it.first
            eventGroupReferenceId = it.second
          }
        }
      collectionIntervals.putAll(collectionIntervalsMap)
    }
  }

  private suspend fun getEventGroup(name: String): EventGroup? {
    return try {
      throttler.onReady {
        eventGroupsClient.getEventGroup(getEventGroupRequest { this.name = name })
      }
    } catch (e: StatusException) {
      logger.info("Error getting EventGroup: $name")
      null
    }
  }

  protected suspend fun refuseRequisition(name: String, reason: Refusal) {
    try {
      throttler.onReady {
        requisitionsClient.refuseRequisition(
          refuseRequisitionRequest {
            this.name = name
            refusal = reason
          }
        )
      }
    } catch (e: StatusException) {
      throw Exception("Error refusing Requisition: $name", e)
    }
  }

  companion object {
    @JvmStatic
    protected val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
