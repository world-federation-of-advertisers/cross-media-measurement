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
import java.security.GeneralSecurityException
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.groupedRequisitions

/**
 * Naively returns each requisition as their own group. Generally not recommended for production use
 * cases.
 */
class SingleRequisitionGrouper(
  /** The DataProvider's decryption key. */
  private val privateEncryptionKey: PrivateKeyHandle,
  private val eventGroupsClient: EventGroupsCoroutineStub,
) : RequisitionGrouper {

  override fun groupRequisitions(requisitions: List<Requisition>): List<GroupedRequisitions> {
    return requisitions.mapNotNull { requisition ->
      val measurementSpec: MeasurementSpec? =
        try {
          requisition.measurementSpec.unpack()
        } catch (e: InvalidProtocolBufferException) {
          logger.info("Unable to parse measurement spec for ${requisition.name}: ${e.message}")
          null
        }
      if (measurementSpec == null) {
        null
      }
      val requisitionSpec: RequisitionSpec? =
        try {
          decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
            .unpack()
        } catch (e: GeneralSecurityException) {
          logger.info("RequisitionSpec decryption failed for ${requisition.name}: ${e.message}")
          null
        } catch (e: InvalidProtocolBufferException) {
          logger.info("Unable to parse requisition spec for ${requisition.name}: ${e.message}")
          null
        }
      if (requisitionSpec == null) {
        null
      }
      val eventGroupMap = mutableMapOf<String, String>()
      val collectionIntervalsMap = mutableMapOf<String, Interval>()
      for (eventGroupEntry in requisitionSpec!!.events.eventGroupsList) {
        val eventGroupName = eventGroupEntry.key
        val eventGroupReferenceId = runBlocking {
          getEventGroup(eventGroupName).eventGroupReferenceId
        }
        eventGroupMap[eventGroupName] = eventGroupReferenceId
        if (eventGroupName in collectionIntervalsMap) {
          logger.info(
            "Identical event group names not allowed in same requisition: ${requisition.name}"
          )
          null
        }
        collectionIntervalsMap[eventGroupName] = eventGroupEntry.value.collectionInterval
      }
      groupedRequisitions {
        modelLine = measurementSpec!!.modelLine
        this.requisitions += Any.pack(requisition)
        this.eventGroupMap.putAll(eventGroupMap)
        collectionIntervals.putAll(collectionIntervalsMap)
      }
    }
  }

  private suspend fun getEventGroup(name: String): EventGroup {
    return eventGroupsClient.getEventGroup(getEventGroupRequest { this.name = name })
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
