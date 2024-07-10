/*
 * Copyright 2021 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.duchy.testing

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import kotlin.random.Random
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.internal.duchy.externalRequisitionKey
import org.wfanet.measurement.internal.duchy.requisitionDetails
import org.wfanet.measurement.internal.duchy.requisitionMetadata
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.Requisition
import org.wfanet.measurement.system.v1alpha.RequisitionKey
import org.wfanet.measurement.system.v1alpha.requisition

data class TestRequisition(
  val externalRequisitionId: String,
  val serializedMeasurementSpecProvider: () -> ByteString,
) {
  val requisitionSpecHash: ByteString = Random.Default.nextBytes(32).toByteString()
  val nonce: Long = Random.Default.nextLong()
  val nonceHash = Hashing.hashSha256(nonce)
  val requisitionFingerprint
    get() = Hashing.hashSha256(serializedMeasurementSpecProvider().concat(requisitionSpecHash))

  fun toSystemRequisition(
    globalId: String,
    state: Requisition.State,
    externalDuchyId: String = "",
  ) = requisition {
    name = RequisitionKey(globalId, externalRequisitionId).toName()
    requisitionSpecHash = this@TestRequisition.requisitionSpecHash
    nonceHash = this@TestRequisition.nonceHash
    this.state = state
    if (externalDuchyId.isNotBlank()) {
      fulfillingComputationParticipant =
        ComputationParticipantKey(globalId, externalDuchyId).toName()
    }
    if (state == Requisition.State.FULFILLED) {
      nonce = this@TestRequisition.nonce
    }
  }

  fun toRequisitionMetadata(state: Requisition.State, externalDuchyId: String = "") =
    requisitionMetadata {
      externalKey = externalRequisitionKey {
        externalRequisitionId = this@TestRequisition.externalRequisitionId
        requisitionFingerprint = this@TestRequisition.requisitionFingerprint
      }
      details = requisitionDetails {
        nonceHash = this@TestRequisition.nonceHash
        if (externalDuchyId.isNotBlank()) {
          externalFulfillingDuchyId = externalDuchyId
        }
        if (state == Requisition.State.FULFILLED) {
          nonce = this@TestRequisition.nonce
        }
      }
    }
}
