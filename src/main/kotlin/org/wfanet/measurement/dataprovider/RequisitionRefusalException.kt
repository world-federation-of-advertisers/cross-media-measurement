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

package org.wfanet.measurement.dataprovider

import org.wfanet.measurement.api.v2alpha.Requisition

/**
 * Exception thrown when a requisition is refused.
 *
 * @param justification The justification for refusing the requisition
 * @param message A message explaining the reason for refusal
 * @param cause The cause of the refusal, if any
 */
sealed class RequisitionRefusalException(
  val justification: Requisition.Refusal.Justification,
  message: String,
  cause: Throwable? = null,
) : Exception(message, cause) {

  /** Base implementation of [RequisitionRefusalException]. */
  open class Default(
    justification: Requisition.Refusal.Justification,
    message: String,
    cause: Throwable? = null,
  ) : RequisitionRefusalException(justification, message, cause)

  /** [RequisitionRefusalException] for test EventGroups. */
  class Test(
    justification: Requisition.Refusal.Justification,
    message: String,
    cause: Throwable? = null,
  ) : RequisitionRefusalException(justification, message, cause)
}

/**
 * Thrown to indicate that the specification of a [Requisition] is invalid, e.g. that
 * [Requisition.encryptedRequisitionSpec] or [Requisition.measurementSpec] is invalid.
 */
class InvalidRequisitionException(message: String, cause: Throwable? = null) :
  RequisitionRefusalException.Default(
    Requisition.Refusal.Justification.SPEC_INVALID,
    message,
    cause,
  )

/**
 * Thrown to indicate that a [Requisition] should be fulfillable, but must be refused due to some
 * unrecoverable system error.
 */
class UnfulfillableRequisitionException(message: String, cause: Throwable? = null) :
  RequisitionRefusalException.Default(
    Requisition.Refusal.Justification.UNFULFILLABLE,
    message,
    cause,
  )
