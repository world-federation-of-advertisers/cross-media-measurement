// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.eventdataprovider.validation

import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec

/**
 * [RequisitionValidator] that validates a requisition against each validator in [validators].
 * Validators are applied in order, and the first non-valid result is returned.
 */
class CompoundValidator(private val validators: Iterable<RequisitionValidator>) :
  RequisitionValidator {

  override fun validate(
    requisition: Requisition,
    requisitionSpec: RequisitionSpec,
  ): List<Requisition.Refusal> =
    validators
      .map { it.validate(requisition, requisitionSpec) }
      .filter { it.isNotEmpty() }
      .flatten()
}
