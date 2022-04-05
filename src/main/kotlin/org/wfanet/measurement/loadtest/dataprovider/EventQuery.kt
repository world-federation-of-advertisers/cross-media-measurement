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

package org.wfanet.measurement.loadtest.dataprovider

import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventFilter

// TODO(@uakyol): Delete once the GCS correctness test supports EventFilters.
enum class Sex(val string: String) {
  MALE("M"),
  FEMALE("F")
}

enum class AgeGroup(val string: String) {
  RANGE_18_34("18_34"),
  RANGE_35_54("35_54"),
  ABOVE_54("55+")
}

enum class SocialGrade(val string: String) {
  ABC1("ABC1"),
  C2DE("C2DE"),
}

enum class Complete(val integer: Int) {
  COMPLETE(1),
  INCOMPLETE(0)
}

data class QueryParameter(
  val beginDate: String,
  val endDate: String,
  val sex: Sex?,
  val ageGroup: AgeGroup?,
  val socialGrade: SocialGrade?,
  val complete: Complete?,
)

// // TODO(@uakyol) : Rename to QueryParameter after QueryParameter is deleted.
// data class FilterParameter(
//   val celExpr: String,
//   val eventMessage: Message,
// )

/** A query to get the list of user virtual IDs for a particular requisition. */
abstract class EventQuery {
  abstract fun getUserVirtualIds(eventFilter: EventFilter): Sequence<Long>
}
