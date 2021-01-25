// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.db.computation

import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType

/** Helper class for working with [ComputationType] protocols. */
object ComputationTypes : ComputationTypeEnumHelper<ComputationType> {
  override fun protocolEnumToLong(value: ComputationType): Long {
    return value.numberAsLong
  }

  override fun longToProtocolEnum(value: Long): ComputationType {
    // forNumber() returns null for unrecognized enum values for the proto.
    return ComputationType.forNumber(value.toInt()) ?: ComputationType.UNRECOGNIZED
  }
}
