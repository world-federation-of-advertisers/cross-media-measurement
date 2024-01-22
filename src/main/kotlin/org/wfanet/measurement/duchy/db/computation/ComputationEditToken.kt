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

import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType

/** Information about a computation needed to edit a computation. */
data class ComputationEditToken<ProtocolT, StageT>(
  /** The identifier for the computation used locally. */
  val localId: Long,
  /** The protocol used for the computation. */
  val protocol: ProtocolT,
  /** The stage of the computation when the token was created. */
  val stage: StageT,
  /** The number of the current attempt of this stage for this computation. */
  val attempt: Int,
  /**
   * The version number of the last known edit to the computation. The version is a monotonically
   * increasing number used as a guardrail to protect against concurrent edits to the same
   * computation.
   */
  val editVersion: Long,
  /** The global identifier for the computation. */
  val globalId: String,
)

fun ComputationToken.toDatabaseEditToken():
  ComputationEditToken<ComputationType, ComputationStage> {
  val protocol = computationStage.toComputationType()
  if (protocol == ComputationType.UNRECOGNIZED) {
    failGrpc { "Computation type for $this is unknown" }
  }
  return ComputationEditToken(
    localId = localComputationId,
    protocol = protocol,
    stage = computationStage,
    attempt = attempt,
    editVersion = version,
    globalId = globalComputationId,
  )
}
