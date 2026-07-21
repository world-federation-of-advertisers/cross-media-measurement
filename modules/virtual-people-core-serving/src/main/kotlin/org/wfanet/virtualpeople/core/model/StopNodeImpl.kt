// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.virtualpeople.core.model

import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.virtualpeople.common.LabelerEvent

/** The implementation of the CompiledNode with stop_node set. */
internal class StopNodeImpl private constructor(nodeConfig: CompiledNode) : ModelNode(nodeConfig) {

  /**
   * Always use ModelNode::Build to get a ModelNode object. Users should never call the factory
   * function or constructor of the derived class directly.
   */
  companion object {
    fun build(nodeConfig: CompiledNode): ModelNode {
      if (!nodeConfig.hasStopNode()) {
        error("This is not a stop node.")
      }
      return StopNodeImpl(nodeConfig)
    }
  }

  /** No operation is needed when applying a StopNode. */
  override fun apply(event: LabelerEvent.Builder) {
    return
  }
}
