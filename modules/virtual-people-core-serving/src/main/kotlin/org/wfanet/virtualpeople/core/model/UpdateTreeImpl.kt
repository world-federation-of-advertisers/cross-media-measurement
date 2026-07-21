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

import org.wfanet.virtualpeople.common.LabelerEvent
import org.wfanet.virtualpeople.common.UpdateTree

internal class UpdateTreeImpl private constructor(private val root: ModelNode) :
  AttributesUpdaterInterface {

  /** Applies the attached model tree, which is represented by the root node, to the [event]. */
  override fun update(event: LabelerEvent.Builder) {
    root.apply(event)
  }

  internal companion object {
    /**
     * Always use [AttributesUpdaterInterface].build to get an [AttributesUpdaterInterface] object.
     * Users should not call the factory method or the constructor of the derived classes directly.
     *
     * Throws an error when it fails to build [ModelNode] from [config].root.
     */
    internal fun build(config: UpdateTree, nodeRefs: MutableMap<Int, ModelNode>): UpdateTreeImpl {
      return UpdateTreeImpl(ModelNode.build(config.root, nodeRefs))
    }
  }
}
