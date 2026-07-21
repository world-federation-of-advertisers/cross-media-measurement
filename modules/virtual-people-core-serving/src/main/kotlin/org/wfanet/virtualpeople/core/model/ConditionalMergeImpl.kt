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

import org.wfanet.virtualpeople.common.ConditionalMerge
import org.wfanet.virtualpeople.common.LabelerEvent
import org.wfanet.virtualpeople.common.fieldfilter.FieldFilter
import org.wfanet.virtualpeople.core.model.utils.FieldFiltersMatcher

internal class ConditionalMergeImpl
/**
 * @param matcher matcher used to match input events to the conditions.
 * @param updates selected update will be merged to the input event.
 * @param passThroughNonMatches When calling Update, if no column matches, throws error if
 * [passThroughNonMatches] is [PassThroughNonMatches.NO].
 */
private constructor(
  private val matcher: FieldFiltersMatcher,
  private val updates: List<LabelerEvent>,
  private val passThroughNonMatches: PassThroughNonMatches
) : AttributesUpdaterInterface {

  /**
   * Updates [event] with selected node. The node is selected by matching [event] with conditions
   * through [matcher]. The update of selected node is merged into [event].
   *
   * Throws an error if no node matches [event], and passThroughNonMatches is
   * [PassThroughNonMatches.NO].
   */
  override fun update(event: LabelerEvent.Builder) {
    val index = matcher.getFirstMatch(event)
    if (index == -1) {
      if (passThroughNonMatches == PassThroughNonMatches.YES) {
        return
      } else {
        error("No node matching for event: $event")
      }
    }
    if (index < 0 || index >= updates.size) {
      /** This should never happen. */
      error("The returned index is out of range.")
    }
    event.mergeFrom(updates[index])
    return
  }

  companion object {

    /**
     * Always use [AttributesUpdaterInterface.build] to get an [AttributesUpdaterInterface] object.
     * Users should not call the factory method or the constructor of the derived classes directly.
     *
     * Throws an error when any of the following happens:
     * 1. [config].nodes is empty.
     * 2. [config].nodes.condition is not set.
     * 3. [config].nodes.update is not set.
     * 4. Fails to build [FieldFilter] from any [config].nodes.condition.
     */
    internal fun build(config: ConditionalMerge): ConditionalMergeImpl {
      if (config.nodesCount == 0) {
        error("No nodes in ConditionalMerge: $config")
      }

      val filters: List<FieldFilter> =
        config.nodesList.map { node ->
          if (!node.hasCondition()) {
            error("No condition in the node in ConditionalMerge: $node")
          }
          if (!node.hasUpdate()) {
            error("No update in the node in ConditionalMerge: $node")
          }
          FieldFilter.create(LabelerEvent.getDescriptor(), node.condition)
        }

      val updates: List<LabelerEvent> = config.nodesList.map { it.update }
      val matcher = FieldFiltersMatcher(filters)
      val passThroughNonMatches =
        if (config.passThroughNonMatches) PassThroughNonMatches.YES else PassThroughNonMatches.NO
      return ConditionalMergeImpl(matcher, updates, passThroughNonMatches)
    }
  }
}
