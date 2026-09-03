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

import com.google.protobuf.Descriptors.EnumValueDescriptor
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Descriptors.FieldDescriptor.Type
import org.wfanet.virtualpeople.common.ConditionalAssignment
import org.wfanet.virtualpeople.common.LabelerEvent
import org.wfanet.virtualpeople.common.fieldfilter.FieldFilter
import org.wfanet.virtualpeople.common.fieldfilter.utils.getFieldFromProto
import org.wfanet.virtualpeople.common.fieldfilter.utils.getValueFromProto
import org.wfanet.virtualpeople.common.fieldfilter.utils.setValueToProtoBuilder

internal class ConditionalAssignmentImpl
private constructor(private val condition: FieldFilter, private val assignments: List<Assignment>) :
  AttributesUpdaterInterface {

  /**
   * If [condition] is matched, for each entry in [assignments], assigns the value of source field
   * to target field. If [condition] is not matched, does nothing.
   */
  override fun update(event: LabelerEvent.Builder) {
    if (condition.matches(event)) {
      for (a in assignments) {
        a.assigner.apply(event, a.source, a.target)
      }
    }
  }

  companion object {

    private inline fun <reified T> assign(
      event: LabelerEvent.Builder,
      source: List<FieldDescriptor>,
      target: List<FieldDescriptor>,
    ) {
      val fieldValue = getValueFromProto<T>(event, source)
      if (!fieldValue.isSet) {
        return
      }
      setValueToProtoBuilder(event, target, fieldValue.value)
    }

    private fun getAssigner(type: Type): Assigner {
      return when (type) {
        Type.INT32 -> Int32Assigner
        Type.UINT32 -> UInt32Assigner
        Type.INT64 -> Int64Assigner
        Type.UINT64 -> UInt64Assigner
        Type.BOOL -> BoolAssigner
        Type.ENUM -> EnumAssigner
        Type.STRING -> StringAssigner
        else -> error("Unsupported field type for ConditionalAssignment: $type")
      }
    }

    /**
     * Always use [AttributesUpdaterInterface.build] to get an [AttributesUpdaterInterface] object.
     * Users should not call the factory method or the constructor of the derived classes directly.
     *
     * Returns error status when any of the following happens:
     * 1. [config].condition is not set.
     * 2. [config].assignments is empty.
     * 3. Fails to build a [FieldFilter] from [config].condition.
     * 4. In any entry of [config].assignments, sourceField or targetField is not set or does not
     *    refer to a valid field.
     * 5. In any entry of [config].assignments, sourceField and targetField refer to different type
     *    of fields. (like int32 vs int64)
     */
    internal fun build(config: ConditionalAssignment): ConditionalAssignmentImpl {
      if (!config.hasCondition()) {
        error("Condition is not set in ConditionalAssignment: $config")
      }
      if (config.assignmentsCount == 0) {
        error("No assignments in ConditionalAssignment: $config")
      }
      val condition = FieldFilter.create(LabelerEvent.getDescriptor(), config.condition)

      val assignments: List<Assignment> =
        config.assignmentsList.map { assignmentConfig ->
          if (!assignmentConfig.hasSourceField()) {
            error("All assignments must have source_field set in ConditionalAssignment: $config")
          }
          if (!assignmentConfig.hasTargetField()) {
            error("All assignments must have target_field set in ConditionalAssignment: $config")
          }
          val source: List<FieldDescriptor> =
            getFieldFromProto(LabelerEvent.getDescriptor(), assignmentConfig.sourceField)
          val target: List<FieldDescriptor> =
            getFieldFromProto(LabelerEvent.getDescriptor(), assignmentConfig.targetField)
          if (source.last().type != target.last().type) {
            error(
              "All assignments must have source_field and target_field being " +
                "the same type in ConditionalAssignment: $config "
            )
          }
          Assignment(source, target, getAssigner(source.last().type))
        }

      return ConditionalAssignmentImpl(condition, assignments)
    }

    /**
     * Monomorphic per-type assigner. Each `object` is a single instance with a single `apply`
     * method, so the JIT sees a monomorphic call site at each `assigner.apply(...)` invocation and
     * can inline through. The reified-T lambda dispatch the previous code used was erased at
     * runtime and forced primitive boxing.
     */
    internal sealed interface Assigner {
      fun apply(
        event: LabelerEvent.Builder,
        source: List<FieldDescriptor>,
        target: List<FieldDescriptor>,
      )
    }

    internal object Int32Assigner : Assigner {
      override fun apply(
        event: LabelerEvent.Builder,
        source: List<FieldDescriptor>,
        target: List<FieldDescriptor>,
      ) = assign<Int>(event, source, target)
    }

    internal object UInt32Assigner : Assigner {
      override fun apply(
        event: LabelerEvent.Builder,
        source: List<FieldDescriptor>,
        target: List<FieldDescriptor>,
      ) = assign<UInt>(event, source, target)
    }

    internal object Int64Assigner : Assigner {
      override fun apply(
        event: LabelerEvent.Builder,
        source: List<FieldDescriptor>,
        target: List<FieldDescriptor>,
      ) = assign<Long>(event, source, target)
    }

    internal object UInt64Assigner : Assigner {
      override fun apply(
        event: LabelerEvent.Builder,
        source: List<FieldDescriptor>,
        target: List<FieldDescriptor>,
      ) = assign<ULong>(event, source, target)
    }

    internal object BoolAssigner : Assigner {
      override fun apply(
        event: LabelerEvent.Builder,
        source: List<FieldDescriptor>,
        target: List<FieldDescriptor>,
      ) = assign<Boolean>(event, source, target)
    }

    internal object EnumAssigner : Assigner {
      override fun apply(
        event: LabelerEvent.Builder,
        source: List<FieldDescriptor>,
        target: List<FieldDescriptor>,
      ) = assign<EnumValueDescriptor>(event, source, target)
    }

    internal object StringAssigner : Assigner {
      override fun apply(
        event: LabelerEvent.Builder,
        source: List<FieldDescriptor>,
        target: List<FieldDescriptor>,
      ) = assign<String>(event, source, target)
    }
  }
}

private data class Assignment(
  val source: List<FieldDescriptor>,
  val target: List<FieldDescriptor>,
  val assigner: ConditionalAssignmentImpl.Companion.Assigner,
)
