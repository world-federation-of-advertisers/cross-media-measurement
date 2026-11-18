/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.eventfiltration

import com.google.protobuf.Descriptors
import com.google.protobuf.Message
import org.projectnessie.cel.Ast
import org.projectnessie.cel.Env
import org.projectnessie.cel.EnvOption
import org.projectnessie.cel.Program
import org.projectnessie.cel.checker.Decls
import org.projectnessie.cel.common.types.BoolT
import org.projectnessie.cel.common.types.Err
import org.projectnessie.cel.common.types.pb.ProtoTypeRegistry
import org.projectnessie.cel.common.types.ref.Val
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.common.ProtoReflection.allDependencies
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters.matches
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidator

object EventFilters {
  /**
   * Compiles a [Program] that should be fed into [matches] function to indicate if an event should
   * be filtered or not, based on the filtering [celExpr].
   *
   * @param eventMessageDescriptor protobuf descriptor of the event message type. This message type
   *   should contain fields of types that have been annotated with the
   *   `wfa.measurement.api.v2alpha.EventTemplateDescriptor` option.
   * @param celExpr Common Expression Language (CEL) expression defining the predicate to apply to
   *   event messages
   * @param operativeFields fields in [celExpr] that will be not be altered after the normalization
   *   operation. If provided, [celExpr] is normalized to operative negation normal form by bubbling
   *   down all the negation operations to the leafs by applying De Morgan's laws recursively and by
   *   setting all the leaf comparison nodes (e.g. x == 47 ) that contain any field other than the
   *   operative fields to true. If not provided or empty, the normalization operation will not be
   *   performed.
   * @throws [EventFilterValidationException] if [celExpr] is not valid.
   */
  fun compileProgram(
    eventMessageDescriptor: Descriptors.Descriptor,
    celExpr: String,
    operativeFields: Set<String> = emptySet(),
  ): Program {
    val env = createEnv(eventMessageDescriptor)
    val ast = compile(env, celExpr, operativeFields)
    return env.program(ast)
  }

  /**
   * Compiles [celExpr] to an [Ast].
   *
   * @throws [EventFilterValidationException] if [celExpr] is not valid.
   */
  private fun compile(env: Env, celExpr: String, operativeFields: Set<String> = emptySet()): Ast =
    EventFilterValidator.compile(env, celExpr, operativeFields)

  /** Creates an [Env] from [eventMessageDescriptor]. */
  private fun createEnv(eventMessageDescriptor: Descriptors.Descriptor): Env {
    val celTypeRegistry: ProtoTypeRegistry =
      ProtoTypeRegistry.newRegistry().apply {
        registerDescriptor(eventMessageDescriptor.file)
        for (fileDescriptor in eventMessageDescriptor.file.allDependencies) {
          registerDescriptor(fileDescriptor)
        }
      }
    val celVariables =
      eventMessageDescriptor.fields
        .filter { it.messageType.options.hasExtension(EventAnnotationsProto.eventTemplate) }
        .map { field ->
          val fieldType =
            checkNotNull(
              celTypeRegistry.findFieldType(eventMessageDescriptor.fullName, field.name)
            ) {
              "Type not found for field ${field.fullName}"
            }
          Decls.newVar(field.name, fieldType.type)
        }
    return Env.newEnv(
      EnvOption.container(eventMessageDescriptor.fullName),
      EnvOption.customTypeAdapter(celTypeRegistry),
      EnvOption.customTypeProvider(celTypeRegistry),
      EnvOption.declarations(celVariables),
    )
  }

  /**
   * Indicates if an Event should be filtered or not, based on a Program previously compiled with
   * [compileProgram] function.
   *
   * @param event is a protobuf Message that contains each type of event template as fields. See
   *   `event_annotations.proto`.
   *
   * Throws a [EventFilterException] with the following codes:
   * * [EventFilterException.Code.EVALUATION_ERROR]
   * * [EventFilterException.Code.INVALID_RESULT]
   */
  fun matches(event: Message, program: Program): Boolean {
    val variables: Map<String, Any> =
      event.descriptorForType.fields.associateBy({ it.name }, { event.getField(it) })
    val result: Program.EvalResult = program.eval(variables)
    val value: Val = result.`val`
    if (value is Err) {
      throw EventFilterException(EventFilterException.Code.EVALUATION_ERROR, value.toString())
    }
    if (value !is BoolT) {
      throw EventFilterException(
        EventFilterException.Code.INVALID_RESULT,
        "Evaluation of CEL expression should result in a boolean value",
      )
    }
    return value.booleanValue()
  }
}
