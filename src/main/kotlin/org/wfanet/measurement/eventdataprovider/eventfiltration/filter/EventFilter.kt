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

package org.wfanet.measurement.eventdataprovider.eventfiltration.filter

import com.google.protobuf.Message
import org.projectnessie.cel.Ast
import org.projectnessie.cel.Env
import org.projectnessie.cel.EnvOption
import org.projectnessie.cel.Program
import org.projectnessie.cel.checker.Decls
import org.projectnessie.cel.common.types.BoolT
import org.projectnessie.cel.common.types.Err
import org.projectnessie.cel.common.types.pb.ProtoTypeRegistry

/**
 * Indicates if an Event should be filtered or not, based on the filtering CEL expression.
 *
 * Event is a protobuf Message that contains each type of event template as fields. See
 * `event_annotations.proto`.
 */
class EventFilter(celExpr: String, eventMessage: Message) {
  private val program: Program
  init {
    val env = envForEventMessage(eventMessage)
    val ast = tryCompile(env, celExpr)
    program = env.program(ast)
  }

  private fun tryCompile(env: Env, celExpr: String): Ast {
    var astAndIssues: Env.AstIssuesTuple
    try {
      astAndIssues = env.compile(celExpr)
    } catch (e: Exception) {
      throw EventFilterException(EventFilterException.Code.INVALID_CEL_EXPRESSION)
    }
    if (astAndIssues.hasIssues()) {
      throw EventFilterException(
        EventFilterException.Code.INVALID_CEL_EXPRESSION,
        astAndIssues.issues.toString()
      )
    }
    return astAndIssues.ast
  }

  private fun envForEventMessage(eventMessage: Message): Env {
    val typeRegistry: ProtoTypeRegistry = ProtoTypeRegistry.newRegistry(eventMessage)
    val celVariables =
      eventMessage.descriptorForType.fields.map { field ->
        val typeName = field.messageType.fullName
        val defaultValue = eventMessage.getField(field) as? Message
        checkNotNull(defaultValue) { "eventMessage field should have Message type" }
        typeRegistry.registerMessage(defaultValue)
        Decls.newVar(
          field.name,
          Decls.newObjectType(typeName),
        )
      }
    return Env.newEnv(
      EnvOption.customTypeAdapter(typeRegistry),
      EnvOption.customTypeProvider(typeRegistry),
      EnvOption.declarations(celVariables),
    )
  }

  fun matches(event: Message): Boolean {
    val variables = event.allFields.entries.associate { it.key.name to it.value }
    val result = program.eval(variables)
    val value = result.`val`
    if (value is Err) {
      throw EventFilterException(
        EventFilterException.Code.EVALUATION_ERROR,
        value.toString(),
      )
    }
    if (value !is BoolT) {
      throw EventFilterException(
        EventFilterException.Code.INVALID_EVALUATION_RESULT,
        "CEL expression result is not a boolean",
      )
    }
    return value.booleanValue()
  }
}
