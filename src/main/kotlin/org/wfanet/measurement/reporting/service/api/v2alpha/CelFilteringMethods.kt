/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.protobuf.Message
import org.projectnessie.cel.Env
import org.projectnessie.cel.EnvOption
import org.projectnessie.cel.checker.Decls
import org.projectnessie.cel.common.types.Err
import org.projectnessie.cel.common.types.pb.ProtoTypeRegistry
import org.projectnessie.cel.common.types.ref.Val

/** Builds a CEL Env from a [Message]. */
fun buildCelEnvironment(message: Message): Env {
  // Build CEL ProtoTypeRegistry.
  val celTypeRegistry = ProtoTypeRegistry.newRegistry()
  celTypeRegistry.registerMessage(message)

  // Build CEL Env.
  val descriptor = message.descriptorForType
  val env =
    Env.newEnv(
      EnvOption.container(descriptor.fullName),
      EnvOption.customTypeProvider(celTypeRegistry),
      EnvOption.customTypeAdapter(celTypeRegistry),
      EnvOption.declarations(
        descriptor.fields.map {
          Decls.newVar(it.name, celTypeRegistry.findFieldType(descriptor.fullName, it.name).type)
        }
      ),
    )
  return env
}

/**
 * Filters a list of [Message] using a CEL [Env] and a CEL filter string.
 *
 * @throws [IllegalArgumentException] when the filter string is not valid.
 */
fun <T : Message> filterList(env: Env, items: List<T>, filter: String): List<T> {
  if (filter.isEmpty()) {
    return items
  }

  val astAndIssues =
    try {
      env.compile(filter)
    } catch (_: NullPointerException) {
      // NullPointerException is thrown when an operator in the filter is not a CEL operator.
      throw IllegalArgumentException("filter is not a valid CEL expression")
    }
  if (astAndIssues.hasIssues()) {
    throw IllegalArgumentException("filter is not a valid CEL expression: ${astAndIssues.issues}")
  }
  val program = env.program(astAndIssues.ast)

  return items.filter { item ->
    val variables: Map<String, Any> =
      mutableMapOf<String, Any>().apply {
        for (fieldDescriptor in item.descriptorForType.fields) {
          put(fieldDescriptor.name, item.getField(fieldDescriptor))
        }
      }
    val result: Val = program.eval(variables).`val`
    if (result is Err) {
      throw result.toRuntimeException()
    }

    if (result.value() !is Boolean) {
      throw IllegalArgumentException("filter does not evaluate to boolean")
    }

    result.booleanValue()
  }
}
