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

package org.wfanet.measurement.common.cel

import com.google.protobuf.Descriptors
import com.google.protobuf.Message
import org.projectnessie.cel.Env
import org.projectnessie.cel.EnvOption
import org.projectnessie.cel.checker.Decls
import org.projectnessie.cel.common.types.Err
import org.projectnessie.cel.common.types.pb.ProtoTypeRegistry
import org.projectnessie.cel.common.types.ref.Val
import org.wfanet.measurement.common.ProtoReflection.allDependencies

/**
 * Thrown by [CelPredicates.validate] when a CEL predicate string fails to compile or does not
 * evaluate to a boolean. Callers should treat as a programming/configuration error.
 */
class CelValidationException(message: String) : Exception(message)

/** Utilities for working with CEL predicate expressions (expressions that evaluate to a bool). */
object CelPredicates {
  /**
   * Validates that [filter] is a legal CEL predicate against [env]: it must compile and its result
   * type must be boolean. An empty [filter] is a no-op.
   *
   * A filter is rejected if any of the following hold:
   * - **Syntax errors** -- trailing operators, unmatched parens, missing operands.
   * - **Unknown top-level identifiers** -- the expression references a type that does not exist on
   *   the [env]'s message declarations.
   * - **Unknown nested fields** -- the type exists but the field path does not.
   * - **Type-mismatched operands** -- `bool == float`, `string < int`, etc.
   * - **Non-boolean result** -- the expression compiles but yields a non-bool. Downstream consumers
   *   that expect a bool predicate (e.g. RequisitionSpec filters, which cause an EDP to reject the
   *   requisition when the filter does not evaluate to a boolean) must reject up-front.
   *
   * @throws CelValidationException when [filter] fails to compile, references unknown fields,
   *   or does not return a boolean. The exception message describes the specific failure.
   */
  fun validate(env: Env, filter: String) {
    if (filter.isEmpty()) {
      return
    }
    val astAndIssues =
      try {
        env.compile(filter)
      } catch (_: NullPointerException) {
        // CEL throws NPE when the filter uses a non-CEL operator. Same swallowing pattern as
        // filterList in this package; treat as a syntax error.
        throw CelValidationException("not a valid CEL expression")
      }
    if (astAndIssues.hasIssues()) {
      throw CelValidationException("not a valid CEL expression: ${astAndIssues.issues}")
    }
    if (astAndIssues.ast.resultType != Decls.Bool) {
      throw CelValidationException("does not evaluate to a boolean")
    }
  }
}

/**
 * Builds a CEL Env from a [Message].
 *
 * Prefer this overload over [buildCelEnvironment(Descriptors.Descriptor)] whenever a concrete
 * compiled message is available. It also binds the descriptor's `reflectType` to [message]'s
 * runtime class via `ProtoTypeRegistry.registerMessage`, so a subsequent [filterList] call can
 * convert runtime values of that class without falling back to `DynamicMessage`-shaped paths.
 */
fun buildCelEnvironment(message: Message): Env {
  return buildCelEnvironment(message.descriptorForType) { registry ->
    registry.registerMessage(message)
  }
}

/**
 * Builds a CEL Env from a [Descriptors.Descriptor].
 *
 * Use only when no compiled [Message] is available -- the BasicReport CEL path, where the event
 * message is loaded from a deployment-supplied descriptor set. Other call sites should use
 * [buildCelEnvironment(Message)] so the registry binds a real Kotlin/Java class to the descriptor
 * rather than the `DynamicMessage` default.
 */
fun buildCelEnvironment(descriptor: Descriptors.Descriptor): Env {
  return buildCelEnvironment(descriptor) {}
}

/**
 * Shared core for the two public overloads. Registers [descriptor]'s file plus all transitive file
 * dependencies (so types declared in imported files -- e.g. EventTemplate sub-messages -- are
 * resolvable), then lets [additionalRegistration] bind extras (the [Message] overload uses this to
 * preserve the `reflectType` mapping `registerMessage` would have set on its own).
 */
private fun buildCelEnvironment(
  descriptor: Descriptors.Descriptor,
  additionalRegistration: (ProtoTypeRegistry) -> Unit,
): Env {
  val celTypeRegistry = ProtoTypeRegistry.newRegistry()
  celTypeRegistry.registerDescriptor(descriptor.file)
  for (dep in descriptor.file.allDependencies) {
    celTypeRegistry.registerDescriptor(dep)
  }
  additionalRegistration(celTypeRegistry)

  return Env.newEnv(
    EnvOption.container(descriptor.fullName),
    EnvOption.customTypeProvider(celTypeRegistry),
    EnvOption.customTypeAdapter(celTypeRegistry),
    EnvOption.declarations(
      descriptor.fields.map {
        Decls.newVar(it.name, celTypeRegistry.findFieldType(descriptor.fullName, it.name).type)
      }
    ),
  )
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
