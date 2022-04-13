/**
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * ```
 *      http://www.apache.org/licenses/LICENSE-2.0
 * ```
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.eventfiltration

import com.google.protobuf.Message
import org.projectnessie.cel.Ast
import org.projectnessie.cel.Env
import org.projectnessie.cel.Program
import org.projectnessie.cel.common.types.BoolT
import org.projectnessie.cel.common.types.Err
import org.projectnessie.cel.common.types.ref.Val
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidator

object EventFilters {
  /**
   * Compiles a [Program] that should be fed into [matches] function to indicate if an [Event]
   * should be filtered or not, based on the filtering [celExpr].
   *
   * @param celExpr string descrbing the event filter in common expression language format.
   *
   * @param defaultEventMessage default instance for a [Message] that contains each type of event
   * template as fields. See `event_annotations.proto`.
   *
   * @param operativeFields fields in [celExpr] that will be not be altered after the normalization
   * operation. If provided, [celExpr] is normalized to operative negative normal form by
   * bubbling down all the negation operations to the leafs by applying De Morgan's laws recursively
   * and by setting all the leaf comparison nodes (e.g. x == 47 ) that contain any field other than
   * the operative fields to true.
   *
   * @throws [EventFilterValidationException] if [celExpr] is not valid. with the following codes:
   * * [EventFilterValidationException.Code.INVALID_CEL_EXPRESSION]
   * * [EventFilterValidationException.Code.INVALID_VALUE_TYPE]
   * * [EventFilterValidationException.Code.UNSUPPORTED_OPERATION]
   * * [EventFilterValidationException.Code.EXPRESSION_IS_NOT_CONDITIONAL]
   * * [EventFilterValidationException.Code.INVALID_OPERATION_OUTSIDE_LEAF]
   * * [EventFilterValidationException.Code.FIELD_COMPARISON_OUTSIDE_LEAF]
   */
  fun compileProgram(
    celExpr: String,
    defaultEventMessage: Message,
    operativeFields: Set<String> = emptySet()
  ): Program =
    EventFilterValidator.compileProgramWithEventMessage(
      celExpr,
      defaultEventMessage,
      operativeFields
    )

  /**
   * Validates an Event Filtering CEL expression according to Halo rules.
   *
   * @throws [EventFilterValidationException] if [celExpr] is not valid. with the following codes:
   * * [EventFilterValidationException.Code.INVALID_CEL_EXPRESSION]
   * * [EventFilterValidationException.Code.INVALID_VALUE_TYPE]
   * * [EventFilterValidationException.Code.UNSUPPORTED_OPERATION]
   * * [EventFilterValidationException.Code.EXPRESSION_IS_NOT_CONDITIONAL]
   * * [EventFilterValidationException.Code.INVALID_OPERATION_OUTSIDE_LEAF]
   * * [EventFilterValidationException.Code.FIELD_COMPARISON_OUTSIDE_LEAF]
   */
  fun compile(celExpr: String, env: Env): Ast = EventFilterValidator.compile(celExpr, env)

  /**
   * Indicates if an Event should be filtered or not, based on a Program previously compiled with
   * [compileProgram] function.
   *
   * @param event is a protobuf Message that contains each type of event template as fields. See
   * `event_annotations.proto`.
   *
   * Throws a [EventFilterException] with the following codes:
   * * [EventFilterException.Code.EVALUATION_ERROR]
   * * [EventFilterException.Code.INVALID_RESULT]
   */
  fun matches(event: Message, program: Program): Boolean {
    val variables: Map<String, Any> = event.allFields.entries.associate { it.key.name to it.value }
    val result: Program.EvalResult = program.eval(variables)
    val value: Val = result.`val`
    if (value is Err) {
      throw EventFilterException(
        EventFilterException.Code.EVALUATION_ERROR,
        value.toString(),
      )
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
