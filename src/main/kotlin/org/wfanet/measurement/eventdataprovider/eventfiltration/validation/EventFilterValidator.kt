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

package org.wfanet.measurement.eventdataprovider.eventfiltration.validation

import com.google.api.expr.v1alpha1.Expr
import org.projectnessie.cel.Env
import org.projectnessie.cel.Issues

/**
 * Validates an Event Filtering CEL expression according to Halo rules.
 *
 * Throws a [EventFilterValidationException] on [validate] with the following codes:
 * * [EventFilterValidationException.Code.INVALID_CEL_EXPRESSION]
 * * [EventFilterValidationException.Code.INVALID_VALUE_TYPE]
 * * [EventFilterValidationException.Code.UNSUPPORTED_OPERATION]
 * * [EventFilterValidationException.Code.EXPRESSION_IS_NOT_CONDITIONAL]
 * * [EventFilterValidationException.Code.INVALID_OPERATION_OUTSIDE_LEAF]
 * * [EventFilterValidationException.Code.FIELD_COMPARISON_OUTSIDE_LEAF]
 */
object EventFilterValidator {

  private val hasIndent = { args: List<Expr> -> args.find { it.hasIdentExpr() } != null }
  private val hasCallExpr = { args: List<Expr> -> args.find { it.hasCallExpr() } != null }

  private fun validateInOperator(callExpr: Expr.Call) {
    val left = callExpr.argsList[0]
    val right = callExpr.argsList[1]
    if (!left.hasIdentExpr() && !left.hasSelectExpr()) {
      throw EventFilterValidationException(
        EventFilterValidationException.Code.INVALID_VALUE_TYPE,
        "Operator @in left argument should be a variable",
      )
    }
    if (!right.hasListExpr()) {
      throw EventFilterValidationException(
        EventFilterValidationException.Code.INVALID_VALUE_TYPE,
        "Operator @in right argument should be a list",
      )
    }
    if (right.hasListExpr()) {
      for (element in right.listExpr.elementsList) {
        if (!element.hasConstExpr()) {
          throw EventFilterValidationException(
            EventFilterValidationException.Code.INVALID_VALUE_TYPE,
            "Operator @in right argument should be a list of constants or a variable",
          )
        }
      }
    }
  }

  fun failOnInvalidExpression(issues: Issues) {
    if (issues.hasIssues()) {
      throw EventFilterValidationException(
        EventFilterValidationException.Code.INVALID_CEL_EXPRESSION,
        issues.toString(),
      )
    }
  }

  private fun failOnListOutsideInOperator(expr: Expr) {
    if (expr.hasListExpr()) {
      throw EventFilterValidationException(
        EventFilterValidationException.Code.INVALID_VALUE_TYPE,
        "Lists are only allowed on the right side of a @in operator",
      )
    }
  }

  private fun failOnSingleToplevelValue() {
    throw EventFilterValidationException(
      EventFilterValidationException.Code.EXPRESSION_IS_NOT_CONDITIONAL,
      "Expression cannot be a single value, should be a conditional",
    )
  }

  private fun failOnVariableOutsideLeaf(args: List<Expr>) {
    if (hasIndent(args) && hasCallExpr(args)) {
      throw EventFilterValidationException(
        EventFilterValidationException.Code.FIELD_COMPARISON_OUTSIDE_LEAF,
        "Field comparison should be done only on the leaf expressions",
      )
    }
  }

  private fun failOnInvalidOperationOutsideLeaf(callExpr: Expr.Call) {
    val operator = callExpr.function
    if (LEAF_ONLY_OPERATORS.contains(operator) && hasCallExpr(callExpr.argsList)) {
      throw EventFilterValidationException(
        EventFilterValidationException.Code.INVALID_OPERATION_OUTSIDE_LEAF,
        "Operator $operator should only be used on leaf expressions",
      )
    }
  }

  private fun failOnNotAllowedOperator(operator: String?) {
    if (!ALLOWED_OPERATORS.contains(operator)) {
      throw EventFilterValidationException(
        EventFilterValidationException.Code.UNSUPPORTED_OPERATION,
        "Operator $operator is not allowed",
      )
    }
  }

  private fun validateExpr(expr: Expr) {
    if (!expr.hasCallExpr()) {
      failOnListOutsideInOperator(expr)
      return
    }
    val callExpr: Expr.Call = expr.callExpr
    val operator = callExpr.function
    failOnNotAllowedOperator(operator)
    if (operator == "@in") {
      validateInOperator(callExpr)
      return
    }
    failOnVariableOutsideLeaf(callExpr.argsList)
    failOnInvalidOperationOutsideLeaf(callExpr)
    for (arg in callExpr.argsList) {
      validateExpr(arg)
    }
  }

  fun validate(celExpression: String, env: Env) {
    val astAndIssues = env.compile(celExpression)
    failOnInvalidExpression(astAndIssues.issues)
    val expr = astAndIssues.ast.expr
    if (!expr.hasCallExpr()) {
      failOnSingleToplevelValue()
    }
    validateExpr(expr)
  }
}

private val LEAF_ONLY_OPERATORS =
  listOf(
    "_>_",
    "_<_",
    "_!=_",
    "_==_",
    "_<=_",
    "@in",
  )
private val BOOLEAN_OPERATORS =
  listOf(
    "!_",
    "_&&_",
    "_||_",
  )
private val ALLOWED_OPERATORS = LEAF_ONLY_OPERATORS + BOOLEAN_OPERATORS
