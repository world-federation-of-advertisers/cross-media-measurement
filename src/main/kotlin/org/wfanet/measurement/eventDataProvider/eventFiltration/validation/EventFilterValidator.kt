package org.wfanet.measurement.eventDataProvider.eventFiltration.validation

import com.google.api.expr.v1alpha1.Expr
import org.projectnessie.cel.common.Source
import org.projectnessie.cel.parser.Options
import org.projectnessie.cel.parser.Parser

object EventFilterValidator {
  private val leafOnlyOperators =
    listOf(
      "_>_",
      "_<_",
      "_!=_",
      "_==_",
      "_<=_",
      "@in",
    )
  private val booleanOperators =
    listOf(
      "!_",
      "_&&_",
      "_||_",
    )
  private val allowedOperators = leafOnlyOperators + booleanOperators

  private fun validateInOperator(callExpr: Expr.Call) {
    val left = callExpr.argsList[0]
    val right = callExpr.argsList[1]
    if (!left.hasIdentExpr()) {
      throw EventFilterValidationException(
        EventFilterValidationException.Code.INVALID_VALUE_TYPE,
        "Operator @in left argument should be a variable",
      )
    }
    if (!right.hasIdentExpr() && !right.hasListExpr()) {
      throw EventFilterValidationException(
        EventFilterValidationException.Code.INVALID_VALUE_TYPE,
        "Operator @in right argument should be either a variable or a list",
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

  private fun failOnInvalidExpression(result: Parser.ParseResult) {
    if (result.hasErrors()) {
      throw EventFilterValidationException(
        EventFilterValidationException.Code.INVALID_CEL_EXPRESSION,
        result.errors.toDisplayString(),
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

  private fun failOnSingleToplevelValue(level: Int) {
    if (level == 0) {
      throw EventFilterValidationException(
        EventFilterValidationException.Code.EXPRESSION_IS_NOT_CONDITIONAL,
        "Expression cannot be a single value, should be a conditional",
      )
    }
  }

  private fun failOnVariableOutsideLeaf(args: List<Expr>) {
    val hasIndent = args.find { it.hasIdentExpr() } != null
    val hasCallExpr = args.find { it.hasCallExpr() } != null
    if (hasIndent && hasCallExpr) {
      throw EventFilterValidationException(
        EventFilterValidationException.Code.FIELD_COMPARISON_OUTSIDE_LEAF,
        "Field comparison should be done only on the leaf expressions",
      )
    }
  }

  private fun failOnInvalidOperationOutsideLeaf(callExpr: Expr.Call) {
    val operator = callExpr.function
    val hasCallExpr = callExpr.argsList.find { it.hasCallExpr() } != null
    if (leafOnlyOperators.contains(operator) && hasCallExpr) {
      throw EventFilterValidationException(
        EventFilterValidationException.Code.INVALID_OPERATION_OUTSIDE_LEAF,
        "Operator $operator should only be used on leaf expressions",
      )
    }
  }

  private fun validateExpr(expr: Expr, level: Int) {
    if (expr.hasCallExpr()) {
      val callExpr: Expr.Call = expr.callExpr
      val operator = callExpr.function
      if (!allowedOperators.contains(operator)) {
        throw EventFilterValidationException(
          EventFilterValidationException.Code.UNSUPPORTED_OPERATION,
          "Operator $operator is not allowed",
        )
      }
      if (operator == "@in") {
        validateInOperator(callExpr)
        return
      }
      failOnVariableOutsideLeaf(callExpr.argsList)
      failOnInvalidOperationOutsideLeaf(callExpr)
      for (arg in callExpr.argsList) {
        validateExpr(arg, level + 1)
      }
    } else {
      failOnSingleToplevelValue(level)
      failOnListOutsideInOperator(expr)
    }
  }

  fun validate(celExpression: String) {
    val result = Parser.parse(Options.builder().build(), Source.newTextSource(celExpression))
    failOnInvalidExpression(result)
    validateExpr(result.expr, 0)
  }
}
