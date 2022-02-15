package org.wfanet.measurement.eventDataProvider.eventFiltration.validation

class EventFilterValidationException : Exception {
  val code: Code

  constructor(code: Code, message: String) : super(message) {
    this.code = code
  }
  enum class Code {
    // An expression that cannot be interpreted as a valid CEL
    INVALID_CEL_EXPRESSION,
    // Expression uses a CEL operation that currently is not supported for Halo
    UNSUPPORTED_OPERATION,
    // A CEL value has a type that is invalid for Halo
    INVALID_VALUE_TYPE,
    // Field comparison is not done within a leaf node
    FIELD_COMPARISON_OUTSIDE_LEAF,
    // The expression is a single value, not a condition
    EXPRESSION_IS_NOT_CONDITIONAL,
    // Leaf-only operator used outside leaf
    INVALID_OPERATION_OUTSIDE_LEAF,
  }
}
