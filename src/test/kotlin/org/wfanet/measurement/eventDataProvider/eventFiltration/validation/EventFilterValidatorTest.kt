package org.wfanet.measurement.eventDataProvider.eventFiltration.validation

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.eventDataProvider.eventFiltration.validation.EventFilterValidationException.Code as Code

@RunWith(JUnit4::class)
class EventFilterValidatorTest {
  fun assertThatFailsWithCode(celExpression: String, code: Code) {
    val e =
      assertFailsWith(EventFilterValidationException::class) {
        EventFilterValidator.validate(celExpression)
      }
    assertThat(e.code).isEqualTo(code)
  }

  @Test
  fun `it fails on invalid operation`() {
    assertThatFailsWithCode("1 + 1", Code.UNSUPPORTED_OPERATION)
  }

  @Test
  fun `it works on supported operation`() {
    EventFilterValidator.validate("age > 30")
  }

  @Test
  fun `comparing field to a list of constants is valid`() {
    EventFilterValidator.validate("age in [10, 20, 30]")
  }

  @Test
  fun `variable is invalid within a list`() {
    assertThatFailsWithCode("age in [a, b, c]", Code.INVALID_VALUE_TYPE)
  }

  @Test
  fun `constant is invalid at the left side of IN operator`() {
    assertThatFailsWithCode("10 in [10, 20, 30]", Code.INVALID_VALUE_TYPE)
  }

  @Test
  fun `a single expression is invalid`() {
    assertThatFailsWithCode("age", Code.EXPRESSION_IS_NOT_CONDITIONAL)
  }

  @Test
  fun `list is not valid outside @in operator`() {
    assertThatFailsWithCode("[1, 2, 3] == [1, 2, 3]", Code.INVALID_VALUE_TYPE)
  }

  @Test
  fun `fields should be compared only at leafs`() {
    assertThatFailsWithCode("field1 == (field2 != field3)", Code.FIELD_COMPARISON_OUTSIDE_LEAF)
  }

  @Test
  fun `a complex comparison works`() {
    EventFilterValidator.validate("age < 20 || age > 50")
  }

  @Test
  fun `it disallows operator outside leafs`() {
    assertThatFailsWithCode("(a == b) <= (field2 < field3)", Code.INVALID_OPERATION_OUTSIDE_LEAF)
  }
}
