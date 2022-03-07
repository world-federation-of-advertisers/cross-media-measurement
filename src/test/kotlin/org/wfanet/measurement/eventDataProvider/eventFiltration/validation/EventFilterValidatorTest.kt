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

package org.wfanet.measurement.eventDataProvider.eventFiltration.validation

import com.google.api.expr.v1alpha1.Decl
import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.projectnessie.cel.Env
import org.projectnessie.cel.EnvOption
import org.projectnessie.cel.checker.Decls
import org.projectnessie.cel.common.types.pb.ProtoTypeRegistry
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplate
import org.wfanet.measurement.eventDataProvider.eventFiltration.validation.EventFilterValidationException.Code as Code

private const val TEMPLATE_PREFIX = "org.wfa.measurement.api.v2alpha.event_templates.testing"

@RunWith(JUnit4::class)
class EventFilterValidatorTest {
  private fun env(vararg vars: Decl): Env {
    return Env.newEnv(EnvOption.declarations(*vars))
  }
  private val booleanVar = { name: String -> Decls.newVar(name, Decls.Bool) }
  private val intVar = { name: String -> Decls.newVar(name, Decls.Int) }
  private val stringVar = { name: String -> Decls.newVar(name, Decls.String) }
  private fun videoTemplateVar(name: String): Decl {
    return Decls.newVar(
      name,
      Decls.newObjectType("$TEMPLATE_PREFIX.TestVideoTemplate"),
    )
  }

  private fun envWithTestVideoTemplate(vararg vars: Decl): Env {
    var typeRegistry: ProtoTypeRegistry =
      ProtoTypeRegistry.newRegistry(
        TestVideoTemplate.getDefaultInstance(),
      )
    return Env.newEnv(
      EnvOption.customTypeAdapter(typeRegistry),
      EnvOption.customTypeProvider(typeRegistry),
      EnvOption.declarations(*vars),
    )
  }

  private fun assertThatFailsWithCode(celExpression: String, code: Code, env: Env = Env.newEnv()) {
    val e =
      assertFailsWith(EventFilterValidationException::class) {
        EventFilterValidator.validate(celExpression, env)
      }
    assertThat(e.code).isEqualTo(code)
  }

  @Test
  fun `fails on invalid operation`() {
    assertThatFailsWithCode("1 + 1", Code.UNSUPPORTED_OPERATION)
  }

  @Test
  fun `works on supported operation`() {
    EventFilterValidator.validate(
      "age > 30",
      env(intVar("age")),
    )
  }

  @Test
  fun `fails on comparing string to int`() {
    assertThatFailsWithCode(
      "age > 30",
      Code.INVALID_CEL_EXPRESSION,
      env(stringVar("age")),
    )
  }

  @Test
  fun `fails on comparing int to boolean conditional`() {
    assertThatFailsWithCode(
      "age && (date > 10)",
      Code.INVALID_CEL_EXPRESSION,
      env(
        intVar("age"),
        intVar("date"),
      )
    )
  }

  @Test
  fun `comparing field to a list of constants is valid`() {
    EventFilterValidator.validate(
      "age in [10, 20, 30]",
      env(intVar("age")),
    )
  }

  @Test
  fun `comparing field to a list of constants fails if types don't match`() {
    assertThatFailsWithCode(
      "age in [10, 20, 30]",
      Code.INVALID_CEL_EXPRESSION,
      env(stringVar("age")),
    )
  }

  @Test
  fun `variable is invalid within a list`() {
    assertThatFailsWithCode(
      "age in [a, b, c]",
      Code.INVALID_VALUE_TYPE,
      env(
        intVar("age"),
        intVar("a"),
        intVar("b"),
        intVar("c"),
      )
    )
  }

  @Test
  fun `constant is invalid at the left side of IN operator`() {
    assertThatFailsWithCode("10 in [10, 20, 30]", Code.INVALID_VALUE_TYPE)
  }

  @Test
  fun `a single expression is invalid`() {
    assertThatFailsWithCode("age", Code.EXPRESSION_IS_NOT_CONDITIONAL, env(stringVar("age")))
  }

  @Test
  fun `list is not valid outside @in operator`() {
    assertThatFailsWithCode("[1, 2, 3] == [1, 2, 3]", Code.INVALID_VALUE_TYPE)
  }

  @Test
  fun `fields should be compared only at leafs`() {
    assertThatFailsWithCode(
      "field1 == (field2 != field3)",
      Code.FIELD_COMPARISON_OUTSIDE_LEAF,
      env(
        booleanVar("field1"),
        stringVar("field2"),
        stringVar("field3"),
      ),
    )
  }

  @Test
  fun `a complex comparison works`() {
    EventFilterValidator.validate(
      "age < 20 || age > 50",
      env(intVar("age")),
    )
  }

  @Test
  fun `it disallows operator outside leafs`() {
    assertThatFailsWithCode(
      "(a == b) <= (field2 < field3)",
      Code.INVALID_OPERATION_OUTSIDE_LEAF,
      env(
        intVar("a"),
        intVar("b"),
        stringVar("field2"),
        stringVar("field3"),
      ),
    )
  }

  @Test
  fun `fields can be compared between each other`() {
    EventFilterValidator.validate(
      "field1 == field2",
      env(
        intVar("field1"),
        intVar("field2"),
      ),
    )
  }

  @Test
  fun `fails on inexistant template field`() {
    assertThatFailsWithCode(
      "vt.date == 10",
      Code.INVALID_CEL_EXPRESSION,
      envWithTestVideoTemplate(videoTemplateVar("vt"))
    )
  }

  @Test
  fun `can use valid template fields on comparison`() {
    EventFilterValidator.validate(
      "vt.age.value == 1",
      envWithTestVideoTemplate(videoTemplateVar("vt"))
    )
  }

  @Test
  fun `can use template with IN operator`() {
    EventFilterValidator.validate(
      "vt.age.value in [0, 1]",
      envWithTestVideoTemplate(videoTemplateVar("vt"))
    )
  }
}
