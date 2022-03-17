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
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplate
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException.Code as Code

private const val TEMPLATE_PREFIX = "wfa.measurement.api.v2alpha.event_templates.testing"

@RunWith(JUnit4::class)
class EventFilterValidatorTest {
  private fun env(vararg vars: Decl): Env {
    return Env.newEnv(EnvOption.declarations(*vars))
  }
  private val booleanVar = { name: String -> Decls.newVar(name, Decls.Bool) }
  private val intVar = { name: String -> Decls.newVar(name, Decls.Int) }
  private val stringVar = { name: String -> Decls.newVar(name, Decls.String) }
  private fun bannerTemplateVar(name: String): Decl {
    return Decls.newVar(
      name,
      Decls.newObjectType("$TEMPLATE_PREFIX.TestBannerTemplate"),
    )
  }
  private fun videoTemplateVar(name: String): Decl {
    return Decls.newVar(
      name,
      Decls.newObjectType("$TEMPLATE_PREFIX.TestVideoTemplate"),
    )
  }

  private fun envWithTestTemplateVars(vararg vars: Decl): Env {
    var typeRegistry: ProtoTypeRegistry =
      ProtoTypeRegistry.newRegistry(
        TestBannerTemplate.getDefaultInstance(),
        TestVideoTemplate.getDefaultInstance(),
      )
    return Env.newEnv(
      EnvOption.customTypeAdapter(typeRegistry),
      EnvOption.customTypeProvider(typeRegistry),
      EnvOption.declarations(*vars),
    )
  }

  private fun assertFailsWithCode(celExpression: String, code: Code, env: Env = Env.newEnv()) {
    val e =
      assertFailsWith(EventFilterValidationException::class) {
        EventFilterValidator.compile(celExpression, env)
      }
    assertThat(e.code).isEqualTo(code)
  }

  @Test
  fun `fails on invalid operation`() {
    assertFailsWithCode("1 + 1", Code.UNSUPPORTED_OPERATION)
  }

  @Test
  fun `fails on invalid expression`() {
    assertFailsWithCode("+", Code.INVALID_CEL_EXPRESSION)
  }

  @Test
  fun `works on supported operation`() {
    EventFilterValidator.compile(
      "age > 30",
      env(intVar("age")),
    )
  }

  @Test
  fun `fails on comparing string to int`() {
    assertFailsWithCode(
      "age > 30",
      Code.INVALID_CEL_EXPRESSION,
      env(stringVar("age")),
    )
  }

  @Test
  fun `fails on comparing int to boolean conditional`() {
    assertFailsWithCode(
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
    EventFilterValidator.compile(
      "age in [10, 20, 30]",
      env(intVar("age")),
    )
  }

  @Test
  fun `comparing field to a list of constants fails if types don't match`() {
    assertFailsWithCode(
      "age in [10, 20, 30]",
      Code.INVALID_CEL_EXPRESSION,
      env(stringVar("age")),
    )
  }

  @Test
  fun `variable is invalid within a list`() {
    assertFailsWithCode(
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
    assertFailsWithCode("10 in [10, 20, 30]", Code.INVALID_VALUE_TYPE)
  }

  @Test
  fun `a single expression is invalid`() {
    assertFailsWithCode("age", Code.EXPRESSION_IS_NOT_CONDITIONAL, env(stringVar("age")))
  }

  @Test
  fun `list is not valid outside @in operator`() {
    assertFailsWithCode("[1, 2, 3] == [1, 2, 3]", Code.INVALID_VALUE_TYPE)
  }

  @Test
  fun `fields should be compared only at leafs`() {
    assertFailsWithCode(
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
    EventFilterValidator.compile(
      "age < 20 || age > 50",
      env(intVar("age")),
    )
  }

  @Test
  fun `it disallows operator outside leafs`() {
    assertFailsWithCode(
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
    EventFilterValidator.compile(
      "field1 == field2",
      env(
        intVar("field1"),
        intVar("field2"),
      ),
    )
  }

  @Test
  fun `fails on inexistant template field`() {
    assertFailsWithCode(
      "vt.date == 10",
      Code.INVALID_CEL_EXPRESSION,
      envWithTestTemplateVars(videoTemplateVar("vt"))
    )
  }

  @Test
  fun `can use valid template fields on comparison`() {
    EventFilterValidator.compile(
      "bt.gender.value == 2 && vt.age.value == 1",
      envWithTestTemplateVars(
        bannerTemplateVar("bt"),
        videoTemplateVar("vt"),
      )
    )
  }

  @Test
  fun `can use template with IN operator`() {
    EventFilterValidator.compile(
      "vt.age.value in [0, 1]",
      envWithTestTemplateVars(videoTemplateVar("vt"))
    )
  }
}
