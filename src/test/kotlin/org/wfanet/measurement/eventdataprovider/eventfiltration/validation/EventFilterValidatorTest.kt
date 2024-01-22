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

import com.google.api.expr.v1alpha1.Constant
import com.google.api.expr.v1alpha1.Decl
import com.google.api.expr.v1alpha1.Expr
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoFluentAssertion
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.projectnessie.cel.Env
import org.projectnessie.cel.EnvOption
import org.projectnessie.cel.checker.Decls
import org.projectnessie.cel.common.types.pb.ProtoTypeRegistry
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Banner
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Video
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException.Code as Code

private const val TEMPLATE_PREFIX = "wfa.measurement.api.v2alpha.event_templates.testing"
private val OPERATIVE_FIELDS =
  setOf("person.age_group", "person.gender", "person.social_grade_group")

@RunWith(JUnit4::class)
class EventFilterValidatorTest {
  private fun env(vararg vars: Decl): Env {
    return Env.newEnv(EnvOption.declarations(*vars))
  }

  private val booleanVar = { name: String -> Decls.newVar(name, Decls.Bool) }
  private val intVar = { name: String -> Decls.newVar(name, Decls.Int) }
  private val stringVar = { name: String -> Decls.newVar(name, Decls.String) }

  private fun bannerTemplateVar(): Decl {
    return Decls.newVar("banner", Decls.newObjectType("$TEMPLATE_PREFIX.Banner"))
  }

  private fun videoTemplateVar(): Decl {
    return Decls.newVar("video", Decls.newObjectType("$TEMPLATE_PREFIX.Video"))
  }

  private fun personTemplateVar(): Decl {
    return Decls.newVar("person", Decls.newObjectType("$TEMPLATE_PREFIX.Person"))
  }

  private fun envWithTestTemplateVars(vararg vars: Decl): Env {
    val typeRegistry: ProtoTypeRegistry =
      ProtoTypeRegistry.newRegistry(
        Banner.getDefaultInstance(),
        Video.getDefaultInstance(),
        Person.getDefaultInstance(),
      )
    return Env.newEnv(
      EnvOption.customTypeAdapter(typeRegistry),
      EnvOption.customTypeProvider(typeRegistry),
      EnvOption.declarations(*vars),
    )
  }

  private fun compile(celExpression: String, env: Env): Expr =
    EventFilterValidator.compile(env, celExpression, emptySet()).expr

  private fun compileToNormalForm(
    celExpression: String,
    env: Env,
    operativeFields: Set<String>,
  ): Expr = EventFilterValidator.compile(env, celExpression, operativeFields).expr

  private inline fun assertFailsWithCode(
    code: Code,
    block: () -> Unit,
  ): EventFilterValidationException {
    return assertFailsWith<EventFilterValidationException> { block() }
      .also { assertThat(it.code).isEqualTo(code) }
  }

  private fun assertIgnoringId(expr: Expr): ProtoFluentAssertion {
    return assertThat(expr).ignoringFields(Expr.ID_FIELD_NUMBER)
  }

  @Test
  fun `fails on invalid operation`() {
    assertFailsWithCode(Code.UNSUPPORTED_OPERATION) { compile("1 + 1", Env.newEnv()) }
  }

  @Test
  fun `fails on invalid expression`() {
    assertFailsWithCode(Code.INVALID_CEL_EXPRESSION) { compile("+", Env.newEnv()) }
  }

  @Test
  fun `works on supported operation`() {
    compile("age > 30", env(intVar("age")))
  }

  @Test
  fun `fails on comparing string to int`() {
    assertFailsWithCode(Code.INVALID_CEL_EXPRESSION) { compile("age > 30", env(stringVar("age"))) }
  }

  @Test
  fun `fails on comparing int to boolean conditional`() {
    assertFailsWithCode(Code.INVALID_CEL_EXPRESSION) {
      compile("age && (date > 10)", env(intVar("age"), intVar("date")))
    }
  }

  @Test
  fun `comparing field to a list of constants is valid`() {
    compile("age in [10, 20, 30]", env(intVar("age")))
  }

  @Test
  fun `comparing field to a list of constants fails if types don't match`() {
    assertFailsWithCode(Code.INVALID_CEL_EXPRESSION) {
      compile("age in [10, 20, 30]", env(stringVar("age")))
    }
  }

  @Test
  fun `variable is invalid within a list`() {
    assertFailsWithCode(Code.INVALID_VALUE_TYPE) {
      compile("age in [a, b, c]", env(intVar("age"), intVar("a"), intVar("b"), intVar("c")))
    }
  }

  @Test
  fun `constant is invalid at the left side of IN operator`() {
    assertFailsWithCode(Code.INVALID_VALUE_TYPE) { compile("10 in [10, 20, 30]", Env.newEnv()) }
  }

  @Test
  fun `a single expression is invalid`() {
    assertFailsWithCode(Code.EXPRESSION_IS_NOT_CONDITIONAL) {
      compile("age", env(stringVar("age")))
    }
  }

  @Test
  fun `list is not valid outside @in operator`() {
    assertFailsWithCode(Code.INVALID_VALUE_TYPE) { compile("[1, 2, 3] == [1, 2, 3]", Env.newEnv()) }
  }

  @Test
  fun `fields should be compared only at leafs`() {
    assertFailsWithCode(Code.FIELD_COMPARISON_OUTSIDE_LEAF) {
      compile(
        "field1 == (field2 != field3)",
        env(booleanVar("field1"), stringVar("field2"), stringVar("field3")),
      )
    }
  }

  @Test
  fun `a complex comparison works`() {
    compile("age < 20 || age > 50", env(intVar("age")))
  }

  @Test
  fun `it disallows operator outside leafs`() {
    assertFailsWithCode(Code.INVALID_OPERATION_OUTSIDE_LEAF) {
      compile(
        "(a == b) <= (field2 < field3)",
        env(intVar("a"), intVar("b"), stringVar("field2"), stringVar("field3")),
      )
    }
  }

  @Test
  fun `fields can be compared between each other`() {
    compile("field1 == field2", env(intVar("field1"), intVar("field2")))
  }

  @Test
  fun `fails on non-existent template field`() {
    assertFailsWithCode(Code.INVALID_CEL_EXPRESSION) {
      compile("vt.date == 10", envWithTestTemplateVars(videoTemplateVar()))
    }
  }

  @Test
  fun `can use valid template fields on comparison`() {
    compile(
      "banner.viewable == true && person.age_group == 1",
      envWithTestTemplateVars(bannerTemplateVar(), personTemplateVar()),
    )
  }

  @Test
  fun `can use template with IN operator`() {
    compile("person.age_group in [0, 1]", envWithTestTemplateVars(personTemplateVar()))
  }

  @Test
  fun `can use template with has operator`() {
    compile("has(person.age_group)", envWithTestTemplateVars(personTemplateVar()))
  }

  @Test
  fun `can use template with has operator with other operators`() {
    compile(
      "has(person.age_group) && person.age_group in [0, 1]",
      envWithTestTemplateVars(personTemplateVar()),
    )
  }

  @Test
  fun `compiles to Normal Form correctly with single operative field`() {
    val env = envWithTestTemplateVars(personTemplateVar())

    val expression = "person.age_group in [0, 1]"
    val expectedNormalizedExpression = "person.age_group in [0, 1]"

    assertIgnoringId(compileToNormalForm(expression, env, OPERATIVE_FIELDS))
      .isEqualTo(compile(expectedNormalizedExpression, env))
  }

  @Test
  @Ignore("Not implemented") // TODO(world-federation-of-advertisers/cross-media-measurement#819)
  fun `compiles to Normal Form correctly with non-scalar operative field child`() {
    val env = envWithTestTemplateVars(videoTemplateVar(), personTemplateVar())
    // Note that this is a contrived example, as this field would be non-operative in practice.
    val operativeFields = OPERATIVE_FIELDS.plus("video.length")
    val expression = "video.length.seconds > 30"

    assertIgnoringId(compileToNormalForm(expression, env, operativeFields))
      .isEqualTo(compile(expression, env))
  }

  @Test
  fun `compiles to Normal Form correctly with single non operative field`() {
    val env = envWithTestTemplateVars(videoTemplateVar(), personTemplateVar())

    val expression = "video.viewed_fraction > 0.25"
    val expectedCompiledNormalizedExpression =
      Expr.newBuilder().setConstExpr(Constant.newBuilder().setBoolValue(true)).build()

    assertIgnoringId(compileToNormalForm(expression, env, OPERATIVE_FIELDS))
      .isEqualTo(expectedCompiledNormalizedExpression)
  }

  @Test
  fun `compiles to Normal Form correctly with single non operative field with presence check`() {
    val env = envWithTestTemplateVars(videoTemplateVar(), personTemplateVar())

    val expression = "has(video.viewed_fraction)"
    val expectedCompiledNormalizedExpression =
      Expr.newBuilder().setConstExpr(Constant.newBuilder().setBoolValue(true)).build()

    assertIgnoringId(compileToNormalForm(expression, env, OPERATIVE_FIELDS))
      .isEqualTo(expectedCompiledNormalizedExpression)
  }

  @Test
  fun `compiles to Normal Form correctly with single operative field with presence check`() {
    val env = envWithTestTemplateVars(videoTemplateVar(), personTemplateVar())

    // Note that this is a contrived example, as the value field is scalar and therefore always
    // present. In practice, presence checks are only useful on wrapper messages.
    val expression = "has(person.age_group)"
    val expectedNormalizedExpression = "has(person.age_group)"

    assertIgnoringId(compileToNormalForm(expression, env, OPERATIVE_FIELDS))
      .isEqualTo(compile(expectedNormalizedExpression, env))
  }

  @Test
  fun `compiles to Normal Form correctly with non operative fields`() {
    val env = envWithTestTemplateVars(bannerTemplateVar(), personTemplateVar())

    val expression = "banner.viewable == true && person.age_group == 1"
    val expectedNormalizedExpression = "true && person.age_group == 1"

    assertIgnoringId(compileToNormalForm(expression, env, OPERATIVE_FIELDS))
      .isEqualTo(compile(expectedNormalizedExpression, env))
  }

  @Test
  fun `compiles to Normal Form correctly with no non operative fields and negation`() {
    val env = envWithTestTemplateVars(personTemplateVar())
    val expression = "!(person.gender == 2 && person.age_group == 1)"
    val expectedNormalizedExpression = "!(person.gender == 2) || !(person.age_group == 1)"

    assertIgnoringId(compileToNormalForm(expression, env, OPERATIVE_FIELDS))
      .isEqualTo(compile(expectedNormalizedExpression, env))
  }

  @Test
  fun `compiles to Normal Form correctly with non operative fields and negation`() {
    val env = envWithTestTemplateVars(personTemplateVar(), videoTemplateVar())

    val expression =
      "!(person.gender == 2 && person.age_group == 1) && " + "!(video.viewed_fraction > 0.25)"
    val expectedNormalizedExpression = "(!(person.gender == 2) || !(person.age_group == 1)) && true"

    assertIgnoringId(compileToNormalForm(expression, env, OPERATIVE_FIELDS))
      .isEqualTo(compile(expectedNormalizedExpression, env))
  }

  @Test
  fun `compiles to Normal Form correctly with complex expression`() {
    val env = envWithTestTemplateVars(personTemplateVar(), videoTemplateVar())

    val expression =
      """
      !(person.gender == 2  || video.viewed_fraction > 0.25) && !(
        person.age_group == 1 || (
          person.age_group == 2 || !(
            (video.viewed_fraction > 0.25 && video.viewed_fraction < 1.0)
          )
        )
      )
      """
        .trim()

    val expectedNormalizedExpression =
      "(!(person.gender == 2)  && true) && (!(person.age_group == 1) && " +
        "(!(person.age_group == 2) && ((true && true))) )"

    assertIgnoringId(compileToNormalForm(expression, env, OPERATIVE_FIELDS))
      .isEqualTo(compile(expectedNormalizedExpression, env))
  }
}
