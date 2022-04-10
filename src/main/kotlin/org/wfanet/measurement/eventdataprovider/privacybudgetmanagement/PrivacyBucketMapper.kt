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
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import com.google.api.expr.v1alpha1.Decl
import com.google.api.expr.v1alpha1.Decl.IdentDecl
import com.google.api.expr.v1alpha1.Type
import com.google.api.expr.v1alpha1.Type.PrimitiveType
import com.google.protobuf.Message
import com.google.protobuf.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventGroupEntry
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException

private const val PRIVACY_BUCKET_VID_SAMPLE_WIDTH = 0.01f

private val OPERATIVE_PRIVACY_BUDGET_FIELDS = setOf("privacy_budget.age", "privacy_budget.gender")

fun toDelc(fieldName: String): Decl {
  return Decl.newBuilder()
    .setName(fieldName)
    .setIdent(
      IdentDecl.newBuilder().setType(Type.newBuilder().setPrimitive(PrimitiveType.BOOL).build())
    )
    .build()
}

/**
 * Returns a list of privacy bucket groups that might be affected by a query.
 *
 * @param requisitionSpec The requisitionSpec protobuf that is associated with the query. The date
 * range and demo groups are obtained from this.
 * @param measurementSpec The measurementSpec protobuf that is associated with the query. The VID
 * sampling interval is obtained from from this.
 * @return A list of potentially affected PrivacyBucketGroups. It is guaranteed that the items in
 * this list are disjoint. In the current implementation, each privacy bucket group represents a
 * single privacy bucket.
 */
fun getPrivacyBucketGroups(
  measurementSpec: MeasurementSpec,
  requisitionSpec: RequisitionSpec
): List<PrivacyBucketGroup> {

  val vidSamplingIntervalStart = measurementSpec.reachAndFrequency.vidSamplingInterval.start
  val vidSamplingIntervalWidth = measurementSpec.reachAndFrequency.vidSamplingInterval.width
  val vidSamplingIntervalEnd = vidSamplingIntervalStart + vidSamplingIntervalWidth

  return requisitionSpec
    .getEventGroupsList()
    .flatMap { getPrivacyBucketGroups(it.value, vidSamplingIntervalStart, vidSamplingIntervalEnd) }
    .toList()
}

private fun getPrivacyBucketGroups(
  eventGroupEntryValue: EventGroupEntry.Value,
  vidSamplingIntervalStart: Float,
  vidSamplingIntervalEnd: Float
): Sequence<PrivacyBucketGroup> {
  val program =
    try {
      EventFilters.compileProgram(
        eventGroupEntryValue.filter.expression,
        // TODO(@uakyol) : Update to Event proto once real event templates are checked in.
        testEvent {},
        OPERATIVE_PRIVACY_BUDGET_FIELDS
      )
    } catch (e: EventFilterValidationException) {
      throw PrivacyBudgetManagerException(
        PrivacyBudgetManagerExceptionType.INVLAID_PRIVACY_BUCKET_FILTER,
        emptyList()
      )
    }

  val startDate: LocalDate = eventGroupEntryValue.collectionInterval.startTime.toLocalDate("UTC")
  val endDate: LocalDate = eventGroupEntryValue.collectionInterval.endTime.toLocalDate("UTC")

  val vids =
    PrivacyLandscape.vids.filter { it >= vidSamplingIntervalStart && it <= vidSamplingIntervalEnd }
  val dates =
    PrivacyLandscape.dates.filter {
      (it.isAfter(startDate) || it.isEqual(startDate)) &&
        (it.isBefore(endDate) || it.isEqual(endDate))
    }

  return sequence {
    for (vid in vids) {
      for (date in dates) {
        for (ageGroup in PrivacyLandscape.ageGroups) {
          for (gender in PrivacyLandscape.genders) {
            val privacyBucketGroup =
              PrivacyBucketGroup(
                "ACME",
                date,
                date,
                ageGroup,
                gender,
                vid,
                PRIVACY_BUCKET_VID_SAMPLE_WIDTH
              )
            if (EventFilters.matches(privacyBucketGroup.toEventProto() as Message, program)) {
              yield(privacyBucketGroup)
            }
          }
        }
      }
    }
  }
}

private fun Timestamp.toLocalDate(timeZone: String): LocalDate =
  Instant.ofEpochSecond(this.getSeconds(), this.getNanos().toLong())
    .atZone(ZoneId.of(timeZone)) // This is problematic!
    .toLocalDate()

/*
  //   throw PrivacyBudgetManagementInternalException(
  //     PrivacyBudgetManagementInternalException.Code.BAD_INPUT
  //   )

  //   val decls: EnvOption =
  //     declarations(
  //       Decls.newVar("date", Decls.Int),
  //       Decls.newVar("age", Decls.Int),
  //       Decls.newVar("gender", Decls.String),
  //       Decls.newVar("vid", Decls.Double)
  //     )

  //   val env: Env = newEnv(decls)
  //   val astIss: AstIssuesTuple =
  //     env.compile(
  //       "(date < 30 && age < 30 && age > 18 && vid > 0.2 && gender == 'F' ) || (date > 10 && age
  // < 30 && age > 18 && vid > 0.2 && gender == 'M' )"
  //     )

  //   val prg: Program = env.program(astIss.getAst())
  //   val num_buckets = 10_000

  //   for (i in 1..num_buckets) {
  //     val date = Random.nextInt(0, 100)
  //     val age = Random.nextInt(10, 50)
  //     val vid = Random.nextFloat()
  //     val paramMap: Map<String, Any> =
  //       mapOf("date" to date, "age" to age, "vid" to vid, "gender" to "M")
  //     val out: EvalResult = evaluateCel(prg, paramMap)
  //   }


  // val cont:Container = Container.newContainer(Container.name("ok"));
  // val env: CheckerEnv = newStandardCheckerEnv(cont, reg);

  // val env:Env = Env.newEnv(EnvOption.declarations(toDelc("a"), toDelc("b"), toDelc("c"),
  // toDelc("d"), toDelc("e"), toDelc("f")));

//   val env: Env =
//     Env.newEnv(
//       EnvOption.container("org.wfanet.measurement.api.v2alpha"),
//       EnvOption.types(DataProvider.getDefaultInstance()),
//       EnvOption.declarations(Decls.newVar("x", Decls.newObjectType("google.api.expr.test.v1.proto3.TestAllTypes"))
//     ))

    // val PACKAGE_NAME = "org.wfanet.measurement.api.v2alpha.event_templates.testing"
    val TEMPLATE_PREFIX = "org.wfa.measurement.api.v2alpha.event_templates.testing"
    // val BANNER_TEMPLATE_NAME = "$TEMPLATE_PREFIX.TestBannerTemplate"

//     val haloTypeRegistry = EventTemplateTypeRegistry.createRegistryForPackagePrefix(PACKAGE_NAME)
//     println(haloTypeRegistry.getDescriptorForType(BANNER_TEMPLATE_NAME))

    // this is crucial!!!!! https://github.com/google/cel-spec/blob/master/doc/intro.md

    val expression : String = "(vt.ugur == \"something\") && (bt.age > 5)"
    // this is crazy! this doesn't work
    // val dpType:Type = Decls.newObjectType("org.wfanet.measurement.api.v2alpha.DataProvider");

    // But this works!!!!
    // val dpType:Type = Decls.newObjectType("wfa.measurement.api.v2alpha.DataProvider");
    // val reg:TypeRegistry = ProtoTypeRegistry.newRegistry(DataProvider.getDefaultInstance());

    val dpType:Type = Decls.newObjectType("$TEMPLATE_PREFIX.TestVideoTemplate");
    val btType:Type = Decls.newObjectType("$TEMPLATE_PREFIX.TestBannerTemplate");
    val reg:TypeRegistry = ProtoTypeRegistry.newRegistry(TestVideoTemplate.getDefaultInstance(), TestBannerTemplate.getDefaultInstance());

    val env:Env =
        newEnv(
            EnvOption.customTypeAdapter(reg),
            EnvOption.customTypeProvider(reg),
            // EnvOption.container("org.wfanet.measurement.api.v2alpha"),
            // EnvOption.types(TestVideoTemplate.getDefaultInstance(), TestBannerTemplate.getDefaultInstance()),
            EnvOption.declarations(Decls.newVar("vt", dpType), Decls.newVar("bt", btType)));

    // val src:Source = newTextSource(expression)
    // val astAndIssues: AstIssuesTuple = env.check(env.parseSource(src).ast);
    val astAndIssues: AstIssuesTuple =  env.compile(expression)
    val prg:Program = env.program(astAndIssues.getAst());
    println(astAndIssues.getIssues().toString())
    println(astAndIssues.getAst())
    // val out: EvalResult = prg.eval(hashMapOf("dp" to dataProvider{name  = "something"}))
    val out: EvalResult = prg.eval(hashMapOf("vt" to testVideoTemplate{ugur  = "something"}, "bt" to testBannerTemplate{age  = 10}))

    println(out.getVal().value())
    return listOf<PrivacyBucketGroup>()
}

*/
