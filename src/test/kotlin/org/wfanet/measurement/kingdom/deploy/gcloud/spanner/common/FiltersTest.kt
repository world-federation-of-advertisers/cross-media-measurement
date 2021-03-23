// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.AnyOfClause
import org.wfanet.measurement.common.GreaterThanClause
import org.wfanet.measurement.common.TerminalClause
import org.wfanet.measurement.common.allOf

@RunWith(JUnit4::class)
class FiltersTest {
  sealed class Foo : TerminalClause {
    data class A(val a: List<Long>) : Foo(), AnyOfClause
    data class B(val b: List<String>) : Foo(), AnyOfClause
    data class C(val c: Long) : Foo(), GreaterThanClause
  }

  object FooSqlConverter : SqlConverter<Foo> {
    override fun sqlData(v: Foo): SqlConverter.SqlData = when (v) {
      is Foo.A -> SqlConverter.SqlData("fieldA", "bindingA", Value.int64Array(v.a))
      is Foo.B -> SqlConverter.SqlData("fieldB", "bindingB", Value.stringArray(v.b))
      is Foo.C -> SqlConverter.SqlData("fieldC", "bindingC", Value.int64(v.c))
    }
  }

  @Test
  fun `empty AllOfClause`() {
    assertTrue(allOf<Foo>().empty)
    assertFalse(allOf<Foo>(Foo.A(listOf())).empty)
  }

  @Test
  fun `toSqlTest single clause`() {
    val queryBuilder = Statement.newBuilder("WHERE ")
    val filter = allOf<Foo>(
      Foo.A(listOf(1L, 2L, 3L))
    )
    filter.toSql(queryBuilder, FooSqlConverter)
    val query: Statement = queryBuilder.build()

    assertThat(query.sql)
      .isEqualTo("WHERE (fieldA IN UNNEST(@bindingA))")

    assertThat(query.parameters)
      .containsExactly("bindingA", Value.int64Array(listOf(1L, 2L, 3L)))
  }

  @Test
  fun `toSqlTest multiple clauses`() {
    val queryBuilder = Statement.newBuilder("WHERE ")
    val filter = allOf(
      Foo.A(listOf(1L, 2L, 3L)),
      Foo.B(listOf("a", "b", "c")),
      Foo.C(456)
    )
    filter.toSql(queryBuilder, FooSqlConverter)
    val query: Statement = queryBuilder.build()

    assertThat(query.sql).isEqualTo(
      """
      |WHERE (fieldA IN UNNEST(@bindingA))
      |  AND (fieldB IN UNNEST(@bindingB))
      |  AND (fieldC > @bindingC)
      """.trimMargin()
    )

    assertThat(query.parameters)
      .containsExactly(
        "bindingA",
        Value.int64Array(listOf(1L, 2L, 3L)),
        "bindingB",
        Value.stringArray(listOf("a", "b", "c")),
        "bindingC",
        Value.int64(456)
      )
  }
}
