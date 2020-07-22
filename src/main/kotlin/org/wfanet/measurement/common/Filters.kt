// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.common

/**
 * Represents a common pattern for filters on lists: the intersection of unions.
 *
 * For example, in SQL this translates to where-clauses of the form:
 *
 *    WHERE (x IN UNNEST(@x1))
 *      AND (y IN UNNEST(@x2))
 *      AND (z > @z1)
 */
data class AllOfClause<V : TerminalClause>(val clauses: Iterable<V>) {
  val empty: Boolean
    get() = !clauses.iterator().hasNext()
}

interface TerminalClause
interface AnyOfClause : TerminalClause
interface GreaterThanClause : TerminalClause

fun <V : TerminalClause> allOf(clauses: Iterable<V>): AllOfClause<V> = AllOfClause(clauses)
fun <V : TerminalClause> allOf(vararg clauses: V): AllOfClause<V> =
  AllOfClause(clauses.asIterable())
