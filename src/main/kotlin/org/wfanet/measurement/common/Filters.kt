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
