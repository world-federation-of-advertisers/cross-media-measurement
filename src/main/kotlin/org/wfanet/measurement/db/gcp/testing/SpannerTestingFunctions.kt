package org.wfanet.measurement.db.gcp.testing

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Returns the results of a spanner query as a list of [Struct].
 */
fun queryForResults(dbClient: DatabaseClient, sqlQuery: String): List<Struct> {
  val resultSet = dbClient.singleUse().executeQuery(Statement.of(sqlQuery))
  val result = mutableListOf<Struct>()
  while (resultSet.next()) {
    result.add(resultSet.currentRowAsStruct)
  }
  return result
}

/**
 * Asserts that a query returns the expected results.
 */
fun assertQueryReturns(dbClient: DatabaseClient, sqlQuery: String, vararg expected: Struct) {
  val expectedList = expected.toList()
  val expectedColumns = expectedList.map { it.type.toString() }.toSet()
  val results = queryForResults(dbClient, sqlQuery)
  val resultsColumns = results.map { it.type.toString() }.toSet()
  assertTrue(
    expectedColumns.size == 1,
    "All 'expected: Struct' object should have the same column headings, " +
      "but was ${expectedColumns.joinToString("\n")}"
  )
  assertTrue(
    resultsColumns.size == 1,
    "All query results to have the same column headings, " +
      "but was ${resultsColumns.joinToString("\n")}"
  )
  assertEquals(expectedColumns, resultsColumns)
  assertEquals(
    expected.toList(), results,
    """
    Expected:
      Columns (should be one item)
        $expectedColumns
      Values (one item per row)
        ${expectedList.debugString()}
    but was:
      Columns (should be one item)
        $resultsColumns
      Values (one item per row)
        ${results.debugString()}
    """.trimIndent()
  )
}

fun assertQueryReturnsNothing(dbClient: DatabaseClient, sqlQuery: String) {
  val results = queryForResults(dbClient, sqlQuery)
  val resultsColumns = results.map { it.type.toString() }.toSet()
  assertTrue(
    results.isEmpty(),
    "Expected no results, but got $resultsColumns with values ${results.debugString()}"
  )
}

private fun List<Struct>.debugString(): String {
  return this.map(Struct::toString).joinToString("\n", postfix = "\n")
}
