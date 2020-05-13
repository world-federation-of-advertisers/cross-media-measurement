package org.wfanet.measurement.service.db.gcp.testing

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import kotlin.test.assertEquals

/**
 * Returns the results of a spanner query as a list of [Struct].
 */
fun queryForResults(dbClient: DatabaseClient, sqlQuery: String) : List<Struct> {
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
fun assertQueryReturns(dbClient: DatabaseClient, sqlQuery: String, vararg  expected: Struct) {
  val results = queryForResults(dbClient, sqlQuery)
  assertEquals(expected.toList(), results,
   """
   Expected:
     Columns (should be one item)
       ${expected.toList().map { it.type.toString() } .toSet()}
     Values (one item per row)
       ${expected.map { it.toString() + '\n' }}
   but was:
     Columns (should be one item)
       ${results.map { it.type.toString() } .toSet()}
     Values (one item per row)
       ${results.map { it.toString() + '\n' }}
   """.trimIndent())
}
