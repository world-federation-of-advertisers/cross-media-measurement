/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.tools

import com.google.cloud.spanner.Database as SpannerDatabase
import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.TransactionContext
import com.google.cloud.spanner.jdbc.CloudSpannerJdbcConnection
import liquibase.change.custom.CustomTaskChange
import liquibase.database.Database
import liquibase.exception.ValidationErrors
import liquibase.resource.ResourceAccessor
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.insertOrUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement

@Suppress("unused") // Used at runtime by Liquibase.
class CopyEventGroups : CustomTaskChange {
  private var rowCount = 0

  override fun getConfirmationMessage(): String {
    return "Copied $rowCount rows from EventGroups to EventGroupsNew"
  }

  override fun setUp() {}

  override fun setFileOpener(resourceAccessor: ResourceAccessor) {}

  override fun validate(database: Database): ValidationErrors {
    return ValidationErrors()
  }

  override fun execute(database: Database) {
    val connection =
      database.connection.underlyingConnection.unwrap(CloudSpannerJdbcConnection::class.java)

    // Work around https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/issues/251
    dropForeignKeyConstraints(connection)

    val dbClient: DatabaseClient = connection.databaseClient
    var dataProviderId = INVALID_ID
    var eventGroupId = INVALID_ID
    do {
      dbClient.readWriteTransaction().run { txn: TransactionContext ->
        val query =
          statement(BASE_SQL) {
            if (dataProviderId != INVALID_ID) {
              appendClause(WHERE_CLAUSE)
              bind("dataProviderId").to(dataProviderId)
              bind("eventGroupId").to(eventGroupId)
            }
            appendClause(ORDER_BY_CLAUSE)
            appendClause(LIMIT_CLAUSE)
          }
        txn.executeQuery(query).use { resultSet ->
          dataProviderId = INVALID_ID
          while (resultSet.next()) {
            rowCount++
            dataProviderId = resultSet.getLong("DataProviderId")
            eventGroupId = resultSet.getLong("EventGroupId")

            txn.buffer(
              insertOrUpdateMutation("EventGroupsNew") {
                for (columnName in resultSet.type.structFields.map { it.name }) {
                  if (columnName == "MediaTypes" || resultSet.isNull(columnName)) {
                    continue
                  }
                  set(columnName).to(resultSet.getValue(columnName))
                }
              }
            )
            for (mediaType in resultSet.getLongArray("MediaTypes")) {
              txn.buffer(
                insertOrUpdateMutation("EventGroupMediaTypesNew") {
                  set("DataProviderId").to(dataProviderId)
                  set("EventGroupId").to(eventGroupId)
                  set("MediaType").to(mediaType)
                }
              )
            }
          }
        }
      }
    } while (dataProviderId != INVALID_ID)
  }

  private fun dropForeignKeyConstraints(connection: CloudSpannerJdbcConnection) {
    val sql =
      """
      SELECT CONSTRAINT_NAME
      FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
      WHERE
        TABLE_NAME = 'EventGroups'
        AND CONSTRAINT_TYPE = 'FOREIGN KEY'
      """
        .trimIndent()
    val constraintNames: List<String> =
      connection.databaseClient.singleUse().executeQuery(statement(sql)).use { resultSet ->
        buildList {
          while (resultSet.next()) {
            add(resultSet.getString(0))
          }
        }
      }
    val statements = constraintNames.map { "ALTER TABLE EventGroups DROP CONSTRAINT $it" }

    val database: SpannerDatabase =
      connection.spanner.databaseAdminClient.newDatabaseBuilder(connection.databaseId).build()
    database.updateDdl(statements, null).get()
  }

  companion object {
    private const val BATCH_SIZE = 100
    private const val INVALID_ID = 0L

    private val BASE_SQL =
      """
      SELECT
        *,
        ARRAY(
          SELECT MediaType
          FROM EventGroupMediaTypes
          WHERE
            EventGroupMediaTypes.DataProviderId = EventGroups.DataProviderId
            AND EventGroupMediaTypes.EventGroupId = EventGroups.EventGroupId
        ) AS MediaTypes,
      FROM EventGroups
      """
        .trimIndent()
    private val WHERE_CLAUSE =
      """
      WHERE
        DataProviderId > @dataProviderId
        OR (
          DataProviderId = @dataProviderId
          AND EventGroupId > @eventGroupId
        )
      """
        .trimIndent()
    private const val ORDER_BY_CLAUSE = "ORDER BY DataProviderId, EventGroupId"
    private const val LIMIT_CLAUSE = "LIMIT $BATCH_SIZE"
  }
}
