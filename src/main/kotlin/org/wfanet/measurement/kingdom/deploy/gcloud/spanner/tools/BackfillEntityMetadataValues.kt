// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.tools

import com.google.protobuf.Struct
import java.nio.file.Path
import java.util.logging.FileHandler
import java.util.logging.Logger
import java.util.logging.SimpleFormatter
import kotlin.properties.Delegates
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EntityMetadataValues
import picocli.CommandLine

/** Backfills the values-only search representation without changing EventGroup timestamps. */
@CommandLine.Command(
  name = "BackfillEntityMetadataValues",
  description = ["Backfill missing EventGroup entity metadata search values."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
class BackfillEntityMetadataValues : Runnable {
  @CommandLine.Mixin private lateinit var spannerFlags: SpannerFlags

  @set:CommandLine.Option(
    names = ["--batch-size"],
    description = ["Maximum rows per write transaction."],
    defaultValue = "100",
  )
  private var batchSize by Delegates.notNull<Int>()

  @set:CommandLine.Option(
    names = ["--dry-run"],
    description = ["Count missing rows without writing."],
    defaultValue = "false",
  )
  private var dryRun by Delegates.notNull<Boolean>()

  @CommandLine.Option(
    names = ["--log-file"],
    description = ["Append progress and completion logs to this file."],
    defaultValue = "entity-metadata-values-backfill.log",
  )
  private lateinit var logFile: Path

  override fun run() {
    require(batchSize > 0) { "batch-size must be positive" }
    val handler = FileHandler(logFile.toString(), true).apply { formatter = SimpleFormatter() }
    logger.addHandler(handler)
    try {
      val rowCount = runBlocking {
        spannerFlags.usingSpanner { spanner -> countOrBackfill(spanner.databaseClient) }
      }
      logger.info { "Completed entity metadata values backfill: rows=$rowCount, dryRun=$dryRun" }
    } finally {
      logger.removeHandler(handler)
      handler.close()
    }
  }

  private suspend fun countOrBackfill(client: AsyncDatabaseClient): Long {
    if (dryRun) {
      return client
        .singleUse()
        .executeQuery(
          statement("SELECT COUNT(*) AS MissingEntityMetadataValues $MISSING_VALUES_SQL")
        )
        .single()
        .getLong("MissingEntityMetadataValues")
    }

    var after: RowKey? = null
    var rowCount = 0L
    while (true) {
      val cursor = after
      val rows =
        client.readWriteTransaction().run { transaction ->
          val query =
            statement("SELECT DataProviderId, EventGroupId, EntityMetadata $MISSING_VALUES_SQL") {
              if (cursor != null) {
                appendClause(
                  "AND (DataProviderId > @dataProviderId " +
                    "OR (DataProviderId = @dataProviderId AND EventGroupId > @eventGroupId))"
                )
                bind("dataProviderId").to(cursor.dataProviderId)
                bind("eventGroupId").to(cursor.eventGroupId)
              }
              appendClause("ORDER BY DataProviderId, EventGroupId LIMIT @batchSize")
              bind("batchSize").to(batchSize.toLong())
            }
          val batch = transaction.executeQuery(query).toList()
          for (row in batch) {
            val metadata =
              if (row.isNull("EntityMetadata")) Struct.getDefaultInstance()
              else row.getProtoMessage("EntityMetadata", Struct.getDefaultInstance())
            transaction.bufferUpdateMutation("EventGroups") {
              set("DataProviderId").to(row.getLong("DataProviderId"))
              set("EventGroupId").to(row.getLong("EventGroupId"))
              set("EntityMetadataValues").toStringArray(EntityMetadataValues.flatten(metadata))
            }
          }
          batch
        }
      if (rows.isEmpty()) {
        break
      }
      // Advance only after commit; retries must read the same batch and its current protobufs.
      val last = rows.last()
      after = RowKey(last.getLong("DataProviderId"), last.getLong("EventGroupId"))
      rowCount += rows.size
      logger.info { "Backfilled $rowCount EventGroups" }
    }
    return rowCount
  }

  private data class RowKey(val dataProviderId: Long, val eventGroupId: Long)

  companion object {
    private val logger =
      Logger.getLogger(BackfillEntityMetadataValues::class.java.name).apply {
        useParentHandlers = false
      }
    private const val MISSING_VALUES_SQL = "FROM EventGroups WHERE EntityMetadataValues IS NULL"
  }
}

fun main(args: Array<String>) = commandLineMain(BackfillEntityMetadataValues(), args)
