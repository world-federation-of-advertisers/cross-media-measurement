/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.tools

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.logging.LogEntry
import com.google.cloud.logging.Logging
import com.google.cloud.logging.Logging.EntryListOption
import com.google.cloud.logging.Logging.SortingField
import com.google.cloud.logging.Logging.SortingOrder
import com.google.cloud.logging.LoggingOptions
import com.google.cloud.logging.Payload
import com.google.cloud.sql.core.GcpConnectionFactoryProvider
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.google.protobuf.util.Timestamps
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeParseException
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.gcloud.spanner.SpannerDatabaseConnector
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import org.wfanet.measurement.reporting.deploy.v2.common.SpannerFlags
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.getBasicReportByExternalId
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MetricReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportReader
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.MetricKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import picocli.CommandLine

private const val REPORT_NOT_CREATED = "(not created)"

/** Identifiers that connect one BasicReport to work performed by downstream services. */
internal data class ReportTraceContext(
  val basicReportName: String?,
  val basicReportState: String?,
  val reportName: String,
  val metricNames: List<String>,
  val measurementNames: List<String>,
  val createTime: Instant?,
) {
  val correlationValues: List<String>
    get() =
      buildList {
          if (basicReportName != null) {
            add(basicReportName)
          }
          if (reportName != REPORT_NOT_CREATED) {
            add(reportName)
          }
          addAll(metricNames)
          addAll(measurementNames)
        }
        .distinct()
}

/** A single Cloud Logging entry in an end-to-end report timeline. */
internal data class ReportTraceLogEntry(
  val timestamp: Instant,
  val service: String,
  val severity: String,
  val trace: String?,
  val message: String,
)

internal fun interface ReportTraceLogReader {
  fun read(
    correlationValues: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): List<ReportTraceLogEntry>
}

internal fun interface ReportTraceSpanReader {
  fun read(
    project: String,
    reportName: String,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): List<ReportTraceLogEntry>
}

internal fun interface BasicReportTraceResolver {
  suspend fun resolve(basicReportKey: BasicReportKey): ReportTraceContext
}

/** Resolves the durable resource-name chain stored by the Reporting service. */
internal class DatabaseBasicReportTraceResolver(
  private val spannerClient: org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient,
  private val postgresClient: PostgresDatabaseClient,
) : BasicReportTraceResolver {
  override suspend fun resolve(basicReportKey: BasicReportKey): ReportTraceContext {
    val basicReport =
      spannerClient.readOnlyTransaction().use { transaction ->
        transaction
          .getBasicReportByExternalId(
            basicReportKey.cmmsMeasurementConsumerId,
            basicReportKey.basicReportId,
          )
          .basicReport
      }

    if (basicReport.externalReportId.isEmpty()) {
      return ReportTraceContext(
        basicReportName = basicReportKey.toName(),
        basicReportState = basicReport.state.name,
        reportName = REPORT_NOT_CREATED,
        metricNames = emptyList(),
        measurementNames = emptyList(),
        createTime = Instant.ofEpochMilli(Timestamps.toMillis(basicReport.createTime)),
      )
    }

    val reportName =
      ReportKey(basicReport.cmmsMeasurementConsumerId, basicReport.externalReportId).toName()
    val readContext = postgresClient.readTransaction()
    try {
      val reportResult =
        checkNotNull(
          ReportReader(readContext)
            .readReportByExternalId(
              basicReport.cmmsMeasurementConsumerId,
              basicReport.externalReportId,
            )
        ) {
          "Associated Report $reportName was not found"
        }

      val createMetricRequestIds =
        reportResult.report.reportingMetricEntriesMap.values
          .flatMap { it.metricCalculationSpecReportingMetricsList }
          .flatMap { it.reportingMetricsList }
          .map { it.createMetricRequestId }
          .distinct()
      val metricResults =
        MetricReader(readContext)
          .readMetricsByRequestId(reportResult.measurementConsumerId, createMetricRequestIds)
          .toList()

      val metricNames =
        metricResults
          .map {
            MetricKey(basicReport.cmmsMeasurementConsumerId, it.metric.externalMetricId).toName()
          }
          .distinct()
          .sorted()
      val measurementNames =
        metricResults
          .flatMap { it.metric.weightedMeasurementsList }
          .mapNotNull { it.measurement.cmmsMeasurementId.takeIf(String::isNotEmpty) }
          .map { MeasurementKey(basicReport.cmmsMeasurementConsumerId, it).toName() }
          .distinct()
          .sorted()

      return ReportTraceContext(
        basicReportName = basicReportKey.toName(),
        basicReportState = basicReport.state.name,
        reportName = reportName,
        metricNames = metricNames,
        measurementNames = measurementNames,
        createTime = Instant.ofEpochMilli(Timestamps.toMillis(basicReport.createTime)),
      )
    } finally {
      readContext.close()
    }
  }
}

/** Reads all matching entries with one server-side Cloud Logging filter. */
internal class GoogleCloudReportTraceLogReader(private val logging: Logging) :
  ReportTraceLogReader {
  override fun read(
    correlationValues: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): List<ReportTraceLogEntry> {
    require(correlationValues.isNotEmpty()) { "At least one correlation value is required" }
    val filter = ReportTraceOutput.buildLogFilter(correlationValues, startTime, endTime)
    return logging
      .listLogEntries(
        EntryListOption.filter(filter),
        EntryListOption.pageSize(limit),
        EntryListOption.sortOrder(SortingField.TIMESTAMP, SortingOrder.ASCENDING),
      )
      .iterateAll()
      .take(limit)
      .map { it.toReportTraceLogEntry() }
      .sortedBy { it.timestamp }
  }

  private fun LogEntry.toReportTraceLogEntry(): ReportTraceLogEntry {
    val resourceLabels = resource?.labels.orEmpty()
    val service =
      listOf("service_name", "container_name", "job_name", "function_name").firstNotNullOfOrNull {
        resourceLabels[it]
      } ?: resource?.type ?: logName.substringAfterLast('/')
    return ReportTraceLogEntry(
      timestamp = instantTimestamp ?: Instant.EPOCH,
      service = service,
      severity = severity.name,
      trace = trace?.takeIf(String::isNotEmpty),
      message = getPayload<Payload<*>>()?.toString() ?: "",
    )
  }
}

/** Reads complete spans from the Cloud Trace v1 API using Application Default Credentials. */
internal class GoogleCloudReportTraceSpanReader(
  private val credentials: GoogleCredentials,
  private val httpClient: HttpClient,
) : ReportTraceSpanReader {
  constructor() :
    this(
      GoogleCredentials.getApplicationDefault().createScoped(TRACE_READ_SCOPE),
      HttpClient.newHttpClient(),
    )

  override fun read(
    project: String,
    reportName: String,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): List<ReportTraceLogEntry> {
    credentials.refreshIfExpired()
    val traceAttribute =
      if (reportName.contains("/basicReports/")) {
        BASIC_REPORT_TRACE_ATTRIBUTE
      } else {
        REPORT_TRACE_ATTRIBUTE
      }
    val filter = "+label:$traceAttribute:\"$reportName\""
    val query =
      mapOf(
          "view" to "COMPLETE",
          "pageSize" to limit.coerceAtMost(MAX_TRACE_PAGE_SIZE).toString(),
          "startTime" to startTime.toString(),
          "endTime" to endTime.toString(),
          "filter" to filter,
        )
        .entries
        .joinToString("&") { (key, value) -> "$key=${urlEncode(value)}" }
    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("https://cloudtrace.googleapis.com/v1/projects/$project/traces?$query"))
        .header("Authorization", "Bearer ${checkNotNull(credentials.accessToken).tokenValue}")
        .GET()
        .build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    check(response.statusCode() in 200..299) {
      "Cloud Trace API returned HTTP ${response.statusCode()}: ${response.body().take(1000)}"
    }
    val root = JsonParser.parseString(response.body()).asJsonObject
    val traces = root.getAsJsonArray("traces") ?: return emptyList()
    return traces
      .flatMap { traceElement ->
        val trace = traceElement.asJsonObject
        val traceId = trace.requiredString("traceId")
        val spans = trace.getAsJsonArray("spans") ?: return@flatMap emptyList()
        spans.map { spanElement -> spanElement.asJsonObject.toTraceEntry(project, traceId) }
      }
      .sortedBy { it.timestamp }
      .take(limit)
  }

  private fun JsonObject.toTraceEntry(project: String, traceId: String): ReportTraceLogEntry {
    val labels = getAsJsonObject("labels")
    val service =
      labels?.optionalString("g.co/agent/name")
        ?: labels?.optionalString("service.name")
        ?: labels?.optionalString("/http/host")
        ?: "unknown-service"
    val endTimeValue = optionalString("endTime")
    val endTime = if (endTimeValue == null) null else Instant.parse(endTimeValue)
    val startTime = Instant.parse(requiredString("startTime"))
    val duration = if (endTime == null) null else Duration.between(startTime, endTime).toMillis()
    val details = buildString {
      append("span ").append(requiredString("name"))
      if (duration != null) {
        append(" duration_ms=").append(duration)
      }
      val error = labels?.optionalString("error")
      if (error != null) {
        append(" error=").append(error)
      }
    }
    return ReportTraceLogEntry(
      timestamp = startTime,
      service = service,
      severity = "TRACE",
      trace = "projects/$project/traces/$traceId",
      message = details,
    )
  }

  private fun JsonObject.requiredString(name: String): String = get(name).asString

  private fun JsonObject.optionalString(name: String): String? =
    get(name)?.takeUnless { it.isJsonNull }?.asString

  companion object {
    private const val TRACE_READ_SCOPE = "https://www.googleapis.com/auth/trace.readonly"
    private const val BASIC_REPORT_TRACE_ATTRIBUTE = "xmm.basic_report.name"
    private const val REPORT_TRACE_ATTRIBUTE = "xmm.report.name"
    private const val MAX_TRACE_PAGE_SIZE = 1000

    private fun urlEncode(value: String): String = URLEncoder.encode(value, StandardCharsets.UTF_8)
  }
}

private object ReportTraceOutput {
  fun buildLogFilter(
    correlationValues: Collection<String>,
    startTime: Instant,
    endTime: Instant,
  ): String {
    val identifiers =
      correlationValues.distinct().joinToString(" OR ") { value ->
        val escaped = value.replace("\\", "\\\\").replace("\"", "\\\"")
        "textPayload:\"$escaped\" OR jsonPayload.message:\"$escaped\" OR " +
          "jsonPayload.\"xmm.basic_report.name\"=\"$escaped\" OR " +
          "jsonPayload.\"xmm.report.name\"=\"$escaped\" OR " +
          "jsonPayload.attributes.\"xmm.basic_report.name\"=\"$escaped\" OR " +
          "jsonPayload.attributes.\"xmm.report.name\"=\"$escaped\" OR " +
          "jsonPayload.\"edpa.report_id\"=\"$escaped\" OR " +
          "jsonPayload.\"edpa.results_fulfiller.report_id\"=\"$escaped\""
      }
    return "timestamp>=\"$startTime\" AND timestamp<=\"$endTime\" AND ($identifiers)"
  }

  fun render(context: ReportTraceContext, entries: List<ReportTraceLogEntry>): String =
    buildString {
      appendLine("Report execution trace")
      val basicReportName = context.basicReportName
      if (basicReportName != null) {
        append("BasicReport: ").append(basicReportName)
        val basicReportState = context.basicReportState
        if (basicReportState != null) {
          append(" [").append(basicReportState).append(']')
        }
        appendLine()
      }
      appendLine("Report: ${context.reportName}")
      appendLine("Metrics: ${context.metricNames.size}")
      appendLine("Measurements: ${context.measurementNames.size}")
      appendLine()
      if (entries.isEmpty()) {
        appendLine("No matching log entries were found in the selected time range.")
        return@buildString
      }
      for (entry in entries.sortedBy { it.timestamp }) {
        append(entry.timestamp)
          .append("  ")
          .append(entry.severity.padEnd(7))
          .append("  [")
          .append(entry.service)
          .append("] ")
          .append(entry.message.replace('\n', ' '))
        val trace = entry.trace
        if (trace != null) {
          append(" (trace=").append(trace.substringAfterLast('/')).append(')')
        }
        appendLine()
      }
    }
}

@CommandLine.Command(
  name = "report-trace",
  description = ["Prints a chronological Cloud Trace and Logging timeline for a report."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
internal class ReportTrace(
  private val logReaderFactory: (String) -> ReportTraceLogReader,
  private val spanReaderFactory: () -> ReportTraceSpanReader,
  private val resolverFactory:
    (SpannerDatabaseConnector, PostgresDatabaseClient) -> BasicReportTraceResolver,
) : Runnable {
  @CommandLine.Spec private lateinit var spec: CommandLine.Model.CommandSpec

  @CommandLine.Mixin private lateinit var spannerFlags: SpannerFlags

  @CommandLine.Option(
    names = ["--project"],
    required = true,
    description = ["Google Cloud project."],
  )
  private lateinit var project: String

  @CommandLine.Option(
    names = ["--postgres-database"],
    description = ["Name of the Reporting Postgres database. Required with --basic-report."],
  )
  private var postgresDatabase: String? = null

  @CommandLine.Option(
    names = ["--postgres-cloud-sql-connection-name"],
    description = ["Cloud SQL instance connection name. Required with --basic-report."],
  )
  private var postgresCloudSqlConnectionName: String? = null

  @CommandLine.Option(
    names = ["--postgres-user"],
    description = ["Reporting Postgres user. Required with --basic-report."],
  )
  private var postgresUser: String? = null

  @CommandLine.Option(
    names = ["--statement-timeout"],
    description = ["Postgres statement timeout as an ISO-8601 duration."],
    defaultValue = "PT0S",
  )
  private lateinit var statementTimeout: Duration

  @CommandLine.Option(
    names = ["--basic-report"],
    description = ["BasicReport resource name. Resolves all downstream resource names."],
  )
  private var basicReportName: String? = null

  @CommandLine.Option(
    names = ["--report"],
    description = ["Report resource name for direct mode (does not require reporting databases)."],
  )
  private var reportName: String? = null

  @CommandLine.Option(
    names = ["--start-time"],
    description =
      ["Inclusive RFC 3339 start time. Defaults to five minutes before report creation."],
  )
  private var startTime: String? = null

  @CommandLine.Option(
    names = ["--end-time"],
    description = ["Inclusive RFC 3339 end time."],
    defaultValue = "\${CURRENT-TIME}",
  )
  private lateinit var endTime: String

  @CommandLine.Option(names = ["--limit"], defaultValue = "1000") private lateinit var limit: String

  override fun run() = runBlocking {
    if ((basicReportName == null) == (reportName == null)) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "Exactly one of --basic-report or --report must be specified",
      )
    }
    val entryLimit = limit.toIntOrNull()
    if (entryLimit == null || entryLimit !in 1..MAX_ENTRIES) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "--limit must be between 1 and $MAX_ENTRIES",
      )
    }

    val context =
      if (basicReportName != null) {
        val basicReportKey =
          BasicReportKey.fromName(checkNotNull(basicReportName))
            ?: throw CommandLine.ParameterException(
              spec.commandLine(),
              "Invalid --basic-report resource name: $basicReportName",
            )
        validateDatabaseFlags()
        val postgresClient =
          PostgresDatabaseClient.fromConnectionFactory(buildPostgresConnectionFactory())
        spannerFlags.usingSpanner { spanner ->
          resolverFactory(spanner, postgresClient).resolve(basicReportKey)
        }
      } else {
        val directReportName = checkNotNull(reportName)
        if (ReportKey.fromName(directReportName) == null) {
          throw CommandLine.ParameterException(
            spec.commandLine(),
            "Invalid --report resource name: $directReportName",
          )
        }
        ReportTraceContext(
          basicReportName = null,
          basicReportState = null,
          reportName = directReportName,
          metricNames = emptyList(),
          measurementNames = emptyList(),
          createTime = null,
        )
      }

    val parsedEndTime = parseTime("--end-time", endTime)
    val parsedStartTime =
      startTime?.let { parseTime("--start-time", it) }
        ?: context.createTime?.minus(DEFAULT_LEAD_TIME)
        ?: parsedEndTime.minus(DEFAULT_LOOKBACK)
    if (parsedStartTime.isAfter(parsedEndTime)) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "--start-time must not be after --end-time",
      )
    }
    val spanEntries =
      try {
        spanReaderFactory()
          .read(
            project,
            context.basicReportName ?: context.reportName,
            parsedStartTime,
            parsedEndTime,
            entryLimit,
          )
      } catch (e: Exception) {
        spec.commandLine().err.println("Warning: unable to read Cloud Trace: ${e.message}")
        emptyList()
      }
    val logEntries =
      try {
        logReaderFactory(project)
          .read(context.correlationValues, parsedStartTime, parsedEndTime, entryLimit)
      } catch (e: Exception) {
        spec.commandLine().err.println("Warning: unable to read Cloud Logging: ${e.message}")
        emptyList()
      }
    val entries = (spanEntries + logEntries).sortedBy { it.timestamp }.take(entryLimit)
    spec.commandLine().out.print(ReportTraceOutput.render(context, entries))
  }

  private fun validateDatabaseFlags() {
    val missing = buildList {
      if (runCatching { spannerFlags.projectName }.isFailure) add("--spanner-project")
      if (runCatching { spannerFlags.instanceName }.isFailure) add("--spanner-instance")
      if (runCatching { spannerFlags.databaseName }.isFailure) add("--spanner-database")
      if (postgresDatabase == null) add("--postgres-database")
      if (postgresCloudSqlConnectionName == null) add("--postgres-cloud-sql-connection-name")
      if (postgresUser == null) add("--postgres-user")
    }
    if (missing.isNotEmpty()) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "${missing.joinToString()} required with --basic-report",
      )
    }
  }

  private fun buildPostgresConnectionFactory(): ConnectionFactory {
    return ConnectionFactories.get(
      ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, "gcp")
        .option(ConnectionFactoryOptions.PROTOCOL, "postgresql")
        .option(ConnectionFactoryOptions.USER, checkNotNull(postgresUser))
        .option(ConnectionFactoryOptions.PASSWORD, "UNUSED")
        .option(ConnectionFactoryOptions.DATABASE, checkNotNull(postgresDatabase))
        .option(ConnectionFactoryOptions.HOST, checkNotNull(postgresCloudSqlConnectionName))
        .option(GcpConnectionFactoryProvider.ENABLE_IAM_AUTH, true)
        .option(ConnectionFactoryOptions.STATEMENT_TIMEOUT, statementTimeout)
        .build()
    )
  }

  private fun parseTime(option: String, value: String): Instant {
    return try {
      Instant.parse(value)
    } catch (e: DateTimeParseException) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "Invalid $option '$value': expected an RFC 3339 time",
      )
    }
  }

  companion object {
    private val DEFAULT_LEAD_TIME: Duration = Duration.ofMinutes(5)
    private val DEFAULT_LOOKBACK: Duration = Duration.ofDays(7)
    private const val MAX_ENTRIES = 1000
  }
}

internal class ReportTraceDependencies(
  val logReaderFactory: (String) -> ReportTraceLogReader,
  val spanReaderFactory: () -> ReportTraceSpanReader,
  val resolverFactory:
    (SpannerDatabaseConnector, PostgresDatabaseClient) -> BasicReportTraceResolver,
  val output: java.io.PrintWriter,
  val error: java.io.PrintWriter,
)

internal fun main(args: Array<String>, dependencies: ReportTraceDependencies): Int {
  return CommandLine(
      ReportTrace(
        dependencies.logReaderFactory,
        dependencies.spanReaderFactory,
        dependencies.resolverFactory,
      )
    )
    .setOut(dependencies.output)
    .setErr(dependencies.error)
    .execute(*args)
}

fun main(args: Array<String>) =
  commandLineMain(
    ReportTrace(
      logReaderFactory = { project ->
        GoogleCloudReportTraceLogReader(
          LoggingOptions.newBuilder().setProjectId(project).build().service
        )
      },
      spanReaderFactory = { GoogleCloudReportTraceSpanReader() },
      resolverFactory = { spanner, postgres ->
        DatabaseBasicReportTraceResolver(spanner.databaseClient, postgres)
      },
    ),
    args,
  )
