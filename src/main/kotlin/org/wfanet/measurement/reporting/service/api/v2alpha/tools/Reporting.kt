/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha.tools

import com.google.type.DayOfWeek
import com.google.type.date
import com.google.type.dateTime
import com.google.type.interval
import com.google.type.timeZone
import io.grpc.ManagedChannel
import io.grpc.StatusException
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.common.DurationFormat
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportKt.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.createMetricCalculationSpecRequest
import org.wfanet.measurement.reporting.v2alpha.createReportRequest
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.getMetricCalculationSpecRequest
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.listMetricCalculationSpecsRequest
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsRequest
import org.wfanet.measurement.reporting.v2alpha.listReportsRequest
import org.wfanet.measurement.reporting.v2alpha.metricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.timeIntervals
import picocli.CommandLine

private class ReportingApiFlags {
  @CommandLine.Option(
    names = ["--reporting-server-api-target"],
    description = ["gRPC target (authority) of the reporting server's public API"],
    required = true,
  )
  lateinit var apiTarget: String
    private set

  @CommandLine.Option(
    names = ["--reporting-server-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the reporting server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --reporting-server-api-target.",
      ],
    required = false,
  )
  var apiCertHost: String? = null
    private set
}

private class PageParams {
  @CommandLine.Option(
    names = ["--page-size"],
    description = ["The maximum number of items to return. The maximum value is 1000"],
    required = false,
  )
  var pageSize: Int = 1000
    private set

  @CommandLine.Option(
    names = ["--page-token"],
    description = ["Page token from a previous list call to retrieve the next page"],
    defaultValue = "",
    required = false,
  )
  lateinit var pageToken: String
    private set
}

@CommandLine.Command(name = "create", description = ["Creates a reporting set"])
class CreateReportingSetCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: ReportingSetsCommand

  @CommandLine.Option(
    names = ["--parent"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  class ReportingSetType {
    @CommandLine.Option(
      names = ["--cmms-event-group"],
      description = ["List of CMMS EventGroup resource names"],
      required = false,
    )
    var cmmsEventGroups: List<String>? = null

    @CommandLine.Option(
      names = ["--set-expression"],
      description = ["SetExpression protobuf messages in text format"],
      required = false,
    )
    var textFormatSetExpression: String? = null
  }

  @CommandLine.ArgGroup(exclusive = true, multiplicity = "1", heading = "Reporting Set Type\n")
  private lateinit var type: ReportingSetType

  @CommandLine.Option(
    names = ["--filter"],
    description = ["CEL filter predicate that applies to all `event_groups`"],
    required = false,
    defaultValue = "",
  )
  private lateinit var filterExpression: String

  @CommandLine.Option(
    names = ["--display-name"],
    description = ["Human-readable name for display purposes"],
    required = false,
    defaultValue = "",
  )
  private lateinit var displayNameInput: String

  @CommandLine.Option(
    names = ["--id"],
    description = ["Resource ID of the Reporting Set"],
    required = true,
    defaultValue = "",
  )
  private lateinit var reportingSetId: String

  override fun run() {
    val request = createReportingSetRequest {
      parent = measurementConsumerName
      reportingSet = reportingSet {
        if (type.cmmsEventGroups != null && type.cmmsEventGroups!!.isNotEmpty()) {
          primitive =
            ReportingSetKt.primitive { type.cmmsEventGroups!!.forEach { cmmsEventGroups += it } }
        } else if (type.textFormatSetExpression != null) {
          composite =
            ReportingSetKt.composite {
              expression =
                parseTextProto(
                  type.textFormatSetExpression!!.reader(),
                  ReportingSet.SetExpression.getDefaultInstance(),
                )
            }
        }
        filter = filterExpression
        displayName = displayNameInput
      }
      reportingSetId = this@CreateReportingSetCommand.reportingSetId
    }
    val reportingSet =
      runBlocking(Dispatchers.IO) { parent.reportingSetStub.createReportingSet(request) }
    println(reportingSet)
  }
}

@CommandLine.Command(name = "list", description = ["List reporting sets"])
class ListReportingSetsCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: ReportingSetsCommand

  @CommandLine.Option(
    names = ["--parent"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  @CommandLine.Mixin private lateinit var pageParams: PageParams

  override fun run() {
    val request = listReportingSetsRequest {
      parent = measurementConsumerName
      pageSize = pageParams.pageSize
      pageToken = pageParams.pageToken
    }

    val response =
      runBlocking(Dispatchers.IO) { parent.reportingSetStub.listReportingSets(request) }

    println(response)
  }
}

@CommandLine.Command(
  name = "reporting-sets",
  sortOptions = false,
  subcommands =
    [
      CommandLine.HelpCommand::class,
      CreateReportingSetCommand::class,
      ListReportingSetsCommand::class,
    ],
)
class ReportingSetsCommand : Runnable {
  @CommandLine.ParentCommand lateinit var parent: Reporting
  val reportingSetStub: ReportingSetsCoroutineStub by lazy {
    ReportingSetsCoroutineStub(parent.channel)
  }

  override fun run() {}
}

@CommandLine.Command(
  name = "create-ui-report",
  description = ["Create a report viewable by the Reporting UI"],
)
class CreateUiReportCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: ReportsCommand

  @CommandLine.Option(
    names = ["--request-id"],
    description = ["Request ID for creation of Report"],
    required = false,
    defaultValue = "",
  )
  private lateinit var requestId: String

  @CommandLine.Option(
    names = ["--parent"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  class ReportParams {
    @CommandLine.Option(
      names = ["--id"],
      description = ["Resource ID of the Report"],
      required = true,
      defaultValue = "",
    )
    lateinit var id: String
      private set

    @CommandLine.Option(
      names = ["--display-name"],
      description = ["Display Name for the report"],
      required = true,
      defaultValue = "",
    )
    lateinit var displayName: String
      private set
  }

  @CommandLine.ArgGroup(
    exclusive = false,
    multiplicity = "1",
    heading = "Create Report request configuration\n",
  )
  private lateinit var reportParams: ReportParams

  class ReportingSetParams {
    @CommandLine.Option(
      names = ["--cmms-event-group"],
      description =
        ["EventGroup resource name from the CMMS API. This can be specified multiple times"],
      required = true,
    )
    lateinit var eventGroupNames: List<String>
      private set

    @CommandLine.Option(
      names = ["--reporting-set-id"],
      description = ["Name of the ReportingSet used with the EDP"],
      required = true,
    )
    lateinit var reportingSetName: String
      private set

    @CommandLine.Option(
      names = ["--reporting-set-display-name"],
      description = ["Human-readable name for display purposes"],
      required = false,
    )
    lateinit var reportingSetDisplayName: String
      private set
  }

  @CommandLine.ArgGroup(
    exclusive = false,
    multiplicity = "2..*",
    heading = "Primitive Reporting Set request configuration\n",
  )
  private lateinit var edps: List<ReportingSetParams>

  class MetricFrequencySpecInput {
    @CommandLine.Option(
      names = ["--daily-frequency"],
      description = ["Whether to use daily frequency"],
    )
    var daily: Boolean = false
      private set

    @CommandLine.Option(
      names = ["--day-of-week"],
      description = ["Day of the week for weekly frequency."],
    )
    var dayOfWeek: DayOfWeek = DayOfWeek.DAY_OF_WEEK_UNSPECIFIED
      private set

    @CommandLine.Option(
      names = ["--day-of-month"],
      description =
        [
          """
          Day of the month for monthly frequency. Represented by a number between 1 and 31, inclusive.
          """
        ],
    )
    var dayOfMonth: Int = 0
      private set
  }

  @CommandLine.ArgGroup(
    exclusive = true,
    multiplicity = "1",
    heading = "Metric frequency specification\n",
  )
  private lateinit var metricFrequencySpecInput: MetricFrequencySpecInput

  class ReportingIntervalInput {
    @CommandLine.Option(
      names = ["--report-start"],
      description = ["Start of the report in yyyy-MM-ddTHH:mm:ss"],
      required = true,
    )
    lateinit var reportingIntervalReportStartTime: LocalDateTime
      private set

    @CommandLine.Option(
      names = ["--report-end"],
      description = ["End of the report in yyyy-mm-dd"],
      required = true,
    )
    lateinit var reportingIntervalReportEnd: LocalDate
      private set

    @CommandLine.Option(
      names = ["--report-time-zone"],
      description =
        ["IANA Time zone. If unspecified, it will use the user's default system time zone."],
      required = false,
    )
    var timeZone: String = ZoneId.systemDefault().toString()
      private set
  }

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "1", heading = "Time intervals\n")
  private lateinit var interval: ReportingIntervalInput

  class Grouping {
    @CommandLine.Option(
      names = ["--grouping"],
      description = ["Boolean to indicate the start of a new grouping list."],
      required = true,
    )
    var grouping: Boolean = false
      private set

    @CommandLine.Option(
      names = ["--predicate"],
      description = ["A list of predicates for this grouping."],
      required = true,
    )
    lateinit var predicates: List<String>
      private set
  }

  @CommandLine.ArgGroup(
    exclusive = false,
    multiplicity = "0..*",
    heading = "Grouping Specification\n",
  )
  private lateinit var groupings: List<Grouping>

  private fun createPrimitiveReportingSet(
    edp: ReportingSetParams,
    measurementConsumerName: String,
  ): ReportingSet {
    val request = createReportingSetRequest {
      parent = measurementConsumerName
      reportingSet = reportingSet {
        name = edp.reportingSetName
        displayName = edp.reportingSetDisplayName
        tags.put("ui.halo-cmm.org/reporting_set_type", "individual")
        primitive =
          ReportingSetKt.primitive {
            for (eventGroupName in edp.eventGroupNames) cmmsEventGroups += eventGroupName
          }
      }
    }

    return runBlocking(Dispatchers.IO) { parent.reportingSetStub.createReportingSet(request) }
  }

  // Creates the nested unions for the provided EDPs.
  private fun createUnionExpression(edpNames: List<String>): ReportingSet.SetExpression {
    if (edpNames.size == 2) {
      return ReportingSetKt.setExpression {
        operation = ReportingSet.SetExpression.Operation.UNION
        lhs = ReportingSetKt.SetExpressionKt.operand { reportingSet = edpNames[0] }
        rhs = ReportingSetKt.SetExpressionKt.operand { reportingSet = edpNames[1] }
      }
    } else {
      return ReportingSetKt.setExpression {
        operation = ReportingSet.SetExpression.Operation.UNION
        lhs = ReportingSetKt.SetExpressionKt.operand { reportingSet = edpNames[0] }
        rhs =
          ReportingSetKt.SetExpressionKt.operand {
            expression = createUnionExpression(edpNames.subList(1, edpNames.size))
          }
      }
    }
  }

  private fun createUnionReportingSet(
    edps: List<ReportingSetParams>,
    primitiveRs: List<ReportingSet>,
    measurementConsumerName: String,
  ): ReportingSet {
    val edpNames = edps.map { it.reportingSetName }
    val edpDisplayNames = edps.map { it.reportingSetDisplayName }
    val edpFullNames = primitiveRs.map { it.name }
    val unionName = "union-" + edpNames.joinToString(separator = "-")
    val unionDisplayName = "Union (${edpDisplayNames.joinToString()})"

    val rsRequest = createReportingSetRequest {
      parent = measurementConsumerName
      reportingSet = reportingSet {
        name = unionName
        displayName = unionDisplayName
        tags.put("ui.halo-cmm.org/reporting_set_type", "union")
        composite = ReportingSetKt.composite { expression = createUnionExpression(edpFullNames) }
      }
    }

    return runBlocking(Dispatchers.IO) { parent.reportingSetStub.createReportingSet(rsRequest) }
  }

  private fun createUniqueReportingSet(
    edp: ReportingSetParams,
    primitiveReportingSets: ArrayList<ReportingSet>,
    unionRsName: String,
    measurementConsumerName: String,
  ): ReportingSet {
    // Depending on the name, a match may find other matches. eg. RS name = XYZ and parent name =
    // measurementConsumers/ABCXYZ
    val complement =
      primitiveReportingSets.filter { !it.name.contains("/reportingSets/" + edp.reportingSetName) }
    val primitivePairRs =
      primitiveReportingSets.filter { it.name.contains("/reportingSets/" + edp.reportingSetName) }
    // Since these reporting sets were created from the provided names, exactly one match should be
    // found.
    val primitivePairRsName = primitivePairRs[0].name
    val request = createReportingSetRequest {
      parent = measurementConsumerName
      reportingSet = reportingSet {
        name = edp.reportingSetName + "-unique"
        displayName = edp.reportingSetDisplayName + " Unique"
        tags.put("ui.halo-cmm.org/reporting_set_type", "unique")
        tags.put("ui.halo-cmm.org/reporting_set_id", primitivePairRsName)
        composite =
          ReportingSetKt.composite {
            expression =
              ReportingSetKt.setExpression {
                operation = ReportingSet.SetExpression.Operation.UNION
                lhs = ReportingSetKt.SetExpressionKt.operand { reportingSet = unionRsName }
                rhs =
                  ReportingSetKt.SetExpressionKt.operand {
                    expression =
                      ReportingSetKt.setExpression {
                        operation = ReportingSet.SetExpression.Operation.UNION
                        lhs =
                          ReportingSetKt.SetExpressionKt.operand {
                            reportingSet = complement.elementAt(0).name
                          }
                        rhs =
                          ReportingSetKt.SetExpressionKt.operand {
                            reportingSet = complement.elementAt(1).name
                          }
                      }
                  }
              }
          }
      }
    }

    return runBlocking(Dispatchers.IO) { parent.reportingSetStub.createReportingSet(request) }
  }

  private fun getRandomString(length: Int): String {
    val allowedChars = ('A'..'Z') + ('a'..'z') + ('0'..'9')
    return (1..length).map { allowedChars.random() }.joinToString("")
  }

  private fun createPrimitiveMetricSpecs(
    frequencySpec: MetricFrequencySpecInput,
    measurementConsumerName: String,
    groupings: List<Grouping>,
  ): MetricCalculationSpec {
    val baseName = "basic-metric-spec"
    val specs =
      mutableListOf<MetricSpec>(
        metricSpec {
          reachAndFrequency =
            MetricSpecKt.reachAndFrequencyParams {
              reachPrivacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon = 6.82E-4
                  delta = 1.0E-15
                }
              frequencyPrivacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon = 0.003888
                  delta = 1.0E-15
                }
              maximumFrequency = 10
            }
          vidSamplingInterval = MetricSpecKt.vidSamplingInterval { width = 0.27f }
        },
        metricSpec {
          impressionCount =
            MetricSpecKt.impressionCountParams {
              privacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon = 0.007167
                  delta = 1.0E-15
                }
              maximumFrequencyPerUser = 15
            }
          vidSamplingInterval = MetricSpecKt.vidSamplingInterval { width = 1.0f }
        },
      )

    return this.createMetricSpecs(
      baseName,
      measurementConsumerName,
      specs,
      frequencySpec,
      groupings,
    )
  }

  private fun createCompositeMetricSpecs(
    frequencySpec: MetricFrequencySpecInput,
    measurementConsumerName: String,
    groupings: List<Grouping>,
  ): MetricCalculationSpec {
    val baseName = "other-metric-spec"
    val specs =
      mutableListOf<MetricSpec>(
        metricSpec {
          reach =
            MetricSpecKt.reachParams {
              privacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon = 6.82E-4
                  delta = 1.0E-15
                }
            }
          vidSamplingInterval = MetricSpecKt.vidSamplingInterval { width = 0.01f }
        },
        metricSpec {
          impressionCount =
            MetricSpecKt.impressionCountParams {
              privacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon = 0.007167
                  delta = 1.0E-15
                }
              maximumFrequencyPerUser = 15
            }
          vidSamplingInterval = MetricSpecKt.vidSamplingInterval { width = 1.0f }
        },
      )

    return this.createMetricSpecs(
      baseName,
      measurementConsumerName,
      specs,
      frequencySpec,
      groupings,
    )
  }

  private fun createMetricSpecs(
    name: String,
    measurementConsumerName: String,
    specs: List<MetricSpec>,
    frequencySpec: MetricFrequencySpecInput,
    groupings: List<Grouping>,
  ): MetricCalculationSpec {
    // Add random string to the end of the name to help with uniqueness.
    // This only helps so still need to make sure it's actually unique.
    var retry = true
    var randomString: String
    var newName: String = ""
    while (retry) {
      randomString = getRandomString(6)
      newName = name + "-" + randomString
      val getMetricSpecRequest = getMetricCalculationSpecRequest { this.name = newName }
      try {
        runBlocking(Dispatchers.IO) {
          parent.metricCalculationSpecStub.getMetricCalculationSpec(getMetricSpecRequest)
        }
      } catch (e: StatusException) {
        retry = false
      }
    }

    val msRequestUnique = createMetricCalculationSpecRequest {
      parent = measurementConsumerName
      metricCalculationSpec = metricCalculationSpec {
        this.name = newName
        metricFrequencySpec =
          MetricCalculationSpecKt.metricFrequencySpec {
            if (frequencySpec.daily) {
              daily = MetricCalculationSpec.MetricFrequencySpec.Daily.getDefaultInstance()
            } else if (frequencySpec.dayOfWeek != DayOfWeek.DAY_OF_WEEK_UNSPECIFIED) {
              weekly =
                MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                  this.dayOfWeek = frequencySpec.dayOfWeek
                }
            } else if (frequencySpec.dayOfMonth > 0) {
              monthly =
                MetricCalculationSpecKt.MetricFrequencySpecKt.monthly {
                  dayOfMonth = frequencySpec.dayOfMonth
                }
            }
          }

        for (spec in specs) {
          metricSpecs += spec
        }
        for (grouping in groupings) {
          this.groupings += MetricCalculationSpecKt.grouping { predicates += grouping.predicates }
        }
      }
    }

    return runBlocking(Dispatchers.IO) {
      parent.metricCalculationSpecStub.createMetricCalculationSpec(msRequestUnique)
    }
  }

  // Steps:
  //  1. Create Reporting Sets
  //    a. One for each desired primitive reporting set
  //    b. One for union of all reporting sets
  //    c. One for each unique reporting set (eg. unique A = ABC - BC)
  //  2. Create Metric Calculation Specs
  //    a. One for impressions and reach and frequency cumulative = true (for 1a and 1b)
  //    b. One for impressions and reach cumulative = true (for 1c)
  //    c. Repeat 2a-b with cumulative = false
  //  3. Create the Report with the right RS-CMS mappings
  override fun run() {
    // TODO Get from inputs
    //  - unique post-fix
    //  - union name/displayname

    // 1a. create reporting set primitives
    val primitiveReportingSets = ArrayList<ReportingSet>()
    for (edp in edps) {
      val reportingSet = createPrimitiveReportingSet(edp, measurementConsumerName)
      primitiveReportingSets.add(reportingSet)
    }

    // 1b. create composite reporting sets (union)
    val unionReportingSet =
      createUnionReportingSet(edps, primitiveReportingSets, measurementConsumerName)

    // 1c. create unique reporting sets (union - complement)
    val uniqueReportingSets = ArrayList<ReportingSet>()
    for (edp in edps) {
      val reportingSet =
        createUniqueReportingSet(
          edp,
          primitiveReportingSets,
          unionReportingSet.name,
          measurementConsumerName,
        )
      uniqueReportingSets.add(reportingSet)
    }

    // 2a. create impressions and reach and frequency metric specs
    val metricCalculationSpecAll =
      createPrimitiveMetricSpecs(metricFrequencySpecInput, measurementConsumerName, groupings)

    // 2b. create impressions and reach metric specs
    val metricCalculationSpecUnique =
      createCompositeMetricSpecs(metricFrequencySpecInput, measurementConsumerName, groupings)

    // 3. create the report
    val request = createReportRequest {
      parent = measurementConsumerName
      reportId = this@CreateUiReportCommand.reportParams.id
      requestId = this@CreateUiReportCommand.requestId
      report = report {
        name = this@CreateUiReportCommand.reportParams.id
        this.reportingInterval = reportingInterval {
          reportStart = dateTime {
            year = interval.reportingIntervalReportStartTime.year
            month = interval.reportingIntervalReportStartTime.monthValue
            day = interval.reportingIntervalReportStartTime.dayOfMonth
            hours = interval.reportingIntervalReportStartTime.hour
            minutes = interval.reportingIntervalReportStartTime.minute
            seconds = interval.reportingIntervalReportStartTime.second
            this.timeZone = timeZone { id = interval.timeZone }
          }
          reportEnd = date {
            year = interval.reportingIntervalReportEnd.year
            month = interval.reportingIntervalReportEnd.monthValue
            day = interval.reportingIntervalReportEnd.dayOfMonth
          }
        }
        tags.put("ui.halo-cmm.org", "true")
        tags.put("ui.halo-cmm.org/display_name", reportParams.displayName)

        // Set primitive reporting sets
        for (primitiveRS in primitiveReportingSets) {
          reportingMetricEntries +=
            ReportKt.reportingMetricEntry {
              key = primitiveRS.name
              value =
                ReportKt.reportingMetricCalculationSpec {
                  metricCalculationSpecs += metricCalculationSpecAll.name
                }
            }
        }
        // Set union reporting set
        reportingMetricEntries +=
          ReportKt.reportingMetricEntry {
            key = unionReportingSet.name
            value =
              ReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecs += metricCalculationSpecAll.name
              }
          }
        // set unique reporting sets
        for (uniqueRS in uniqueReportingSets) {
          reportingMetricEntries +=
            ReportKt.reportingMetricEntry {
              key = uniqueRS.name
              value =
                ReportKt.reportingMetricCalculationSpec {
                  metricCalculationSpecs += metricCalculationSpecUnique.name
                }
            }
        }
      }
    }
    val report = runBlocking(Dispatchers.IO) { parent.reportsStub.createReport(request) }

    println(report)
  }
}

@CommandLine.Command(name = "create", description = ["Create a report"])
class CreateReportCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: ReportsCommand

  @CommandLine.Option(
    names = ["--parent"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  @CommandLine.Option(
    names = ["--reporting-metric-entry"],
    description = ["ReportingMetricEntry protobuf messages in text format"],
    required = true,
  )
  lateinit var textFormatReportingMetricEntries: List<String>

  class TimeInput {
    class TimeIntervalInput {
      @CommandLine.Option(
        names = ["--interval-start-time"],
        description = ["Start of time interval in ISO 8601 format of UTC"],
        required = true,
      )
      lateinit var intervalStartTime: Instant
        private set

      @CommandLine.Option(
        names = ["--interval-end-time"],
        description = ["End of time interval in ISO 8601 format of UTC"],
        required = true,
      )
      lateinit var intervalEndTime: Instant
        private set
    }

    class ReportingIntervalInput {
      @CommandLine.Option(
        names = ["--reporting-interval-report-start-time"],
        description = ["Start of the report in yyyy-MM-ddTHH:mm:ss"],
        required = true,
      )
      lateinit var reportingIntervalReportStartTime: LocalDateTime
        private set

      class TimeOffset {
        @CommandLine.Option(
          names = ["--reporting-interval-report-start-utc-offset"],
          description = ["UTC offset in ISO-8601 format of PnDTnHnMn"],
          required = false,
        )
        var utcOffset: Duration? = null
          private set

        @CommandLine.Option(
          names = ["--reporting-interval-report-start-time-zone"],
          description = ["IANA Time zone"],
          required = false,
        )
        var timeZone: String? = null
          private set
      }

      @CommandLine.ArgGroup(
        exclusive = true,
        multiplicity = "1",
        heading = "UTC offset or time zone\n",
      )
      lateinit var reportingIntervalReportStartTimeOffset: TimeOffset

      @CommandLine.Option(
        names = ["--reporting-interval-report-end"],
        description = ["End of the report in yyyy-mm-dd"],
        required = true,
      )
      lateinit var reportingIntervalReportEnd: LocalDate
        private set
    }

    @CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*", heading = "Time intervals\n")
    var timeIntervals: List<TimeIntervalInput>? = null
      private set

    @CommandLine.ArgGroup(
      exclusive = false,
      multiplicity = "1",
      heading = "Reporting interval specification\n",
    )
    var reportingIntervalInput: ReportingIntervalInput? = null
      private set
  }

  @CommandLine.ArgGroup(
    exclusive = true,
    multiplicity = "1",
    heading = "Time interval or reporting interval\n",
  )
  private lateinit var timeInput: TimeInput

  @CommandLine.Option(
    names = ["--id"],
    description = ["Resource ID of the Report"],
    required = true,
    defaultValue = "",
  )
  private lateinit var reportId: String

  @CommandLine.Option(
    names = ["--request-id"],
    description = ["Request ID for creation of Report"],
    required = false,
    defaultValue = "",
  )
  private lateinit var requestId: String

  override fun run() {
    val request = createReportRequest {
      parent = measurementConsumerName
      report = report {
        for (textFormatReportingMetricEntry in textFormatReportingMetricEntries) {
          reportingMetricEntries +=
            parseTextProto(
              textFormatReportingMetricEntry.reader(),
              Report.ReportingMetricEntry.getDefaultInstance(),
            )
        }

        // Either timeIntervals or periodicTimeIntervalInput is set.
        if (timeInput.timeIntervals != null) {
          val intervals = checkNotNull(timeInput.timeIntervals)
          timeIntervals = timeIntervals {
            intervals.forEach {
              timeIntervals += interval {
                startTime = it.intervalStartTime.toProtoTime()
                endTime = it.intervalEndTime.toProtoTime()
              }
            }
          }
        } else {
          val reportingInterval = checkNotNull(timeInput.reportingIntervalInput)
          this.reportingInterval = reportingInterval {
            reportStart = dateTime {
              year = reportingInterval.reportingIntervalReportStartTime.year
              month = reportingInterval.reportingIntervalReportStartTime.monthValue
              day = reportingInterval.reportingIntervalReportStartTime.dayOfMonth
              hours = reportingInterval.reportingIntervalReportStartTime.hour
              minutes = reportingInterval.reportingIntervalReportStartTime.minute
              seconds = reportingInterval.reportingIntervalReportStartTime.second

              if (reportingInterval.reportingIntervalReportStartTimeOffset.utcOffset != null) {
                val utcOffset =
                  checkNotNull(reportingInterval.reportingIntervalReportStartTimeOffset.utcOffset)
                this.utcOffset = utcOffset.toProtoDuration()
              } else {
                val timeZone =
                  checkNotNull(reportingInterval.reportingIntervalReportStartTimeOffset.timeZone)
                this.timeZone = timeZone { id = timeZone }
              }
            }

            reportEnd = date {
              year = reportingInterval.reportingIntervalReportEnd.year
              month = reportingInterval.reportingIntervalReportEnd.monthValue
              day = reportingInterval.reportingIntervalReportEnd.dayOfMonth
            }
          }
        }
      }
      reportId = this@CreateReportCommand.reportId
      requestId = this@CreateReportCommand.requestId
    }
    val report = runBlocking(Dispatchers.IO) { parent.reportsStub.createReport(request) }

    println(report)
  }
}

@CommandLine.Command(name = "list", description = ["List reports"])
class ListReportsCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: ReportsCommand

  @CommandLine.Option(
    names = ["--parent"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  @CommandLine.Mixin private lateinit var pageParams: PageParams

  override fun run() {
    val request = listReportsRequest {
      parent = measurementConsumerName
      pageSize = pageParams.pageSize
      pageToken = pageParams.pageToken
    }

    val response = runBlocking(Dispatchers.IO) { parent.reportsStub.listReports(request) }

    response.reportsList.forEach { println(it.name + " " + it.state.toString()) }
    if (response.nextPageToken.isNotEmpty()) {
      println("nextPageToken: ${response.nextPageToken}")
    }
  }
}

@CommandLine.Command(name = "get", description = ["Get a report"])
class GetReportCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: ReportsCommand

  @CommandLine.Parameters(description = ["API resource name of the Report"])
  private lateinit var reportName: String

  override fun run() {
    val request = getReportRequest { name = reportName }

    val report = runBlocking(Dispatchers.IO) { parent.reportsStub.getReport(request) }
    println(report)
  }
}

@CommandLine.Command(
  name = "reports",
  sortOptions = false,
  subcommands =
    [
      CommandLine.HelpCommand::class,
      CreateReportCommand::class,
      CreateUiReportCommand::class,
      ListReportsCommand::class,
      GetReportCommand::class,
    ],
)
class ReportsCommand : Runnable {
  @CommandLine.ParentCommand lateinit var parent: Reporting

  val reportsStub: ReportsCoroutineStub by lazy { ReportsCoroutineStub(parent.channel) }
  val reportingSetStub: ReportingSetsCoroutineStub by lazy {
    ReportingSetsCoroutineStub(parent.channel)
  }
  val metricCalculationSpecStub: MetricCalculationSpecsCoroutineStub by lazy {
    MetricCalculationSpecsCoroutineStub(parent.channel)
  }

  override fun run() {}
}

@CommandLine.Command(name = "create", description = ["Create a metric calculation spec"])
class CreateMetricCalculationSpecCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: MetricCalculationSpecsCommand

  @CommandLine.Option(
    names = ["--parent"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  @CommandLine.Option(
    names = ["--display-name"],
    description = ["Human-readable name for display purposes"],
    required = false,
    defaultValue = "",
  )
  private lateinit var displayName: String

  @CommandLine.Option(
    names = ["--metric-spec"],
    description = ["MetricSpec protobuf messages in text format"],
    required = true,
  )
  lateinit var textFormatMetricSpecs: List<String>

  @CommandLine.Option(
    names = ["--grouping"],
    description = ["Each grouping is a list of comma-separated predicates"],
    required = false,
  )
  lateinit var groupings: List<String>

  @CommandLine.Option(
    names = ["--filter"],
    description = ["CEL filter predicate that will be conjoined to any Reporting Set filters"],
    required = false,
    defaultValue = "",
  )
  private lateinit var filter: String

  class MetricFrequencySpecInput {
    @CommandLine.Option(
      names = ["--daily-frequency"],
      description = ["Whether to use daily frequency"],
    )
    var daily: Boolean = false
      private set

    @CommandLine.Option(
      names = ["--day-of-the-week"],
      description =
        [
          """
      Day of the week for weekly frequency. Represented by a number between 1 and 7, inclusive,
      where Monday is 1 and Sunday is 7.
      """
        ],
    )
    var dayOfTheWeek: Int = 0
      private set

    @CommandLine.Option(
      names = ["--day-of-the-month"],
      description =
        [
          """
      Day of the month for monthly frequency. Represented by a number between 1 and 31, inclusive.
      """
        ],
    )
    var dayOfTheMonth: Int = 0
      private set
  }

  @CommandLine.ArgGroup(
    exclusive = true,
    multiplicity = "0..1",
    heading = "Metric frequency specification\n",
  )
  private lateinit var metricFrequencySpecInput: MetricFrequencySpecInput

  class TrailingWindowInput {
    @CommandLine.Option(names = ["--day-window-count"], description = ["Size of day window"])
    var dayCount: Int = 0
      private set

    @CommandLine.Option(
      names = ["--week-window-count"],
      description = ["Size of week window"],
      required = false,
    )
    var weekCount: Int = 0
      private set

    @CommandLine.Option(
      names = ["--month-window-count"],
      description = ["Size of month window"],
      required = false,
    )
    var monthCount: Int = 0
      private set
  }

  @CommandLine.ArgGroup(
    exclusive = true,
    multiplicity = "0..1",
    heading = "Trailing window specification\n",
  )
  private lateinit var trailingWindowInput: TrailingWindowInput

  @CommandLine.Option(
    names = ["--id"],
    description = ["Resource ID of the Metric Calculation Spec"],
    required = true,
    defaultValue = "",
  )
  private lateinit var metricCalculationSpecId: String

  override fun run() {
    val request = createMetricCalculationSpecRequest {
      parent = measurementConsumerName
      metricCalculationSpec = metricCalculationSpec {
        displayName = this@CreateMetricCalculationSpecCommand.displayName
        for (textFormatMetricSpec in textFormatMetricSpecs) {
          metricSpecs +=
            parseTextProto(textFormatMetricSpec.reader(), MetricSpec.getDefaultInstance())
        }

        filter = this@CreateMetricCalculationSpecCommand.filter

        for (grouping in this@CreateMetricCalculationSpecCommand.groupings) {
          groupings += MetricCalculationSpecKt.grouping { predicates += grouping.trim().split(',') }
        }

        if (this@CreateMetricCalculationSpecCommand::metricFrequencySpecInput.isInitialized) {
          metricFrequencySpec =
            MetricCalculationSpecKt.metricFrequencySpec {
              if (this@CreateMetricCalculationSpecCommand.metricFrequencySpecInput.daily) {
                daily = MetricCalculationSpec.MetricFrequencySpec.Daily.getDefaultInstance()
              } else if (
                this@CreateMetricCalculationSpecCommand.metricFrequencySpecInput.dayOfTheWeek > 0
              ) {
                weekly =
                  MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                    dayOfWeek =
                      DayOfWeek.forNumber(
                        this@CreateMetricCalculationSpecCommand.metricFrequencySpecInput
                          .dayOfTheWeek
                      )
                  }
              } else if (
                this@CreateMetricCalculationSpecCommand.metricFrequencySpecInput.dayOfTheMonth > 0
              ) {
                monthly =
                  MetricCalculationSpecKt.MetricFrequencySpecKt.monthly {
                    dayOfMonth =
                      this@CreateMetricCalculationSpecCommand.metricFrequencySpecInput.dayOfTheMonth
                  }
              }
            }
        }

        if (this@CreateMetricCalculationSpecCommand::trailingWindowInput.isInitialized) {
          trailingWindow =
            MetricCalculationSpecKt.trailingWindow {
              if (this@CreateMetricCalculationSpecCommand.trailingWindowInput.dayCount > 0) {
                count = this@CreateMetricCalculationSpecCommand.trailingWindowInput.dayCount
                increment = MetricCalculationSpec.TrailingWindow.Increment.DAY
              } else if (
                this@CreateMetricCalculationSpecCommand.trailingWindowInput.weekCount > 0
              ) {
                count = this@CreateMetricCalculationSpecCommand.trailingWindowInput.weekCount
                increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
              } else if (
                this@CreateMetricCalculationSpecCommand.trailingWindowInput.monthCount > 0
              ) {
                count = this@CreateMetricCalculationSpecCommand.trailingWindowInput.monthCount
                increment = MetricCalculationSpec.TrailingWindow.Increment.MONTH
              }
            }
        }
      }

      metricCalculationSpecId = this@CreateMetricCalculationSpecCommand.metricCalculationSpecId
    }
    val metricCalculationSpec =
      runBlocking(Dispatchers.IO) {
        parent.metricCalculationSpecsStub.createMetricCalculationSpec(request)
      }

    println(metricCalculationSpec)
  }
}

@CommandLine.Command(name = "list", description = ["List metric calculation specs"])
class ListMetricCalculationSpecsCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: MetricCalculationSpecsCommand

  @CommandLine.Option(
    names = ["--parent"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  @CommandLine.Mixin private lateinit var pageParams: PageParams

  override fun run() {
    val request = listMetricCalculationSpecsRequest {
      parent = measurementConsumerName
      pageSize = pageParams.pageSize
      pageToken = pageParams.pageToken
    }

    val response =
      runBlocking(Dispatchers.IO) {
        parent.metricCalculationSpecsStub.listMetricCalculationSpecs(request)
      }

    response.metricCalculationSpecsList.forEach { println(it.name) }
    if (response.nextPageToken.isNotEmpty()) {
      println("nextPageToken: ${response.nextPageToken}")
    }
  }
}

@CommandLine.Command(name = "get", description = ["Get a metric calculation spec"])
class GetMetricCalculationSpecCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: MetricCalculationSpecsCommand

  @CommandLine.Parameters(description = ["API resource name of the Metric Calculation Spec"])
  private lateinit var metricCalculationSpecName: String

  override fun run() {
    val request = getMetricCalculationSpecRequest { name = metricCalculationSpecName }

    val metricCalculationSpec =
      runBlocking(Dispatchers.IO) {
        parent.metricCalculationSpecsStub.getMetricCalculationSpec(request)
      }
    println(metricCalculationSpec)
  }
}

@CommandLine.Command(
  name = "metric-calculation-specs",
  sortOptions = false,
  subcommands =
    [
      CommandLine.HelpCommand::class,
      CreateMetricCalculationSpecCommand::class,
      ListMetricCalculationSpecsCommand::class,
      GetMetricCalculationSpecCommand::class,
    ],
)
class MetricCalculationSpecsCommand : Runnable {
  @CommandLine.ParentCommand lateinit var parent: Reporting

  val metricCalculationSpecsStub: MetricCalculationSpecsCoroutineStub by lazy {
    MetricCalculationSpecsCoroutineStub(parent.channel)
  }

  override fun run() {}
}

@CommandLine.Command(name = "list", description = ["List event groups"])
class ListEventGroups : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: EventGroupsCommand

  @CommandLine.Option(
    names = ["--parent"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  @CommandLine.Option(
    names = ["--filter"],
    description = ["Result filter in format of raw CEL expression"],
    required = false,
    defaultValue = "",
  )
  private lateinit var celFilter: String

  @CommandLine.Mixin private lateinit var pageParams: PageParams

  override fun run() {
    val request = listEventGroupsRequest {
      parent = measurementConsumerName
      pageSize = pageParams.pageSize
      pageToken = pageParams.pageToken
      filter = celFilter
    }

    val response = runBlocking(Dispatchers.IO) { parent.eventGroupStub.listEventGroups(request) }

    println(response)
  }
}

@CommandLine.Command(
  name = "event-groups",
  sortOptions = false,
  subcommands = [CommandLine.HelpCommand::class, ListEventGroups::class],
)
class EventGroupsCommand : Runnable {
  @CommandLine.ParentCommand lateinit var parent: Reporting

  val eventGroupStub: EventGroupsCoroutineStub by lazy { EventGroupsCoroutineStub(parent.channel) }

  override fun run() {}
}

@CommandLine.Command(name = "get", description = ["Get data provider"])
class GetDataProvider : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: DataProvidersCommand

  @CommandLine.Parameters(description = ["CMMS DataProvider resource name"])
  private lateinit var cmmsDataProviderName: String

  override fun run() {
    val request = getDataProviderRequest { name = cmmsDataProviderName }

    val response = runBlocking(Dispatchers.IO) { parent.dataProviderStub.getDataProvider(request) }

    println(response)
  }
}

@CommandLine.Command(name = "get", description = ["Get event group metadata descriptor"])
class GetEventGroupMetadataDescriptor : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: EventGroupMetadataDescriptorsCommand

  @CommandLine.Parameters(description = ["CMMS EventGroupMetadataDescriptor resource name"])
  private lateinit var cmmsEventGroupMetadataDescriptorName: String

  override fun run() {
    val request = getEventGroupMetadataDescriptorRequest {
      name = cmmsEventGroupMetadataDescriptorName
    }

    val response =
      runBlocking(Dispatchers.IO) {
        parent.eventGroupMetadataDescriptorStub.getEventGroupMetadataDescriptor(request)
      }

    println(response)
  }
}

@CommandLine.Command(
  name = "batch-get",
  description = ["Batch Get event group metadata descriptors"],
)
class BatchGetEventGroupMetadataDescriptors : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: EventGroupMetadataDescriptorsCommand

  @CommandLine.Parameters(
    description = ["List of CMMS EventGroupMetadataDescriptors resource names"]
  )
  private var cmmsEventGroupMetadataDescriptorNames: List<String> = mutableListOf()

  override fun run() {
    val request = batchGetEventGroupMetadataDescriptorsRequest {
      names += cmmsEventGroupMetadataDescriptorNames
    }

    val response =
      runBlocking(Dispatchers.IO) {
        parent.eventGroupMetadataDescriptorStub.batchGetEventGroupMetadataDescriptors(request)
      }

    println(response)
  }
}

@CommandLine.Command(
  name = "data-providers",
  sortOptions = false,
  subcommands = [CommandLine.HelpCommand::class, GetDataProvider::class],
)
class DataProvidersCommand : Runnable {
  @CommandLine.ParentCommand lateinit var parent: Reporting

  val dataProviderStub: DataProvidersCoroutineStub by lazy {
    DataProvidersCoroutineStub(parent.channel)
  }

  override fun run() {}
}

@CommandLine.Command(
  name = "event-group-metadata-descriptors",
  sortOptions = false,
  subcommands =
    [
      CommandLine.HelpCommand::class,
      GetEventGroupMetadataDescriptor::class,
      BatchGetEventGroupMetadataDescriptors::class,
    ],
)
class EventGroupMetadataDescriptorsCommand : Runnable {
  @CommandLine.ParentCommand lateinit var parent: Reporting

  val eventGroupMetadataDescriptorStub: EventGroupMetadataDescriptorsCoroutineStub by lazy {
    EventGroupMetadataDescriptorsCoroutineStub(parent.channel)
  }

  override fun run() {}
}

@CommandLine.Command(
  name = "reporting",
  description = ["Reporting CLI tool"],
  sortOptions = false,
  subcommands =
    [
      CommandLine.HelpCommand::class,
      ReportingSetsCommand::class,
      ReportsCommand::class,
      MetricCalculationSpecsCommand::class,
      EventGroupsCommand::class,
      DataProvidersCommand::class,
      EventGroupMetadataDescriptorsCommand::class,
    ],
)
class Reporting : Runnable {
  @CommandLine.Mixin private lateinit var tlsFlags: TlsFlags
  @CommandLine.Mixin private lateinit var apiFlags: ReportingApiFlags

  val channel: ManagedChannel by lazy {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = tlsFlags.certFile,
        privateKeyFile = tlsFlags.privateKeyFile,
        trustedCertCollectionFile = tlsFlags.certCollectionFile,
      )
    buildMutualTlsChannel(apiFlags.apiTarget, clientCerts, apiFlags.apiCertHost)
      .withShutdownTimeout(Duration.ofSeconds(1))
  }

  override fun run() {}

  companion object {
    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(Reporting(), args, DurationFormat.ISO_8601)
  }
}

/**
 * Reporting Set, Report, Metric Calculation Spec, Event Group, Event Group Metadata Descriptor, and
 * Data Provider methods.
 *
 * Use the `help` command to see usage details.
 */
fun main(args: Array<String>) = commandLineMain(Reporting(), args)
