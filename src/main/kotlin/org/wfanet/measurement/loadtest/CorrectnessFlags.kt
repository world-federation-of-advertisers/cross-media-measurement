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

package org.wfanet.measurement.loadtest

import java.io.File
import kotlin.properties.Delegates
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import picocli.CommandLine

class CorrectnessFlags {

  @CommandLine.Mixin
  lateinit var spannerFlags: SpannerFlags
    private set

  @CommandLine.Option(
    names = ["--bigquery-table"],
    description = ["Name of the BigQuery table."],
    required = true
  )
  lateinit var tableName: String
    private set

  @set:CommandLine.Option(
    names = ["--data-provider-count"],
    description = ["Number of Data Providers."],
    defaultValue = "2"
  )
  var dataProviderCount by Delegates.notNull<Int>()
    private set

  @set:CommandLine.Option(
    names = ["--campaign-count"],
    description = ["Number of Campaigns per each Data Provider."],
    defaultValue = "1"
  )
  var campaignCount by Delegates.notNull<Int>()
    private set

  @set:CommandLine.Option(
    names = ["--generated-set-size"],
    description = ["Set size of the reach to generate per campaign."],
    defaultValue = "1000"
  )
  var generatedSetSize by Delegates.notNull<Int>()
    private set

  @set:CommandLine.Option(
    names = ["--universe-size"],
    description = ["Universe size of the reach per campaign (Default is 10B)."],
    defaultValue = "10000000000"
  )
  var universeSize by Delegates.notNull<Long>()
    private set

  @CommandLine.Option(
    names = ["--sketch-config-file"],
    description = ["File path for SketchConfig proto message in text format."],
    defaultValue = "config/liquid_legions_sketch_config.textproto"
  )
  lateinit var sketchConfigFile: File
    private set

  @CommandLine.Option(
    names = ["--publisher-data-service-target"],
    description = ["Address and port of the Publisher Data Service."],
    required = true
  )
  lateinit var publisherDataServiceTarget: String
    private set

  @CommandLine.Option(
    names = ["--run-id"],
    description = ["Unique identifier of the run (Default is timestamp)."],
    required = false
  )
  lateinit var runId: String
    private set

  @CommandLine.Option(
    names = ["--event-data-generation"],
    description = ["Event data generation type."],
    defaultValue = "query"
  )
  lateinit var eventDataGeneration: String
    private set

  @CommandLine.Option(
    names = ["--publisher-ids"],
    description = ["Publisher Ids."],
    defaultValue = "1,2,3,4,5,6",
    split = ","
  )
  lateinit var publisherIds: Array<String>
    private set

  @CommandLine.Option(names = ["--sex"], description = ["Sex."], defaultValue = "M,F", split = ",")
  lateinit var sex: Array<String>
    private set

  @CommandLine.Option(
    names = ["--age-groups"],
    description = ["Age Group."],
    defaultValue = "18_34,35_54,55+",
    split = ","
  )
  lateinit var ageGroup: Array<String>
    private set

  @CommandLine.Option(
    names = ["--social-grades"],
    description = ["Social Grade."],
    defaultValue = "ABC1,C2DE",
    split = ","
  )
  lateinit var socialGrade: Array<String>
    private set

  @CommandLine.Option(
    names = ["--complete"],
    description = ["Complete."],
    defaultValue = "0,1",
    split = ","
  )
  lateinit var complete: Array<String>
    private set

  @CommandLine.Option(
    names = ["--begin-date"],
    description = ["Begin Date."],
    defaultValue = "2021-03-01"
  )
  lateinit var beginDate: String
    private set

  @CommandLine.Option(
    names = ["--end-date"],
    description = ["End Date."],
    defaultValue = "2021-03-28"
  )
  lateinit var endDate: String
    private set
}
