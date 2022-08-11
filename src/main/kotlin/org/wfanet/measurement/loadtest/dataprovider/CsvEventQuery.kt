// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.dataprovider

import com.opencsv.CSVReaderBuilder
import java.io.FileReader
import java.nio.file.Paths
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventFilter
import org.wfanet.measurement.common.getRuntimePath

/** Fulfill the query with VIDs imported from CSV file. */
class CsvEventQuery(
  private val edpDisplayName: String,
) : EventQuery() {
  private val edpIdIndex = 0
  private val vidIndex = 7
  private var allFilteredByEdpVids: MutableList<Long> = mutableListOf()

  /** Import VIDs from CSV file. */
  init {
    // Place CSV files in //src/main/kotlin/org/wfanet/measurement/loadtest/dataprovider/data
    val directoryPath =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "kotlin",
        "org",
        "wfanet",
        "measurement",
        "loadtest",
        "dataprovider",
        "data",
      )
    // Update fileName to match the CSV file you want to use
    val fileName = "synthetic-labelled-events-small.csv"
    // val fileName = "benchmark_data_large.csv"
    val fileRuntimePath = getRuntimePath(directoryPath.resolve(fileName)).toString()
    logger.info("Reading data from CSV file...")
    val fileReader = FileReader(fileRuntimePath)

    fileReader.use {
      val csvReader = CSVReaderBuilder(fileReader).withSkipLines(1).build()
      csvReader.use { reader ->
        var row = reader.readNext()
        while (row != null) {
          if (row[edpIdIndex] == edpDisplayName.last().toString()) {
            allFilteredByEdpVids.add(row[vidIndex].toLong())
          }

          row = reader.readNext()
        }
      }
    }
    logger.info("Finished reading data from CSV file")
  }

  override fun getUserVirtualIds(eventFilter: EventFilter): Sequence<Long> {
    logger.info("Querying VIDs from CsvEventQuery...")

    return allFilteredByEdpVids.asSequence()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
