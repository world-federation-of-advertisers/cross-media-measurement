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
import kotlin.random.Random
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventFilter
import org.wfanet.measurement.common.getRuntimePath

/** Fulfill the query with VIDs imported from CSV file. */
class CsvEventQuery : EventQuery() {

  /** Import VIDs from CSV file. */
  override fun getUserVirtualIds(eventFilter: EventFilter): Sequence<Long> {
    // val filePath =
    // "src/main/kotlin/org/wfanet/measurement/loadtest/dataprovider/data/synthetic-labelled-events.csv"

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
    val fileName = "synthetic-labelled-events.csv"
    val filePath = getRuntimePath(directoryPath.resolve(fileName)).toString()
    val fileReader = FileReader(filePath)

    fileReader.use {
      val csvReader = CSVReaderBuilder(fileReader).withSkipLines(1).build()
      csvReader.use {
        var allLines = csvReader.readAll()
        allLines = allLines.subList(0, 5000)
        return sequence { allLines.forEach { line -> yield(line[line.size - 1].toLong()) } }
      }
    }
  }

  fun getUserEvents(eventFilter: EventFilter): Sequence<Long> {
    return sequence {
      for (i in 1..1000) {
        yield(Random.nextInt(1, 10000 + 1).toLong())
      }
    }
  }
}
