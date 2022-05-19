/**
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * ```
 *      http://www.apache.org/licenses/LICENSE-2.0
 * ```
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import com.google.common.truth.Truth.assertThat
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import java.io.BufferedReader
import java.io.FileReader
import java.nio.file.Path
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.random.Random
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.timeInterval
import org.wfanet.measurement.common.getJarResourcePath
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.TestPrivacyBucketMapper

private const val FOLDER_PATH =
  "/google/src/cloud/uakyol/om_open_source/google3/experimental/users/uakyol/pbmForCommscore/cross-media-measurement/src/test/kotlin/org/wfanet/measurement/eventdataprovider/privacybudgetmanagement"

private const val MEASUREMENT_CONSUMER_ID = "ACME"

const val PRIVACY_BUCKET_VID_SAMPLE_WIDTH = 1f / 300f

@RunWith(JUnit4::class)
class CommscorePbmTest {
  private val privacyBucketFilter = PrivacyBucketFilter(TestPrivacyBucketMapper())

  private fun getResourcePath(fileName: String): Path {
    val classLoader: ClassLoader = Thread.currentThread().contextClassLoader
    return requireNotNull(classLoader.getJarResourcePath(fileName)) {
      "Resource $fileName not found"
    }
  }

  private fun getGender(gen: String): Int {
    if (gen == "F") return 1 else return 0
  }

  private fun getAge(age: String): Int {
    if (age == "18-24") return 0
    else if (age == "25-34") return 1
    else if (age == "35-44") return 2 else if (age == "45-54") return 3 else return 4
  }

  private fun getSubExpression(elem: String): String {
    if (elem == "*")
      return "privacy_budget.gender.value in [0, 1] && privacy_budget.age.value in [0, 1, 2]"
    return "(privacy_budget.gender.value == ${getGender(elem.split(" ").get(0))} && privacy_budget.age.value == ${getAge(elem.split(" ").get(1))})"
  }

  private fun getFilterExpression(elem: JsonElement): String {
    if (elem.isJsonArray()) {
      val arr = elem.getAsJsonArray()
      return arr.map { getSubExpression(it.getAsString()) }.joinToString(" || ")
    }
    return getSubExpression(elem.getAsString())
  }

  private fun getStartOffset(elem: JsonElement):Long{
    try{
      return 31 - elem.getAsInt().toLong()
    } catch (e: Exception) {
      return elem.getAsString().split("..").get(0).toLong()
    }
  }

    private fun getEndOffset(elem: JsonElement):Long{
    try{
      return 31 - elem.getAsInt().toLong()
    } catch (e: Exception) {
      return elem.getAsString().split("..").get(1).toLong()
    }
  }
  private fun constructReqSpec(obj: JsonObject): RequisitionSpec {
    return requisitionSpec {
      eventGroups += eventGroupEntry {
        key = "eventGroups/someEventGroup"
        value =
          RequisitionSpecKt.EventGroupEntryKt.value {
            collectionInterval = timeInterval {
              startTime =
                LocalDate.now().minusDays(getStartOffset(obj.get("days"))).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
              endTime = LocalDate.now().minusDays(getEndOffset(obj.get("days"))).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
            }
            filter = eventFilter { expression = getFilterExpression(obj.get("demographics")) }
          }
      }
    }
  }
  private fun constructMeasurementSpec(obj: JsonObject): MeasurementSpec {
    return measurementSpec {
      reachAndFrequency = reachAndFrequency {
        reachPrivacyParams = differentialPrivacyParams {
          epsilon = 0.01
          delta = MAXIMUM_DELTA_PER_BUCKET.toDouble()
        }

        frequencyPrivacyParams = differentialPrivacyParams {
          epsilon = 0.01
          delta = MAXIMUM_DELTA_PER_BUCKET.toDouble()
        }

        vidSamplingInterval = vidSamplingInterval {
          start = PRIVACY_BUCKET_VID_SAMPLE_WIDTH * Random.nextInt(0, 299)
          width = PRIVACY_BUCKET_VID_SAMPLE_WIDTH * 3
        }
      }
    }
  }

  private fun filterRow(obj: JsonObject): Boolean {
    val publisher = obj.get("publisher").getAsString()
    if (publisher != "Google" && publisher != "*") {
      return false
    }
    // if (obj.get("platform").getAsString() != "CrossMedia") return false
    // if (obj.get("query_type").getAsString() != "ReachAndFrequency") return false
    return true
  }

  private fun parseRow(elem: JsonElement): JsonObject? {
    try {
      return elem.getAsJsonObject()
      val obj: JsonObject = elem.getAsJsonObject()
    } catch (e: Exception) {
      println(e)
    }
    return null
  }

  @Test
  fun `chargePrivacyBudget throws PRIVACY_BUDGET_EXCEEDED when given a large single charge`() {

    val br: BufferedReader = BufferedReader(FileReader("$FOLDER_PATH/queries.json"))
    val parser: JsonParser = JsonParser()
    val obj: JsonObject = parser.parse(br).getAsJsonObject()
    println(obj.entrySet().size)
    val newRows = obj.entrySet().map { parseRow(it.value) }.filter { it != null && filterRow(it) }
    println(newRows.size)
    println(newRows.javaClass.kotlin)

    val backingStore = InMemoryBackingStore()
    val pbm =
      PrivacyBudgetManager(privacyBucketFilter, backingStore, 1.0f, MAXIMUM_DELTA_PER_BUCKET)
    for (i in 0..newRows.size - 1) {
      val reqSpec = constructReqSpec(newRows.get(i)!!)
      val measurementSpec = constructMeasurementSpec(newRows.get(i)!!)
      pbm.chargePrivacyBudget(MEASUREMENT_CONSUMER_ID, reqSpec, measurementSpec)
    }
  }
}
