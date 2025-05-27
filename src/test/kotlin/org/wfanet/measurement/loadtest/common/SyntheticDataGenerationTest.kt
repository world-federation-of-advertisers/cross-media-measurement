package org.wfanet.measurement.loadtest.common

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import java.nio.file.Paths

@RunWith(JUnit4::class)
class SyntheticDataGenerationTest {

  @Test
  fun `verifies that data is correctly spread across days`() {
    val syntheticPopulationSpec: SyntheticPopulationSpec =
        parseTextProto(
            TEST_DATA_RUNTIME_PATH.resolve("small_population_spec.textproto").toFile(),
            SyntheticPopulationSpec.getDefaultInstance(),
        )
    val syntheticEventGroupSpec: SyntheticEventGroupSpec =
        parseTextProto(
            TEST_DATA_RUNTIME_PATH.resolve("small_data_spec.textproto").toFile(),
            SyntheticEventGroupSpec.getDefaultInstance(),
        )
    val events =
        SyntheticDataGeneration.generateEvents(
            messageInstance = TestEvent.getDefaultInstance(),
            populationSpec = syntheticPopulationSpec,
            syntheticEventGroupSpec = syntheticEventGroupSpec,
        )
      runBlocking {
          val eventsList = events.toList()
          assertThat(eventsList.map { it.localDate.toString() })
              .isEqualTo(
                  listOf(
                      "2021-03-15",
                      "2021-03-16",
                      "2021-03-17",
                      "2021-03-18",
                      "2021-03-19",
                      "2021-03-20",
                      "2021-03-21",
                  )
              )
          // 8000 total reach / 7 days
          eventsList.map { assertThat(it.impressions.toList().size).isWithin(100).of(8000 / 7) }
          assertThat(eventsList.flatMap {
            it.impressions.toList()
          }.size).isEqualTo(8001)
      }
  }

  companion object {
    private val TEST_DATA_PATH =
        Paths.get(
            "wfa_measurement_system",
            "src",
            "main",
            "proto",
            "wfa",
            "measurement",
            "loadtest",
            "edpaggregator",
        )
    private val TEST_DATA_RUNTIME_PATH = getRuntimePath(TEST_DATA_PATH)!!
  }
}
