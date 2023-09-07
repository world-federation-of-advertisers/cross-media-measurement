// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.api.v2alpha.tools

import java.io.File
import java.time.Instant
import kotlin.properties.Delegates
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import picocli.CommandLine.ArgGroup
import picocli.CommandLine.Option

class CreateMeasurementFlags {

  @Option(
    names = ["--measurement-consumer"],
    description = ["API resource name of the MeasurementConsumer"],
    required = true
  )
  lateinit var measurementConsumer: String

  @Option(
    names = ["--request-id"],
    description = ["ID of API request for idempotency"],
    required = false,
    defaultValue = "",
  )
  lateinit var requestId: String

  @Option(
    names = ["--private-key-der-file"],
    description = ["Private key for MeasurementConsumer"],
    required = true
  )
  lateinit var privateKeyDerFile: File

  @Option(
    names = ["--measurement-ref-id"],
    description = ["Measurement reference id"],
    required = false,
    defaultValue = ""
  )
  lateinit var measurementReferenceId: String

  @ArgGroup(
    exclusive = true,
    multiplicity = "1",
    heading = "Specify either Event or Population Measurement with its params\n"
  )
  lateinit var measurementParams: MeasurementParams

  @Option(
    names = ["--model-line"],
    description = ["API resource name of the ModelLine"],
    required = false,
    defaultValue = "",
  )
  lateinit var modelLine: String

  class MeasurementParams {
    @ArgGroup(exclusive = false, multiplicity = "1", heading = "Event Measurement and params\n")
    var eventMeasurementParams = EventMeasurementParams()

    @ArgGroup(
      exclusive = false,
      multiplicity = "1",
      heading = "Population Measurement and params\n"
    )
    var populationMeasurementParams = PopulationMeasurementParams()

    class EventMeasurementParams {
      class EventDataProviderInput {
        @Option(
          names = ["--event-data-provider"],
          description = ["API resource name of the Event Data Provider"],
          required = true,
        )
        lateinit var name: String
          private set

        @ArgGroup(
          exclusive = false,
          multiplicity = "1..*",
          heading = "Add EventGroups for an Event Data Provider\n"
        )
        lateinit var eventGroupInputs: List<EventGroupInput>
          private set
      }

      class EventGroupInput {
        @Option(
          names = ["--event-group"],
          description = ["API resource name of the EventGroup"],
          required = true,
        )
        lateinit var name: String
          private set

        @Option(
          names = ["--event-filter"],
          description = ["Raw CEL expression of EventFilter"],
          required = false,
          defaultValue = ""
        )
        lateinit var eventFilter: String
          private set

        @Option(
          names = ["--event-start-time"],
          description = ["Start time of Event range in ISO 8601 format of UTC"],
          required = true,
        )
        lateinit var eventStartTime: Instant
          private set

        @Option(
          names = ["--event-end-time"],
          description = ["End time of Event range in ISO 8601 format of UTC"],
          required = true,
        )
        lateinit var eventEndTime: Instant
          private set
      }

      @ArgGroup(exclusive = false, multiplicity = "1..*", heading = "Add Event Data Providers\n")
      lateinit var eventDataProviderInputs: List<EventDataProviderInput>
        private set

      @set:Option(
        names = ["--vid-sampling-start"],
        description = ["Start point of vid sampling interval"],
        required = true,
      )
      var vidSamplingStart by Delegates.notNull<Float>()
        private set

      @set:Option(
        names = ["--vid-sampling-width"],
        description = ["Width of vid sampling interval"],
        required = true,
      )
      var vidSamplingWidth by Delegates.notNull<Float>()
        private set

      class EventMeasurementTypeParams {
        class ReachAndFrequencyParams {
          @Option(
            names = ["--reach-and-frequency"],
            description = ["Measurement Type of ReachAndFrequency"],
            required = true,
          )
          var selected = false
            private set

          @set:Option(
            names = ["--reach-privacy-epsilon"],
            description = ["Epsilon value of reach privacy params"],
            required = true,
          )
          var reachPrivacyEpsilon by Delegates.notNull<Double>()
            private set

          @set:Option(
            names = ["--reach-privacy-delta"],
            description = ["Delta value of reach privacy params"],
            required = true,
          )
          var reachPrivacyDelta by Delegates.notNull<Double>()
            private set

          @set:Option(
            names = ["--frequency-privacy-epsilon"],
            description = ["Epsilon value of frequency privacy params"],
            required = true,
          )
          var frequencyPrivacyEpsilon by Delegates.notNull<Double>()
            private set

          @set:Option(
            names = ["--frequency-privacy-delta"],
            description = ["Epsilon value of frequency privacy params"],
            required = true,
          )
          var frequencyPrivacyDelta by Delegates.notNull<Double>()
            private set

          @set:Option(
            names = ["--max-frequency"],
            description = ["Maximum frequency revealed in the distribution"],
            required = false,
            defaultValue = "10",
          )
          var maximumFrequency by Delegates.notNull<Int>()
            private set
        }

        class ImpressionParams {
          @Option(
            names = ["--impression"],
            description = ["Measurement Type of Impression"],
            required = true,
          )
          var selected = false
            private set

          @set:Option(
            names = ["--impression-privacy-epsilon"],
            description = ["Epsilon value of impression privacy params"],
            required = true,
          )
          var privacyEpsilon by Delegates.notNull<Double>()
            private set

          @set:Option(
            names = ["--impression-privacy-delta"],
            description = ["Epsilon value of impression privacy params"],
            required = true,
          )
          var privacyDelta by Delegates.notNull<Double>()
            private set

          @set:Option(
            names = ["--max-frequency-per-user"],
            description = ["Maximum frequency per user"],
            required = true,
          )
          var maximumFrequencyPerUser by Delegates.notNull<Int>()
            private set
        }

        class DurationParams {
          @Option(
            names = ["--duration"],
            description = ["Measurement Type of Duration"],
            required = true,
          )
          var selected = false
            private set

          @set:Option(
            names = ["--duration-privacy-epsilon"],
            description = ["Epsilon value of duration privacy params"],
            required = true,
          )
          var privacyEpsilon by Delegates.notNull<Double>()
            private set

          @set:Option(
            names = ["--duration-privacy-delta"],
            description = ["Epsilon value of duration privacy params"],
            required = true,
          )
          var privacyDelta by Delegates.notNull<Double>()
            private set

          @set:Option(
            names = ["--max-duration"],
            description = ["Maximum watch duration per user"],
            required = true,
          )
          var maximumWatchDurationPerUser by Delegates.notNull<Int>()
            private set
        }

        @ArgGroup(exclusive = false, heading = "Measurement type ReachAndFrequency and params\n")
        var reachAndFrequency = ReachAndFrequencyParams()
        @ArgGroup(exclusive = false, heading = "Measurement type Impression and params\n")
        var impression = ImpressionParams()
        @ArgGroup(exclusive = false, heading = "Measurement type Duration and params\n")
        var duration = DurationParams()
      }

      @ArgGroup(exclusive = true, multiplicity = "1", heading = "Event Measurement and params\n")
      var eventMeasurementTypeParams = EventMeasurementTypeParams()
    }
    class PopulationMeasurementParams {
      class PopulationInput {
        @Option(
          names = ["--population-filter"],
          description = ["Raw CEL expression of Population Filter"],
          required = false,
          defaultValue = ""
        )
        lateinit var filter: String
          private set

        @Option(
          names = ["--population-start-time"],
          description = ["Start time of Population range in ISO 8601 format of UTC"],
          required = true,
        )
        lateinit var startTime: Instant
          private set

        @Option(
          names = ["--population-end-time"],
          description = ["End time of Population range in ISO 8601 format of UTC"],
          required = true,
        )
        lateinit var endTime: Instant
          private set
      }

      class PopulationDataProviderInput {
        @Option(
          names = ["--population-data-provider"],
          description = ["API resource name of the Population Data Provider"],
          required = true,
        )
        lateinit var name: String
          private set
      }

      @ArgGroup(exclusive = false, heading = "Population Params\n")
      lateinit var populationInputs: PopulationInput
        private set
      @ArgGroup(exclusive = false, heading = "Set Population Data Provider\n")
      lateinit var populationDataProviderInput: PopulationDataProviderInput

      @Option(
        names = ["--population"],
        description = ["Population Measurement"],
        required = true,
      )
      var selected = false
        private set
    }
  }

  fun getReachAndFrequency(): MeasurementSpec.ReachAndFrequency {
    return MeasurementSpecKt.reachAndFrequency {
      reachPrivacyParams = differentialPrivacyParams {
        epsilon =
          measurementParams.eventMeasurementParams.eventMeasurementTypeParams.reachAndFrequency
            .reachPrivacyEpsilon
        delta =
          measurementParams.eventMeasurementParams.eventMeasurementTypeParams.reachAndFrequency
            .reachPrivacyDelta
      }
      frequencyPrivacyParams = differentialPrivacyParams {
        epsilon =
          measurementParams.eventMeasurementParams.eventMeasurementTypeParams.reachAndFrequency
            .frequencyPrivacyEpsilon
        delta =
          measurementParams.eventMeasurementParams.eventMeasurementTypeParams.reachAndFrequency
            .frequencyPrivacyDelta
      }
      maximumFrequency =
        measurementParams.eventMeasurementParams.eventMeasurementTypeParams.reachAndFrequency
          .maximumFrequency
    }
  }

  fun getImpression(): MeasurementSpec.Impression {
    return MeasurementSpecKt.impression {
      privacyParams = differentialPrivacyParams {
        epsilon =
          measurementParams.eventMeasurementParams.eventMeasurementTypeParams.impression
            .privacyEpsilon
        delta =
          measurementParams.eventMeasurementParams.eventMeasurementTypeParams.impression
            .privacyDelta
      }
      maximumFrequencyPerUser =
        measurementParams.eventMeasurementParams.eventMeasurementTypeParams.impression
          .maximumFrequencyPerUser
    }
  }

  fun getDuration(): MeasurementSpec.Duration {
    return MeasurementSpecKt.duration {
      privacyParams = differentialPrivacyParams {
        epsilon =
          measurementParams.eventMeasurementParams.eventMeasurementTypeParams.duration
            .privacyEpsilon
        delta =
          measurementParams.eventMeasurementParams.eventMeasurementTypeParams.duration.privacyDelta
      }
      maximumWatchDurationPerUser =
        measurementParams.eventMeasurementParams.eventMeasurementTypeParams.duration
          .maximumWatchDurationPerUser
    }
  }

  fun getPopulation(): MeasurementSpec.Population {
    return MeasurementSpecKt.population {}
  }
}
