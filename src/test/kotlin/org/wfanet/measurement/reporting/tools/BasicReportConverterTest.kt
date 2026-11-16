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

package org.wfanet.measurement.reporting.tools

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.devtools.build.runfiles.Runfiles
import com.google.protobuf.ByteString
import com.google.protobuf.TextFormat
import com.google.protobuf.UnknownFieldSet
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import org.junit.Test
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.ResultGroup
import org.wfanet.measurement.reporting.v2alpha.copy

class BasicReportConverterTest {
  @Test
  fun `expand reconstructs the committed FULL example`() {
    val exampleDir =
      requireNotNull(
        Runfiles.preload()
          .withSourceRepository("")
          .getRuntimePath(
            Paths.get(
              "wfa_measurement_system",
              "src/main/kotlin/org/wfanet/measurement/reporting/tools/examples",
            )
          )
      )
    val full =
      BasicReport.newBuilder()
        .also {
          TextFormat.getParser().merge(Files.readString(exampleDir.resolve("full.textproto")), it)
        }
        .build()
    val compact =
      CompactBasicReport.newBuilder()
        .also {
          TextFormat.getParser()
            .merge(Files.readString(exampleDir.resolve("compact.textproto")), it)
        }
        .build()

    assertThat(BasicReportConverter.expand(compact, maxResultRows = 4)).isEqualTo(full)
    assertThat(BasicReportConverter.compact(full)).isEqualTo(compact)
  }

  @Test
  fun `compact shares metadata and exposes exact columns`() {
    val compact = BasicReportConverter.compact(ReportFixtures.full())
    assertThat(compact.reportingUnitsCount).isEqualTo(1)
    assertThat(compact.windowsCount).isEqualTo(1)
    assertThat(compact.dimensionsCount).isEqualTo(1)
    assertThat(compact.filtersCount).isEqualTo(1)
    assertThat(compact.componentNamesList)
      .containsExactly("dataProviders/publisher-a", "dataProviders/publisher-b")
      .inOrder()
    assertThat(compact.getGroups(0).metrics.reportingUnit.nonCumulative.reachValuesList)
      .containsExactly(120L, 240L)
      .inOrder()
    assertThat(
        compact.getGroups(0).metrics.reportingUnit.nonCumulative.kPlusReachValues.fixedLength
      )
      .isEqualTo(2)
    assertRoundTrip(ReportFixtures.full())
  }

  @Test
  fun `expand reads hand-authored compact columns`() {
    val compact = compactBasicReport {
      formatVersion = 1
      header = BasicReport.getDefaultInstance()
      groups +=
        CompactBasicReportKt.group {
          rowCount = 2
          metrics = metricSetColumns {
            presence = messagePresence { allPresent = true }
            populationSizeValues += listOf(100L, 200L)
            reportingUnit = reportingUnitMetricSetColumns {
              presence = messagePresence { allPresent = true }
              cumulative = basicMetricSetColumns {
                presence = messagePresence { allPresent = true }
                reachValues += listOf(40L, 70L)
              }
            }
          }
        }
    }
    val expected =
      BasicReport.newBuilder()
        .addResultGroups(
          ResultGroup.newBuilder()
            .addResults(
              ResultGroup.Result.newBuilder()
                .setMetricSet(
                  ResultGroup.MetricSet.newBuilder()
                    .setPopulationSize(100)
                    .setReportingUnit(
                      ResultGroup.MetricSet.ReportingUnitMetricSet.newBuilder()
                        .setCumulative(
                          ResultGroup.MetricSet.BasicMetricSet.newBuilder().setReach(40)
                        )
                    )
                )
            )
            .addResults(
              ResultGroup.Result.newBuilder()
                .setMetricSet(
                  ResultGroup.MetricSet.newBuilder()
                    .setPopulationSize(200)
                    .setReportingUnit(
                      ResultGroup.MetricSet.ReportingUnitMetricSet.newBuilder()
                        .setCumulative(
                          ResultGroup.MetricSet.BasicMetricSet.newBuilder().setReach(70)
                        )
                    )
                )
            )
        )
        .build()
    assertThat(BasicReportConverter.expand(compact, maxResultRows = 2)).isEqualTo(expected)
  }

  @Test
  fun `conversion preserves empty report and empty groups`() {
    assertRoundTrip(BasicReport.getDefaultInstance())
    assertRoundTrip(
      BasicReport.newBuilder().addResultGroups(ResultGroup.getDefaultInstance()).build()
    )
  }

  @Test
  fun `conversion distinguishes absent and empty messages`() {
    assertRoundTrip(
      report(
        ResultGroup.Result.getDefaultInstance(),
        ResultGroup.Result.newBuilder()
          .setMetadata(ResultGroup.MetricMetadata.getDefaultInstance())
          .build(),
        ResultGroup.Result.newBuilder()
          .setMetricSet(ResultGroup.MetricSet.getDefaultInstance())
          .build(),
        ResultGroup.Result.newBuilder()
          .setMetadata(
            ResultGroup.MetricMetadata.newBuilder()
              .setReportingUnitSummary(
                ResultGroup.MetricMetadata.ReportingUnitSummary.getDefaultInstance()
              )
              .setDimensionSpecSummary(
                ResultGroup.MetricMetadata.DimensionSpecSummary.getDefaultInstance()
              )
          )
          .build(),
      )
    )
  }

  @Test
  fun `conversion keeps zero metrics present`() {
    val zero =
      ReportFixtures.row(0).copy {
        metricSet =
          ResultGroup.MetricSet.newBuilder()
            .setReportingUnit(
              ResultGroup.MetricSet.ReportingUnitMetricSet.newBuilder()
                .setNonCumulative(ResultGroup.MetricSet.BasicMetricSet.getDefaultInstance())
            )
            .build()
      }
    val compact = BasicReportConverter.compact(report(zero, zero))
    assertThat(compact.getGroups(0).metrics.reportingUnit.nonCumulative.reachValuesCount)
      .isEqualTo(0)
    assertThat(compact.getGroups(0).metrics.reportingUnit.nonCumulative.presence.allPresent)
      .isTrue()
    assertRoundTrip(report(zero, zero))
  }

  @Test
  fun `conversion preserves negative and extreme integer values`() {
    val row =
      ReportFixtures.row(-5).copy {
        metricSet =
          metricSet
            .toBuilder()
            .setPopulationSize(Long.MAX_VALUE)
            .setReportingUnit(
              metricSet.reportingUnit.toBuilder().setStackedIncrementalReach(0, Long.MIN_VALUE)
            )
            .build()
      }
    assertRoundTrip(report(row))
  }

  @Test
  fun `conversion preserves float bit patterns`() {
    for (value in
      listOf(
        -0.0f,
        Float.fromBits(0x7fc01234),
        Float.POSITIVE_INFINITY,
        Float.NEGATIVE_INFINITY,
        1.234567f,
      )) {
      val row =
        ReportFixtures.row(120).copy {
          metricSet =
            metricSet
              .toBuilder()
              .setReportingUnit(
                metricSet.reportingUnit
                  .toBuilder()
                  .setNonCumulative(
                    metricSet.reportingUnit.nonCumulative.toBuilder().setAverageFrequency(value)
                  )
              )
              .build()
        }
      val restored = assertRoundTrip(report(row))
      assertThat(
          restored
            .getResultGroups(0)
            .getResults(0)
            .metricSet
            .reportingUnit
            .nonCumulative
            .averageFrequency
            .toRawBits()
        )
        .isEqualTo(value.toRawBits())
    }
  }

  @Test
  fun `conversion preserves varying histogram lengths including zeros`() {
    val rows =
      listOf(emptyList(), listOf(0L), listOf(30L, 20L, 10L)).map { values ->
        ReportFixtures.row(120).copy {
          metricSet =
            metricSet
              .toBuilder()
              .setReportingUnit(
                metricSet.reportingUnit
                  .toBuilder()
                  .setNonCumulative(
                    metricSet.reportingUnit.nonCumulative
                      .toBuilder()
                      .clearKPlusReach()
                      .addAllKPlusReach(values)
                  )
              )
              .build()
        }
      }
    assertRoundTrip(report(*rows.toTypedArray()))
  }

  @Test
  fun `conversion preserves varying component counts and ordering`() {
    val base = ReportFixtures.row(120)
    val empty = base.copy { metricSet = metricSet.toBuilder().clearComponents().build() }
    val one =
      base.copy {
        metricSet =
          metricSet
            .toBuilder()
            .clearComponents()
            .addComponents(base.metricSet.getComponents(1))
            .build()
      }
    val reversed =
      base.copy {
        metricSet =
          metricSet
            .toBuilder()
            .clearComponents()
            .addAllComponents(base.metricSet.componentsList.reversed())
            .build()
      }
    assertRoundTrip(report(empty, one, reversed))
  }

  @Test
  fun `conversion preserves absent component values and empty names`() {
    val row =
      ReportFixtures.row(120).copy {
        metricSet =
          metricSet
            .toBuilder()
            .clearComponents()
            .addComponents(ResultGroup.MetricSet.ComponentMetricSetMapEntry.getDefaultInstance())
            .build()
      }
    assertRoundTrip(report(row))
  }

  @Test
  fun `conversion preserves metadata timestamps and distinct summaries`() {
    val base = ReportFixtures.row(120)
    val other =
      base.copy {
        metadata =
          metadata
            .toBuilder()
            .setMetricEndTime(metadata.metricEndTime.toBuilder().setNanos(1))
            .setReportingUnitSummary(
              ResultGroup.MetricMetadata.ReportingUnitSummary.getDefaultInstance()
            )
            .clearDimensionSpecSummary()
            .clearFilter()
            .build()
      }
    assertRoundTrip(report(base, other))
  }

  @Test
  fun `conversion preserves unknown protobuf fields at every level`() {
    val unknown =
      UnknownFieldSet.newBuilder()
        .addField(999, UnknownFieldSet.Field.newBuilder().addVarint(123).build())
        .build()
    val base = ReportFixtures.row(120)
    val row =
      base
        .toBuilder()
        .setUnknownFields(unknown)
        .setMetadata(
          base.metadata
            .toBuilder()
            .setUnknownFields(unknown)
            .setReportingUnitSummary(
              base.metadata.reportingUnitSummary.toBuilder().setUnknownFields(unknown)
            )
        )
        .setMetricSet(
          base.metricSet
            .toBuilder()
            .setUnknownFields(unknown)
            .setReportingUnit(
              base.metricSet.reportingUnit
                .toBuilder()
                .setUnknownFields(unknown)
                .setNonCumulative(
                  base.metricSet.reportingUnit.nonCumulative.toBuilder().setUnknownFields(unknown)
                )
            )
        )
        .build()
    val full =
      report(row, ResultGroup.Result.getDefaultInstance())
        .toBuilder()
        .setUnknownFields(unknown)
        .apply { setResultGroups(0, getResultGroups(0).toBuilder().setUnknownFields(unknown)) }
        .build()
    assertRoundTrip(full)
  }

  @Test
  fun `expand rejects unsupported version`() {
    val compact =
      BasicReportConverter.compact(ReportFixtures.full()).toBuilder().setFormatVersion(2).build()
    assertFailsWith<IllegalArgumentException> { BasicReportConverter.expand(compact, 100) }
  }

  @Test
  fun `expand rejects excessive total row count before allocation`() {
    assertFailsWith<IllegalArgumentException> {
      BasicReportConverter.expand(BasicReportConverter.compact(ReportFixtures.full()), 1)
    }
    assertFailsWith<IllegalArgumentException> {
      BasicReportConverter.expand(
        BasicReportConverter.compact(BasicReport.getDefaultInstance()),
        -1,
      )
    }
  }

  @Test
  fun `expand rejects rows represented by overflowing unsigned value`() {
    assertInvalid { it.setRowCount(-1) }
  }

  @Test
  fun `expand rejects mixed full and compact results`() {
    val compact =
      BasicReportConverter.compact(ReportFixtures.full())
        .toBuilder()
        .setHeader(ReportFixtures.full())
        .build()
    assertFailsWith<IllegalArgumentException> { BasicReportConverter.expand(compact, 100) }
    assertInvalid { it.setHeader(ReportFixtures.full().getResultGroups(0)) }
  }

  @Test
  fun `expand rejects metadata column with wrong length`() {
    assertInvalid { it.clearWindowIndices().addWindowIndices(1) }
  }

  @Test
  fun `expand rejects out of range dictionary index`() {
    assertInvalid { it.setWindowIndices(0, 99) }
    assertInvalid { it.setReportingUnitIndices(0, -1) }
    assertInvalid { it.setDimensionIndices(0, 99) }
    assertInvalid { it.setFilterIndices(0, 99) }
  }

  @Test
  fun `expand rejects references attached to absent metadata`() {
    assertInvalid { it.setWindowIndices(0, 0) }
  }

  @Test
  fun `expand rejects scalar column with wrong length`() {
    assertInvalid {
      it.setMetrics(it.metrics.toBuilder().clearPopulationSizeValues().addPopulationSizeValues(1))
    }
  }

  @Test
  fun `expand rejects malformed histogram lengths`() {
    val full = report(ReportFixtures.row(120), ReportFixtures.row(240))
    val compact = BasicReportConverter.compact(full)
    val vector = compact.getGroups(0).metrics.reportingUnit.nonCumulative.kPlusReachValues
    for (bad in
      listOf(
        vector.toBuilder().setFixedLength(3).build(),
        vector.toBuilder().addLengths(2).build(),
        vector.toBuilder().clearFixedLength().addLengths(2).build(),
      )) {
      assertInvalid {
        it.setMetrics(
          it.metrics
            .toBuilder()
            .setReportingUnit(
              it.metrics.reportingUnit
                .toBuilder()
                .setNonCumulative(
                  it.metrics.reportingUnit.nonCumulative.toBuilder().setKPlusReachValues(bad)
                )
            )
        )
      }
    }
  }

  @Test
  fun `expand rejects malformed presence bitmap`() {
    for (bytes in listOf(ByteString.EMPTY, ByteString.copyFrom(byteArrayOf(0x83.toByte())))) {
      assertInvalid {
        it.setMetrics(it.metrics.toBuilder().setPresence(messagePresence { presentRows = bytes }))
      }
    }
    assertInvalid {
      it.setMetrics(it.metrics.toBuilder().setPresence(messagePresence { allPresent = false }))
    }
  }

  @Test
  fun `expand rejects values in an absent metric message`() {
    assertInvalid {
      it.setMetrics(
        it.metrics
          .toBuilder()
          .setPresence(messagePresence { presentRows = ByteString.copyFrom(byteArrayOf(0)) })
      )
    }
  }

  @Test
  fun `expand rejects noncontiguous component presence`() {
    assertInvalid {
      it.setMetrics(
        it.metrics
          .toBuilder()
          .setComponents(
            0,
            componentEntryColumns {
              presence = messagePresence { presentRows = ByteString.copyFrom(byteArrayOf(0)) }
            },
          )
      )
    }
  }

  @Test
  fun `expand rejects invalid component name reference`() {
    assertInvalid {
      it.setMetrics(
        it.metrics
          .toBuilder()
          .setComponents(0, it.metrics.getComponents(0).toBuilder().setKeyIndices(0, 99))
      )
    }
  }

  @Test
  fun `expand rejects unknown compact schema fields`() {
    val unknown =
      UnknownFieldSet.newBuilder()
        .addField(999, UnknownFieldSet.Field.newBuilder().addVarint(1).build())
        .build()
    val compact =
      BasicReportConverter.compact(ReportFixtures.full())
        .toBuilder()
        .setUnknownFields(unknown)
        .build()
    assertFailsWith<IllegalArgumentException> { BasicReportConverter.expand(compact, 100) }
  }

  private fun report(vararg rows: ResultGroup.Result): BasicReport =
    ReportFixtures.full().copy {
      resultGroups.clear()
      resultGroups += ResultGroup.newBuilder().addAllResults(rows.toList()).build()
    }

  private fun assertRoundTrip(full: BasicReport): BasicReport {
    val compact = CompactBasicReport.parseFrom(BasicReportConverter.compact(full).toByteString())
    val restored = BasicReportConverter.expand(compact, maxResultRows = 1000)
    assertThat(restored).isEqualTo(full)
    return restored
  }

  private fun assertInvalid(change: (CompactBasicReport.Group.Builder) -> Unit) {
    val compact = BasicReportConverter.compact(ReportFixtures.full()).toBuilder()
    compact.setGroups(0, compact.getGroups(0).toBuilder().also(change))
    assertFailsWith<IllegalArgumentException> { BasicReportConverter.expand(compact.build(), 100) }
  }
}
