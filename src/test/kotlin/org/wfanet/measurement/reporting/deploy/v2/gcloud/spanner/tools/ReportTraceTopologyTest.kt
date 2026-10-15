/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.tools

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.config.reporting.DataProviderRoute
import org.wfanet.measurement.config.reporting.dataProviderRoute
import org.wfanet.measurement.config.reporting.reportTraceTopologyConfig

@RunWith(JUnit4::class)
class ReportTraceTopologyTest {
  @Test
  fun `fromConfig requires every route to be specified`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ReportTraceTopology.fromConfig(
          reportTraceTopologyConfig {
            dataProviderRoutes += dataProviderRoute { dataProvider = DATA_PROVIDER }
          }
        )
      }

    assertThat(exception).hasMessageThat().contains("must be DIRECT_EDP or EDPA")
  }

  @Test
  fun `fromConfig rejects duplicate DataProviders`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ReportTraceTopology.fromConfig(
          reportTraceTopologyConfig {
            dataProviderRoutes +=
              listOf(route(DataProviderRoute.Route.EDPA), route(DataProviderRoute.Route.DIRECT_EDP))
          }
        )
      }

    assertThat(exception).hasMessageThat().contains("Duplicate DataProvider route")
  }

  @Test
  fun `fromConfig rejects invalid DataProvider resource name`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ReportTraceTopology.fromConfig(
          reportTraceTopologyConfig {
            dataProviderRoutes += dataProviderRoute {
              dataProvider = "not-a-data-provider"
              route = DataProviderRoute.Route.EDPA
            }
          }
        )
      }

    assertThat(exception).hasMessageThat().contains("not a valid DataProvider resource name")
  }

  @Test
  fun `fromConfig preserves explicit direct and EDPA routes`() {
    val topology =
      ReportTraceTopology.fromConfig(
        reportTraceTopologyConfig {
          dataProviderRoutes +=
            listOf(
              route(DataProviderRoute.Route.EDPA),
              dataProviderRoute {
                dataProvider = DIRECT_DATA_PROVIDER
                this.route = DataProviderRoute.Route.DIRECT_EDP
              },
            )
        }
      )

    assertThat(topology.routeFor(DATA_PROVIDER)).isEqualTo(ReportTraceRequisitionRouteKind.EDPA)
    assertThat(topology.routeFor(DIRECT_DATA_PROVIDER))
      .isEqualTo(ReportTraceRequisitionRouteKind.DIRECT_EDP)
    assertThat(topology.routeFor("dataProviders/missing"))
      .isEqualTo(ReportTraceRequisitionRouteKind.UNKNOWN)
  }

  private fun route(route: DataProviderRoute.Route): DataProviderRoute = dataProviderRoute {
    dataProvider = DATA_PROVIDER
    this.route = route
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/edpa"
    private const val DIRECT_DATA_PROVIDER = "dataProviders/direct"
  }
}
