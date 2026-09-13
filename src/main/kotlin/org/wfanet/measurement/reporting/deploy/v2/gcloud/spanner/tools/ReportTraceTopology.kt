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

import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.config.reporting.DataProviderRoute
import org.wfanet.measurement.config.reporting.ReportTraceTopologyConfig

/** Operator-provided fulfillment topology for all DataProviders in a deployment. */
internal data class ReportTraceTopology(
  val routes: Map<String, ReportTraceRequisitionRouteKind>,
  val provenance: String,
) {
  fun routeFor(dataProvider: String): ReportTraceRequisitionRouteKind =
    routes[dataProvider] ?: ReportTraceRequisitionRouteKind.UNKNOWN

  companion object {
    fun fromConfig(config: ReportTraceTopologyConfig): ReportTraceTopology {
      val routes = mutableMapOf<String, ReportTraceRequisitionRouteKind>()
      for ((index, entry) in config.dataProviderRoutesList.withIndex()) {
        require(DataProviderKey.fromName(entry.dataProvider) != null) {
          "data_provider_routes[$index].data_provider is not a valid DataProvider resource name"
        }
        val route =
          when (entry.route) {
            DataProviderRoute.Route.DIRECT_EDP -> ReportTraceRequisitionRouteKind.DIRECT_EDP
            DataProviderRoute.Route.EDPA -> ReportTraceRequisitionRouteKind.EDPA
            DataProviderRoute.Route.ROUTE_UNSPECIFIED,
            DataProviderRoute.Route.UNRECOGNIZED ->
              throw IllegalArgumentException(
                "data_provider_routes[$index].route must be DIRECT_EDP or EDPA"
              )
          }
        require(routes.put(entry.dataProvider, route) == null) {
          "Duplicate DataProvider route: ${entry.dataProvider}"
        }
      }
      return ReportTraceTopology(
        routes = routes,
        provenance = "operator-provided --topology-config-file (${routes.size} DataProvider routes)",
      )
    }

    fun notSupplied(): ReportTraceTopology =
      ReportTraceTopology(emptyMap(), "not supplied; DataProvider routes are unknown")
  }
}
