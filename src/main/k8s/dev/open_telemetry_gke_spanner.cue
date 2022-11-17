// Copyright 2022 The Cross-Media Measurement Authors
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

package k8s

_database: string @tag("database")

// Name of K8s service account for OpenTelemetry collector.
#CollectorServiceAccount: "otel-collector"

objectSets: [
	collectors,
	openTelemetry.instrumentations,
	openTelemetry.networkPolicies,
]

openTelemetry: #OpenTelemetry

collectors: [Name=string]: #OpenTelemetryCollector & {
	metadata: name: Name
}
collectors: {
	"deployment": {
		spec: {
			nodeSelector:   #ServiceAccountNodeSelector
			serviceAccount: #CollectorServiceAccount
			config:         """
        receivers:
          otlp:
            protocols:
              grpc:
                endpoint: 0.0.0.0:\(#OpenTelemetryReceiverPort)

          googlecloudspanner:
            collection_interval: 60s
            top_metrics_query_max_rows: 100
            backfill_enabled: true
            cardinality_total_limit: 200000
            projects:
              - project_id: \(#GCloudProject)
                instances:
                  - instance_id: \(#SpannerInstance)
                    databases:
                      - \(_database)

        processors:
          batch:
            send_batch_size: 200
            timeout: 10s

        exporters:
          prometheus:
            send_timestamps: true
            endpoint: 0.0.0.0:\(#OpenTelemetryPrometheusExporterPort)
            resource_to_telemetry_conversion:
              enabled: true

        extensions:
          health_check:

        service:
          extensions: [health_check]
          pipelines:
            metrics:
              receivers: [otlp, googlecloudspanner]
              processors: [batch]
              exporters: [prometheus]
       """
		}
	}
}
