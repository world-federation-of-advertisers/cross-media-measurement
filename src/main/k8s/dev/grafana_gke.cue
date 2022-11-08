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

_grafanaSecretName: string @tag("secret_name")

objectSets: [
	networkPolicies,
	grafana.configmaps,
	grafana.services,
	grafana.deployments,
]

grafana: #Grafana & {
	_prometheusUrl: "http://prometheus-frontend.default.svc:\(#PrometheusFrontendPort)"
	_secretName:    _grafanaSecretName

	deployments: {
		"grafana": {
			spec: template: spec: {
				nodeSelector: "iam.gke.io/gke-metadata-server-enabled": "true"
			}
		}
	}
}

networkPolicies: [Name=_]: #NetworkPolicy & {
	_name: Name
}
networkPolicies: {
	"grafana": {
		_app_label: "grafana-app"
		_egresses: "prometheus-frontend": {
			to: [{
				podSelector: matchLabels: app: "prometheus-frontend-app"
			}]
			ports: [{
				port:     #PrometheusFrontendPort
				protocol: "TCP"
			}]
		}
	}
}
