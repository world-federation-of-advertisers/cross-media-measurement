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

package k8s

_bigQueryConfig: #BigQueryConfig & {
	dataset: string @tag("bigquery_dataset")
	table:   string @tag("bigquery_table")
}

_resourceRequirements: ResourceRequirements=#ResourceRequirements & {
	requests: {
		cpu:    "100m"
		memory: "288Mi"
	}
	limits: {
		memory: ResourceRequirements.requests.memory
	}
}

edp_simulators: {
	for edp in _edpConfigs {
		"\(edp.displayName)": {
			_additional_args: ["--publisher-id=\(edp.publisherId)"] + _bigQueryConfig.flags
			_imageConfig: repoSuffix: "simulator/bigquery-edp"
			deployment: {
				_container: {
					resources: _resourceRequirements
				}
			}
		}
	}
}
