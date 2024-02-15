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

_resourceRequirements: ResourceRequirements=#ResourceRequirements & {
	requests: {
		cpu:    "500m"
		memory: "768Mi"
	}
	limits: {
		memory: ResourceRequirements.requests.memory
	}
}

_populationSpec: "/etc/\(#AppName)/config-files/synthetic_population_spec.textproto"

edp_simulators: {
	for i, edp in _edpConfigs {
		let Number = i + 1
		let EventGroupSpec = "/etc/\(#AppName)/config-files/cartesian_synthetic_event_group_spec_\(Number).textproto"

		"\(edp.displayName)": {
			_imageConfig: repoSuffix: "simulator/synthetic-generator-edp"
			_additional_args: [
				"--population-spec=\(_populationSpec)",
				"--event-group-spec==\(EventGroupSpec)",
			]
			deployment: {
				_container: {
					_javaOptions: maxHeapSize: "512M"
					resources: _resourceRequirements
				}
				spec: template: spec: {
					_mounts: "config-files": #ConfigMapMount
				}
			}
		}
	}
}
