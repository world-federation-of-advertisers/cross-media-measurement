// Copyright 2021 The Cross-Media Measurement Authors
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

_eventsCsv: "/data/csvfiles/synthetic-labelled-events.csv"

#EdpSimulatorsResourceRequirements: #ResourceRequirements & {
	limits: memory: "2048Mi"
}

edpSimulators: {
	for edpConfig in _edpConfigs {
		"\(edpConfig.displayName)": {
			_additional_args: [
				"--events-csv=\(_eventsCsv)",
				"--publisher-id=\(edpConfig.publisherId)",
			]
			deployment: {
				_container:
				{
					_javaOptions: maxRamPercentage: 30.0
					resources: #EdpSimulatorsResourceRequirements
				}
				spec: template: spec: {
					_mounts: {
						"csv-files": {
							volume: emptyDir: {}
							volumeMount: mountPath: "/data/csvfiles"
						}
					}
				}
			}
		}
	}
}
