// Copyright 2020 The Measurement System Authors
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

#CorrectnessTest: {
	_image:           string
	_imagePullPolicy: string
	_args: [...string]

	apiVersion: "batch/v1"
	kind:       "Job"
	metadata: {
		name: "correctness-test-job"
		labels: "app.kubernetes.io/name": #AppName
	}
	spec: template: spec: {
		containers: [{
			name:            "correctness-test-container"
			image:           _image
			imagePullPolicy: _imagePullPolicy
			args:            [
						"--data-provider-count=2",
						"--campaign-count=1",
						"--generated-set-size=1000",
						"--universe-size=10000000000",
						"--run-id=",
						"--sketch-config-file=/app/wfa_measurement_system/src/main/kotlin/org/wfanet/measurement/loadtest/config/liquid_legions_sketch_config.textproto",
						"--publisher-data-service-target=" + (#Target & {name: "a-publisher-data-server"}).target,
			] + _args
			volumeMounts: [{
				name:      "cache-volume"
				mountPath: "/cache"
			}]
		}]
		initContainers: [{
			name:  "init-a-publisher-data-server"
			image: "busybox:1.28"
			command: ['sh', '-c', "until nslookup a-publisher-data-server; do echo waiting for a-publisher-data-server; sleep 2; done"]
		}]
		restartPolicy: "OnFailure"
		volumes: [{
			name: "cache-volume"
			emptyDir: {}
		}]
	}
}
