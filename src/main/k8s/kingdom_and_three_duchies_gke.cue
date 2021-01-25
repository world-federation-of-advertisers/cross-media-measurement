// Copyright 2020 The Cross-Media Measurement Authors
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

// cue cmd dump src/main/k8s/kingdom_and_three_duchies_from_cue.cue >
// src/main/k8s/kingdom_and_three_duchies_from_cue.yaml

package k8s

objectSets: [
		setup_job,
		kingdom.kingdom_service,
		kingdom.kingdom_pod,
		kingdom.kingdom_job,
] + [ for d in duchies for v in d {v}]

#Duchies: [
	{
		name: "a"
		key:  "057b22ef9c4e9626c22c13daed1363a1e6a5b309a930409f8d131f96ea2fa888"
	},
	{
		name: "b"
		key:  "31cc32e7cd53ff24f2b64ae8c531099af9867ebf5d9a659f742459947caa29b0"
	},
	{
		name: "c"
		key:  "338cce0306416b70e901436cb9eca5ac758e8ff41d7b58dabadf8726608ca6cc"
	},
]

#GkeDuchy: #Duchy & {
	_duchy_names: [ for d in #Duchies {d.name}]
	_spanner_schema_push_flags: [
		"--ignore-already-existing-databases",
		"--instance-name=qa-instance",
		"--project-name=ads-open-measurement",
	]
	_spanner_flags: [
		"--spanner-instance=qa-instance",
		"--spanner-project=ads-open-measurement",
	]
	_blob_storage_flags: [
		"--google-cloud-storage-bucket=local-measurement-providers",
		"--google-cloud-storage-project=ads-open-measurement",
	]
	_images: {
		"async-computation-control-server": "gcr.io/ads-open-measurement/duchy/async-computation-control"
		"computation-control-server":       "gcr.io/ads-open-measurement/duchy/computation-control"
		"herald-daemon":                    "gcr.io/ads-open-measurement/duchy/herald"
		"liquid-legions-v1-mill-daemon":    "gcr.io/ads-open-measurement/duchy/liquid-legions-v1-mill"
		"liquid-legions-v2-mill-daemon":    "gcr.io/ads-open-measurement/duchy/liquid-legions-v2-mill"
		"metric-values-storage-server":     "gcr.io/ads-open-measurement/duchy/metric-values"
		"publisher-data-server":            "gcr.io/ads-open-measurement/duchy/publisher-data"
		"push-spanner-schema-container":    "gcr.io/ads-open-measurement/setup/push-spanner-schema"
		"spanner-computations-server":      "gcr.io/ads-open-measurement/duchy/spanner-computations"
	}
	_duchy_image_pull_policy: "Always"
	_verbose_grpc_logging:    "false"
}

duchies: {for d in #Duchies {"\(d.name)": #GkeDuchy & {_duchy: d}}}

kingdom: #Kingdom & {
	_duchy_ids: [ for d in #Duchies {"duchy-\(d.name)"}]
	_spanner_schema_push_flags: [
		"--ignore-already-existing-databases",
		"--instance-name=qa-instance",
		"--project-name=ads-open-measurement",
	]
	_spanner_flags: [
		"--spanner-database=kingdom",
		"--spanner-instance=qa-instance",
		"--spanner-project=ads-open-measurement",
	]
	_images: {
		"push-spanner-schema-container": "gcr.io/ads-open-measurement/setup/push-spanner-schema"
		"report-maker-daemon":           "gcr.io/ads-open-measurement/kingdom/report-maker"
		"report-starter-daemon":         "gcr.io/ads-open-measurement/kingdom/report-starter"
		"requisition-linker-daemon":     "gcr.io/ads-open-measurement/kingdom/requisition-linker"
		"gcp-kingdom-data-server":       "gcr.io/ads-open-measurement/kingdom/data-server"
		"global-computation-server":     "gcr.io/ads-open-measurement/kingdom/global-computation"
		"requisition-server":            "gcr.io/ads-open-measurement/kingdom/requisition"
		"system-requisition-server":     "gcr.io/ads-open-measurement/kingdom/system-requisition"
	}
	_kingdom_image_pull_policy: "Always"
	_verbose_grpc_logging:      "false"
}

setup_job: "correctness-test-job": #CorrectnessTest & {
	_image:           "gcr.io/ads-open-measurement/loadtest/correctness-test"
	_imagePullPolicy: "Always"
	_args: [
		"--spanner-database=kingdom",
		"--spanner-instance=qa-instance",
		"--spanner-project=ads-open-measurement",
		"--google-cloud-storage-bucket=local-measurement-providers",
		"--google-cloud-storage-project=ads-open-measurement",
	]
}
