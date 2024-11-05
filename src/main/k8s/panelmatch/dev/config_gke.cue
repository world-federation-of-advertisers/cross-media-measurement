// Copyright 2024 The Cross-Media Measurement Authors
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

#GCloudProject: string @tag("google_cloud_project")

#GCloudConfig: {
	project: #GCloudProject
}

#CloudStorageConfig: Config={
	#GCloudConfig

	bucket: string
	flags: [
		"--google-cloud-storage-project=" + Config.project,
		"--google-cloud-storage-bucket=" + bucket,
	]
}

#CloudDataflowConfig: Config={
	#GCloudConfig

	region:             string
	serviceAccountName: string
	bucket:             string
	flags: [
		"--dataflow-project-id=" + Config.project,
		"--dataflow-region=" + region,
		"--dataflow-service-account=" + serviceAccountName,
		"--dataflow-temp-location=gs://\(bucket)/dataflow-temp/",
		"--dataflow-worker-machine-type=n1-standard-1",
		"--dataflow-disk-size=30",
		"--dataflow-worker-logging-options-level=INFO",
		"--sdk-harness-options-log-level=INFO",
	]
}

#GCloudPrivateCaConfig: #PrivateCaConfig & {
	_extraFlags: [
		"--privateca-project-id=" + #GCloudConfig.project,
	]
}

#GCloudKmsConfig: Config={
	#GCloudConfig

	region:  string
	keyRing: string
	key:     string
	flags: [
		"--tink-key-uri=gcp-kms://projects/\(Config.project)/locations/\(region)/keyRings/\(keyRing)/cryptoKeys/\(key)",
	]
}
