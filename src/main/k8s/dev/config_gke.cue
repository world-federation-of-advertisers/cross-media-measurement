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

#GCloudProject: string @tag("google_cloud_project")

#GCloudConfig: {
	project: #GCloudProject
}

#SpannerConfig: {
	project:      #GCloudProject
	instance:     string @tag("spanner_instance")
	readyTimeout: "30s"
}

#CloudStorageConfig: Config={
	#GCloudConfig

	bucket: string
	flags: [
		"--google-cloud-storage-project=" + Config.project,
		"--google-cloud-storage-bucket=" + bucket,
	]
}

#BigQueryConfig: Config={
	#GCloudConfig

	dataset: string
	table:   string
	flags: [
		"--big-query-project=" + Config.project,
		"--big-query-dataset=" + dataset,
		"--big-query-table=" + table,
	]
}

#PostgresConfig: {
	project:  #GCloudProject
	instance: string @tag("postgres_instance")
	region:   string @tag("postgres_region")
}

#WorkloadIdentityServiceAccount: #ServiceAccount & {
	_iamServiceAccountName: string

	let Principal = "\(_iamServiceAccountName)@\(#GCloudConfig.project).iam.gserviceaccount.com"
	metadata: annotations: "iam.gke.io/gcp-service-account": Principal
}
