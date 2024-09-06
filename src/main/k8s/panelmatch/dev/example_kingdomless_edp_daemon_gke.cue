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

_exchangeDaemonConfig: {
	partyType:            "DATA_PROVIDER"
	partyId:              string @tag("party_id")
	recurringExchangeIds: string @tag("recurring_exchange_ids")
	cloudStorageBucket:   "halo-edp-test-bucket"
	serviceAccountName:   "edp-workflow"
	tinkKeyUri:           "gcp-kms://projects/halo-cmm-dev/locations/us-central1/keyRings/edp-test-key-ring/cryptoKeys/edp-test-key"
	privateCa: {
		name:     "20220302-51i-yj4"
		poolId:   "EdpTestPool"
		location: "us-central1"
	}
	dataflow: {
		region:         "us-central1"
		serviceAccount: "edp-test-service-account@halo-cmm-dev.iam.gserviceaccount.com"
	}
}
