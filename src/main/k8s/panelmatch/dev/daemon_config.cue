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

#ExchangeDaemonConfig: {
	runMode:   "CRON_JOB" | "DAEMON"
	partyType: "DATA_PROVIDER" | "MODEL_PROVIDER"

	_extraFlags: [...string]

	flags: _extraFlags + [
		"--run-mode=" + runMode,
		"--party-type=" + partyType,
		"--blob-size-limit-bytes=1000000000", // 1 GB
		"--storage-signing-algorithm=EC",
		"--polling-interval=10s",
		"--preprocessing-max-byte-size=1000000", // 1 MB
		"--preprocessing-file-count=1000",
		"--max-parallel-claimed-exchange-steps=1",
	]
}

#KingdomlessExchangeDaemonConfig: #ExchangeDaemonConfig & {
	partyId:              string
	recurringExchangeIds: string

	runMode: "CRON_JOB"

	_extraFlags: [
		"--id=" + partyId,
		"--kingdomless-recurring-exchange-ids=" + recurringExchangeIds,
		"--checkpoint-signing-algorithm=SHA256withECDSA",
		"--lookback-window=14d",
		"--task-timeout=24h",
	]
}
