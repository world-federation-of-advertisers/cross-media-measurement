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

#SpannerConfig: {
	project:      "cross-media-measurement-system"
	instance:     "emulator-instance"
	readyTimeout: "30s"

	let EmulatorTarget = #ServiceTarget & {serviceName: "spanner-emulator"}
	emulatorHost: EmulatorTarget.target
}

#JavaOptions: {
	initialHeapSize: _ | *"32M"
	maxHeapSize:     _ | *"96M"
}

#ContainerRegistryConfig: {
	registry:   string @tag("container_registry")
	repoPrefix: string @tag("image_repo_prefix")
}

#ImageConfig: {
	tag: string @tag("image_tag")
}
