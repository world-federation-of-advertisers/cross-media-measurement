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

#ServiceAccountPodSpec: {
	#PodSpec

	serviceAccountName: string
}

#SpotVmPodSpec: {
	#PodSpec

	_tolerations: "high_perf_spot_node": {
		operator: "Equal"
		value:    "true"
		effect:   "NoSchedule"
	}
}

#ServerDeployment: {
	_container: {
		resources: Resources={
			requests: {
				memory: _ | *"320Mi"
			}
			limits: {
				memory: _ | *Resources.requests.memory
			}
		}
	}
}

#JavaOptions: {
	initialHeapSize: _ | *maxHeapSize
	maxHeapSize:     _ | *"64M"
}

#ExternalService: {
	_eipAllocations?: string

	metadata: annotations: {
		"service.beta.kubernetes.io/aws-load-balancer-type":            "nlb"
		"service.beta.kubernetes.io/aws-load-balancer-scheme":          "internet-facing"
		"service.beta.kubernetes.io/aws-load-balancer-nlb-target-type": "ip"
		if _eipAllocations != _|_ {
			"service.beta.kubernetes.io/aws-load-balancer-eip-allocations": _eipAllocations
		}
	}
}
