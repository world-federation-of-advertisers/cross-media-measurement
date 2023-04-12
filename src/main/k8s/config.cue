package k8s

import "strings"

#ContainerRegistryConfig: {
	registry:   string
	repoPrefix: string
}

#ImageConfig: Config={
	#ContainerRegistryConfig

	repoSuffix: string
	tag:        string
	image:      strings.Join([Config.registry, Config.repoPrefix, repoSuffix], "/") + ":\(tag)"
}
