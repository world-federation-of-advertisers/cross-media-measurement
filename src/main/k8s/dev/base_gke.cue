package k8s

#NetworkPolicy: {
	_egresses: {
		gkeMetadataServer: {
			to: [{ipBlock: cidr: "127.0.0.1/32"}]
			ports: [{
				protocol: "TCP"
				port:     988
			}]
		}
	}
}
