// Copyright 2025 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"net/http"
	"strconv"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	_ "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"

	"github.com/world-federation-of-advertisers/cross-media-measurement/cmms/apiv2alpha/cmmspb"
	"github.com/world-federation-of-advertisers/cross-media-measurement/reporting/apiv2alpha/reportingpb"
)

var (
	grpcTarget          = flag.String("reporting-public-api-target", "", "gRPC target for the Reporting public API server (required)")
	grpcTargetCertHost  = flag.String("reporting-public-api-cert-host", "", "hostname of the server's certificate (optional, overrides DNS)")
	tlsTrustedCertsPath = flag.String("cert-collection-file", "", "path to PEM file containing trusted TLS certificates (required)")
	tlsCertPath         = flag.String("tls-cert-file", "", "path to PEM file containing TLS server certificate (required)")
	tlsKeyPath          = flag.String("tls-key-file", "", "path to PEM file containing private key for TLS server certificate (required)")
	port                = flag.Int("port", 8443, "port number to host the gateway on")
)

func run() error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Connect to gRPC server.
	mux := runtime.NewServeMux()
	conn, err := dial(ctx, *grpcTarget, *tlsTrustedCertsPath, *grpcTargetCertHost)
	if err != nil {
		return err
	}

	// Register handlers for every service.
	for _, f := range []func(context.Context, *runtime.ServeMux, *grpc.ClientConn) error{
		reportingpb.RegisterEventGroupsHandler,
		reportingpb.RegisterReportingSetsHandler,
		reportingpb.RegisterImpressionQualificationFiltersHandler,
		reportingpb.RegisterBasicReportsHandler,
		cmmspb.RegisterDataProvidersHandler,
		cmmspb.RegisterEventGroupMetadataDescriptorsHandler,
	} {
		if err := f(ctx, mux, conn); err != nil {
			return err
		}
	}

	// Start HTTP server (and proxy calls to gRPC server endpoint)
	addr := ":" + strconv.Itoa(*port)
	return http.ListenAndServeTLS(addr, *tlsCertPath, *tlsKeyPath, mux)
}

func dial(ctx context.Context, target string, trustedCertsPath string, certHost string) (*grpc.ClientConn, error) {
	creds, err := credentials.NewClientTLSFromFile(trustedCertsPath, certHost)
	if err != nil {
		return nil, err
	}

	return grpc.DialContext(ctx, target, grpc.WithTransportCredentials(creds))
}

func main() {
	flag.Parse()

	if err := run(); err != nil {
		grpclog.Fatal(err)
	}
}
