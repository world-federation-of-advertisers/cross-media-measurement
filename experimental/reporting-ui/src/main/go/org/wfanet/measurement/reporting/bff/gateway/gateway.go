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

package gateway

import (
	"context"
	"fmt"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"net/http"

	gwruntime "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	_ "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"wfa/measurement/reporting/bff/v1alpha/reportingpb"
	"wfa/measurement/reporting/bff/v1alpha/eventgroupspb"
)

// newGateway returns a new gateway server which translates HTTP into gRPC.
func newGateway(ctx context.Context, conn *grpc.ClientConn, opts []gwruntime.ServeMuxOption) (http.Handler, error) {

	mux := gwruntime.NewServeMux(opts...)

	for _, f := range []func(context.Context, *gwruntime.ServeMux, *grpc.ClientConn) error{
		reportingpb.RegisterReportsHandler,
		eventgroupspb.RegisterEventGroupsHandler,
	} {
		if err := f(ctx, mux, conn); err != nil {
			return nil, err
		}
	}
	return mux, nil
}

// dialTCP creates a client connection via TCP.
// "addr" must be a valid TCP address with a port number.
func dial(ctx context.Context, addr string, certPath string) (*grpc.ClientConn, error) {
	fmt.Println(certPath)
	if certPath != "" {
		creds, err := credentials.NewClientTLSFromFile(certPath, "")
		if err == nil {
			fmt.Println("Connection Good")
			return grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(creds))
		}
	}
	return grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}
