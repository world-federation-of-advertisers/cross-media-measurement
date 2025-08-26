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

#CommonPostgresConfig: {
	_flags: [_=string]: string

	user?:             string
	database?:         string
	statementTimeout?: string
	flags: [ for name, value in _flags {"\(name)=\(value)"}]

	_flags: {
		if user != _|_ {"--postgres-user": user}
		if database != _|_ {"--postgres-database": database}
		if statementTimeout != _|_ {"--statement-timeout": statementTimeout}

	}
}

#CloudSqlPostgresConfig: {
	#CommonPostgresConfig

	project:  string
	region:   string
	instance: string
	// Local part of IAM user address.
	iamUserLocal: string

	user:           "\(iamUserLocal)@\(project).iam"
	connectionName: "\(project):\(region):\(instance)"

	_flags: {
		"--postgres-cloud-sql-connection-name": connectionName
	}
}

#AwsPostgresConfig: {
	#CommonPostgresConfig

	secretName: string
	region:     string
	host:       string
	port:       string

	_flags: {
		"--postgres-host":                   host
		"--postgres-port":                   port
		"--postgres-credential-secret-name": secretName
		"--postgres-region":                 region
	}
}

#PostgresConfig: PostgresConfig=#CloudSqlPostgresConfig | #AwsPostgresConfig | *{
	#CommonPostgresConfig
	*#CommonTarget | #ServiceTarget

	password: string

	_flags: {
		"--postgres-host":     PostgresConfig.host
		"--postgres-port":     "\(PostgresConfig.port)"
		"--postgres-password": password
	}
}
