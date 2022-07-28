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

#PostgresConfig: {
  _flags: [_=string]: string

  user: string
  database?: string
  flags: [ for name, value in _flags {"\(name)=\(value)"}]

  _flags: {
      "--postgres-user": user
      if database != _|_ {"--postgres-database": database}
  }
}

#PostgresConfig: {
  host:     string
  port:     uint32 | string
  password: string

  _flags: {
      "--postgres-host": host
      "--postgres-port": "\(port)"
      "--postgres-password": password
  }
} | {
  cloudSqlInstance: string

  // TODO(@tristanvuong2021): remove requirement for password flag
  _flags: {
    "--postgres-cloud-sql-instance": cloudSqlInstance
    "--postgres-password": "password"
  }
}
