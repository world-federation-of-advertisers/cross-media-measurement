-- liquibase formatted sql

-- Copyright 2024 The Cross-Media Measurement Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- changeset sanjayvas:1 dbms:cloudspanner
-- comment: Create initial schema for the Access system

START BATCH DDL;

CREATE TABLE Principals (
  PrincipalId INT64 NOT NULL,

  PrincipalResourceId STRING(63) NOT NULL,
  CreateTime TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp = true),
)
PRIMARY KEY (PrincipalId);

CREATE UNIQUE INDEX PrincipalsByResourceId ON Principals(PrincipalResourceId);

-- Principals with identity type USER.
-- There is at most one UserPrincipal per Principal.
CREATE TABLE UserPrincipals (
  PrincipalId INT64 NOT NULL,

  Issuer STRING(MAX) NOT NULL,
  Subject STRING(MAX) NOT NULL,
)
PRIMARY KEY (PrincipalId),
INTERLEAVE IN PARENT Principals ON DELETE CASCADE;

CREATE UNIQUE INDEX UserPrincipalsByClaims ON UserPrincipals(Issuer, Subject);

CREATE TABLE Roles (
  RoleId INT64 NOT NULL,

  RoleResourceId STRING(63) NOT NULL,
  CreateTime TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp = true),
)
PRIMARY KEY (RoleId);

CREATE UNIQUE INDEX RolesByResourceId ON Roles(RoleResourceId);

CREATE TABLE RoleResourceTypes (
  RoleId INT64 NOT NULL,

  ResourceType STRING(MAX) NOT NULL,
)
PRIMARY KEY (RoleId, ResourceType),
INTERLEAVE IN PARENT Roles ON DELETE CASCADE;

CREATE TABLE RolePermissions (
  RoleId INT64 NOT NULL,
  PermissionId INT64 NOT NULL,

  FOREIGN KEY (RoleId) REFERENCES Roles(RoleId),
)
PRIMARY KEY (RoleId, PermissionId),
INTERLEAVE IN PARENT Roles ON DELETE CASCADE;

CREATE TABLE Policies (
  PolicyId INT64 NOT NULL,
  PolicyResourceId STRING(63) NOT NULL,

  -- Name of the protected resource. This may be empty string to mean API root.
  ProtectedResourceName STRING(MAX) NOT NULL,

  CreateTime TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp = true),
)
PRIMARY KEY (PolicyId);

CREATE UNIQUE INDEX PoliciesByResourceId ON Policies(PolicyResourceId);
CREATE UNIQUE INDEX PoliciesByProtectedResourceName ON Policies(ProtectedResourceName);

CREATE TABLE PolicyBindings (
  PolicyId INT64 NOT NULL,
  RoleId INT64 NOT NULL,
  PrincipalId INT64 NOT NULL,

  FOREIGN KEY (RoleId) REFERENCES Roles(RoleId),
  FOREIGN KEY (PrincipalId) REFERENCES Principals(PrincipalId),
)
PRIMARY KEY (PolicyId, RoleId, PrincipalId),
INTERLEAVE IN PARENT Policies ON DELETE CASCADE;

RUN BATCH;

