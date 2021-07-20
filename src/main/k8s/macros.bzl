# Copyright 2020 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Build rules for generating Kubernetes yaml from CUE files."""

load("//build/cue:defs.bzl", "cue_export")

def cue_dump(name, srcs, expression = None, cue_tags = None):
    outfile = name + ".yaml"
    cue_export(
        name = name,
        srcs = srcs,
        outfile = outfile,
        filetype = "yaml",
        expression = expression,
        cue_tags = cue_tags,
    )

def generate_root_certificate(name, srcs, visibility = None):
    native.genrule(
        name = name,
        srcs = srcs,
        outs = [
            name + ".pem",
            name + ".key",
        ],
        cmd = ("openssl req -out $(@D)/%s.pem" % (name) +
               " -new -newkey ec -pkeyopt ec_paramgen_curve:prime256v1" +
               " -nodes -keyout $(@D)/%s.key" % (name) +
               " -x509 -days 3650 -subj '/O=Server CA/CN=ca.server.example.com' " +
               " -config $(location openssl.cnf) -extensions v3_ca"),
        visibility = visibility,
    )

def generate_server_certificate(name, root, srcs, visibility = None):
    native.genrule(
        name = name,
        srcs = srcs,
        outs = [
            name + ".csr",
            name + ".key",
            name + ".pem",
        ],
        cmd = ("openssl req -out $(@D)/%s.csr -new -newkey" % (name) +
               " ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes" +
               " -keyout $(@D)/%s.key -subj" % (name) +
               " '/O=Server/CN=server.example.com' -config $(location openssl.cnf)" +
               " -extensions v3_req;" +
               " openssl x509 -in $(@D)/%s.csr" % (name) +
               " -out $(@D)/%s.pem" % (name) +
               " -days 3650 -req -CA $(@D)/%s.pem" % (root) +
               " -CAkey $(@D)/%s.key" % (root) +
               " -CAcreateserial -extfile $(location openssl.cnf) -extensions usr_cert"),
        visibility = ["//visibility:public"],
    )
