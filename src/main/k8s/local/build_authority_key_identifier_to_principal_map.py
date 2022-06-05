#!/usr/bin/python
#
# Copyright 2022 The Cross-Media Measurement Authors
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

import os

principal_resource_names = []
with os.popen('/usr/bin/kubectl logs jobs/resource-setup-job') as F:
    for line in F:
        if line.find('Successfully created data provider') > 0:
            principal_resource_names.append(line.split(' ')[5][:-1])

template = ('# proto-file: src/main/proto/wfa/measurement/config/authority_key_to_principal_map.proto',
            '# proto-message: AuthorityKeyToPrincipalMap',
            'entries {',
            r'  authority_key_identifier: "\xD6\x65\x86\x86\xD8\x7E\xD2\xC4\xDA\xD8\xDF\x76\x39\x66\x21\x3A\xC2\x92\xCC\xE2"',
            f'  principal_resource_name: "{principal_resource_names[0]}"',
            '}',
            'entries {',
            r'  authority_key_identifier: "\x6F\x57\x36\x3D\x7C\x5A\x49\x7C\xD1\x68\x57\xCD\xA0\x44\xDF\x68\xBA\xD1\xBA\x86"',
            f'  principal_resource_name: "{principal_resource_names[1]}"'
            '}',
            'entries {',
            r'  authority_key_identifier: "\xEE\xB8\x30\x10\x0A\xDB\x8F\xEC\x33\x3B\x0A\x5B\x85\xDF\x4B\x2C\x06\x8F\x8E\x28"',
            f'  principal_resource_name: "{principal_resource_names[2]}"',
            '}',
            'entries {',
            r'  authority_key_identifier: "\x74\x72\x6D\xF6\xC0\x44\x42\x61\x7D\x9F\xF7\x3F\xF7\xB2\xAC\x0F\x9D\xB0\xCA\xCC"',
            f'  principal_resource_name: "{principal_resource_names[3]}"',
            '}',
            'entries {',
            r'  authority_key_identifier: "\xA6\xED\xBA\xEA\x3F\x9A\xE0\x72\x95\xBF\x1E\xD2\xCB\xC8\x6B\x1E\x0B\x39\x47\xE9"',
            f'  principal_resource_name: "{principal_resource_names[4]}"',
            '}',
            'entries {',
            r'  authority_key_identifier: "\xA7\x36\x39\x6B\xDC\xB4\x79\xC3\xFF\x08\xB6\x02\x60\x36\x59\x84\x3B\xDE\xDB\x93"',
            f'  principal_resource_name: "{principal_resource_names[5]}"',
            '}')

with open('/tmp/authority_key_identifier_to_principal_map.textproto', 'w') as T:
    print('\n'.join(template), file=T)
