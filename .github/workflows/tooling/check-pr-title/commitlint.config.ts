/**
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
    RuleConfigCondition,
    RuleConfigSeverity,
    TargetCaseType,
} from "@commitlint/types";

export default {
    extends: ["@commitlint/config-conventional"],
    rules: {
        "header-max-length": [0], // disabled
        "subject-case": [
            RuleConfigSeverity.Error,
            "never",
            ["camel-case", "kebab-case", "pascal-case", "upper-case"]
        ] as [RuleConfigSeverity, RuleConfigCondition, TargetCaseType[]],
        "subject-empty": [RuleConfigSeverity.Error, "never"] as const,
        "subject-full-stop": [RuleConfigSeverity.Error, "never", "."] as const,
        "type-case": [RuleConfigSeverity.Error, "always", "lower-case"] as const,
        "type-empty": [RuleConfigSeverity.Error, "never"] as const,
        "type-enum": [
            RuleConfigSeverity.Error,
            "always",
            [
                "build",
                "chore",
                "ci",
                "docs",
                "feat",
                "fix",
                "perf",
                "refactor",
                "revert",
                "style",
                "test",
            ],
        ] as [RuleConfigSeverity, RuleConfigCondition, string[]],
    },
};